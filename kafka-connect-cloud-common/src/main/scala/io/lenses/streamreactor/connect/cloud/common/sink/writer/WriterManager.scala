/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.sink.writer

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.ForcedWriteReason
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.data.Schema

import java.io.File
import java.net.URLEncoder
import scala.collection.immutable
import scala.collection.mutable
import scala.util.control.NonFatal

case class MapKey(topicPartition: TopicPartition, partitionValues: immutable.Map[PartitionField, String])

/**
 * Classification of a `preCommit` cycle for a PARTITIONBY topic-partition.
 *
 *  - `Skip`         : dirty flag is clean; `updateMasterLock` is skipped. Offset still returned to Connect.
 *  - `WriteRoutine` : dirty flag is true; `updateMasterLock` is called once. GC runs on success.
 *  - `WriteForced`  : lifecycle force point (revoke / stop / post-`cleanUp`). Same write path as
 *                     `WriteRoutine`; carries a reason tag so the per-reason metric counter is incremented.
 */
private[writer] sealed trait WriteDecision
private[writer] object WriteDecision {
  case object Skip         extends WriteDecision
  case object WriteRoutine extends WriteDecision
  final case class WriteForced(reason: ForcedWriteReason) extends WriteDecision
}

/**
 * Observable outcome of a best-effort force master-lock write executed during
 * `closePartition` (revoke / stop / post-cleanUp lifecycle points).
 *
 *  - `NotNeeded` : no committed offsets yet, no PARTITIONBY writers, or dirty flag was false.
 *                  The durable master-lock floor is already authoritative; nothing to do.
 *  - `Succeeded` : `updateMasterLock` returned `Right(_)`. Durable floor advanced.
 *  - `Failed`    : `updateMasterLock` returned `Left(err)`. Next owner replays from older floor.
 *  - `Threw`     : `updateMasterLock` threw a `NonFatal` exception. Same replay fallback applies.
 */
private[writer] sealed trait ForceWriteOutcome
private[writer] object ForceWriteOutcome {
  case object NotNeeded extends ForceWriteOutcome
  case object Succeeded extends ForceWriteOutcome
  final case class Failed(reason: String) extends ForceWriteOutcome
  final case class Threw(t: Throwable) extends ForceWriteOutcome
}

/**
 * Manages the lifecycle of [[Writer]] instances.
 *
 * A given sink may be writing to multiple locations (partitions), and therefore
 * it is convenient to extract this to another class.
 *
 * This class is not thread safe as it is not designed to be shared between concurrent
 * sinks, since file handles cannot be safely shared without considerable overhead.
 */
class WriterManager[SM <: FileMetadata](
  commitPolicyFn:               TopicPartition => Either[SinkError, CommitPolicy],
  bucketAndPrefixFn:            TopicPartition => Either[SinkError, CloudLocation],
  keyNamerFn:                   TopicPartition => Either[SinkError, KeyNamer],
  stagingFilenameFn:            (TopicPartition, Map[PartitionField, String]) => Either[SinkError, File],
  objKeyBuilderFn:              (TopicPartition, Map[PartitionField, String]) => ObjectKeyBuilder,
  formatWriterFn:               (TopicPartition, File) => Either[SinkError, FormatWriter],
  indexManager:                 IndexManager,
  transformerF:                 MessageDetail => Either[RuntimeException, MessageDetail],
  schemaChangeDetector:         SchemaChangeDetector,
  skipNullValues:               Boolean,
  pendingOperationsProcessors:  PendingOperationsProcessors,
  private[sink] val commitMode: CommitMode       = CommitMode.Default,
  metrics:                      CloudSinkMetrics = new CloudSinkMetrics(),
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends StrictLogging {

  private val writers = mutable.LinkedHashMap.empty[MapKey, Writer[SM]]
  // Auxiliary index: one LinkedHashMap per TopicPartition, kept strictly in lockstep with
  // `writers`. Insertion order is preserved within each bucket so the iteration order seen
  // by `WriterCommitManager` matches the primary map's order. This lets the per-record
  // commit path visit only O(siblings on TP) entries instead of O(all writers).
  private val tpIndex = mutable.Map.empty[TopicPartition, mutable.LinkedHashMap[MapKey, Writer[SM]]]

  private val writerSource: WriterSource[SM] = new WriterSource[SM] {
    override def iterator: Iterator[(MapKey, Writer[SM])] =
      writers.iterator
    override def iteratorForTopicPartition(tp: TopicPartition): Iterator[(MapKey, Writer[SM])] =
      tpIndex.get(tp).fold(Iterator.empty[(MapKey, Writer[SM])])(_.iterator)
  }

  private val writerCommitManager =
    new WriterCommitManager[SM](writerSource, indexManager, pendingOperationsProcessors, commitMode, metrics)

  // Highest globalSafeOffset ever reported to Kafka Connect per TP. Prevents regression on
  // idle-writer eviction. See docs/datalake-exactly-once-partitionby.md ("globalSafeOffset regression").
  private val safeOffsetHighWatermarks = mutable.Map.empty[TopicPartition, Long]

  // Highest globalSafeOffset successfully persisted by `updateMasterLock` for this TP.
  // Lazy-seeded from the durable master-lock floor on first access (mirrors `safeOffsetHighWatermarks`
  // seeding). `dirtyMasterLock(tp)` is the computed predicate `globalSafeOffset > lastWrittenMasterSafeOffset(tp)`.
  // Cleared by `close(tp)` and `cleanUp(tp)`.
  private val lastWrittenMasterSafeOffset = mutable.Map.empty[TopicPartition, Long]

  // Highest globalSafeOffset returned to Kafka Connect for this TP in the current ownership episode.
  // Cleared by `close(tp)` only; PRESERVED across `cleanUp(tp)` so the HWM re-seed after rollback
  // stays monotonic. Role by mode: see docs/datalake-exactly-once-partitionby.md
  // ("`lastReturnedSafeOffset` Role by Mode").
  private val lastReturnedSafeOffset = mutable.Map.empty[TopicPartition, Long]

  // When set, the next `preCommit` for this TP is classified `WriteForced(PostCleanUp)` regardless
  // of the dirty flag. Cleared on the next write attempt (success or failure).
  private val forceWriteAfterCleanUp = mutable.Set.empty[TopicPartition]

  // Batch mode only. Highest offset this task has successfully buffered for the TP in the current
  // ownership episode, across every key. It is the in-memory half of the batch dedup floor: it
  // covers records that are held but not yet durable, which the master lock cannot know about.
  // Cleared wherever those buffers are discarded — `closePartition`, `close()` and `cleanUp(tp)` —
  // because after a rollback the records really do have to be re-delivered.
  private val maxBufferedOffset = mutable.Map.empty[TopicPartition, Long]

  def recommitPending(): Either[SinkError, Unit] = {
    logger.debug(s"[{}] Retry Pending", connectorTaskId.show)
    val result = writerCommitManager.commitPending()
    logger.debug(s"[{}] Retry Pending Complete", connectorTaskId.show)
    result
  }

  def commitFlushableWriters(): Either[BatchCloudSinkError, Unit] = {
    logger.debug(s"[{}] Received call to WriterManager.commitFlushableWriters", connectorTaskId.show)
    writerCommitManager.commitFlushableWriters()
  }

  /**
   * Per-TP close: attempt a force master-lock write if dirty, then clear per-TP state,
   * close writers, and evict the granular-lock cache. Order is mandatory — compute offset
   * before closing writers so the force write has a meaningful value to persist.
   */
  private def closePartition(topicPartition: TopicPartition, reason: ForcedWriteReason): Unit = {
    val tpWriters = writersForTopicPartition(topicPartition)
    // Defensive wrap: state clear, writer close, and cache eviction below must always run
    // regardless of the forced-write outcome.
    // Batch mode never force-writes: the durable floor only ever advances through a completed
    // batch CAS, so writing it here could claim offsets whose objects are still at temp paths.
    if (tpWriters.nonEmpty && commitMode == CommitMode.Granular) {
      val outcome: ForceWriteOutcome =
        try {
          attemptForceMasterLockWrite(topicPartition, tpWriters, reason)
        } catch {
          case NonFatal(t) =>
            logger.warn(
              s"[${connectorTaskId.show}] Force-${reason} master-lock write threw unexpectedly for " +
                s"$topicPartition; proceeding with state cleanup. The next owner / restart will replay " +
                s"from the durable master floor (deduplicated by granular locks).",
              t,
            )
            ForceWriteOutcome.Threw(t)
        }
      outcome match {
        case ForceWriteOutcome.Threw(_) =>
          // Left(err) failures are already counted inside attemptForceMasterLockWrite.
          // Unexpected throws are not, so we increment both counters here.
          metrics.incrementMasterLockFailures()
          metrics.incrementMasterLockWriteForcedFailure(reason)
        case _ =>
      }
    }
    safeOffsetHighWatermarks.remove(topicPartition)
    lastWrittenMasterSafeOffset.remove(topicPartition)
    lastReturnedSafeOffset.remove(topicPartition)
    forceWriteAfterCleanUp.remove(topicPartition)
    maxBufferedOffset.remove(topicPartition)
    // Materialise keys to a list so the map mutation does not invalidate the iterator.
    val keysToRemove = writers
      .view.filterKeys(_.topicPartition == topicPartition)
      .keys
      .toList
    keysToRemove.foreach { key =>
      writers.get(key).foreach(_.close())
      writers.remove(key)
      removeFromTpIndex(key)
    }
    // Writer.close() deliberately leaves cache entries for cleanUpObsoleteLocks to GC;
    // this eviction is the sole memory-cleanup path on shutdown.
    indexManager.evictAllGranularLocks(topicPartition)
  }

  /**
   * Best-effort force write of the master lock for `topicPartition`. Computes `globalSafeOffset`
   * from the current writers + HWM and calls `updateMasterLock` once if `dirtyMasterLock(tp)` is
   * true. Returns a [[ForceWriteOutcome]] that the caller uses to drive observability. Failure is
   * non-fatal; the next owner replays from the durable floor (deduplicated by granular locks).
   */
  private def attemptForceMasterLockWrite(
    topicPartition: TopicPartition,
    tpWriters:      Seq[Writer[SM]],
    reason:         ForcedWriteReason,
  ): ForceWriteOutcome = {
    val firstBufferedOffsets = tpWriters.flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = tpWriters.flatMap(_.getCommittedOffset)
    if (committedOffsets.isEmpty) {
      // No committed offsets yet — durable master-lock floor is already authoritative.
      logger.debug(
        s"[${connectorTaskId.show}] Force-${reason} skipped for $topicPartition: " +
          s"no committed offsets (writers have not yet flushed); durable master-lock floor is authoritative.",
      )
      ForceWriteOutcome.NotNeeded
    } else {
      val calculatedSafeOffset =
        if (firstBufferedOffsets.nonEmpty) firstBufferedOffsets.map(_.value).min
        else committedOffsets.map(_.value).max + 1
      val previousHighWatermark = currentHighWatermark(topicPartition)
      val globalSafeOffset      = math.max(calculatedSafeOffset, previousHighWatermark)
      val lastWritten           = currentLastWrittenMaster(topicPartition)
      val dirty                 = globalSafeOffset > lastWritten

      val hasPartitionByWriters =
        writers.keys.exists(k => k.topicPartition == topicPartition && k.partitionValues.nonEmpty)

      if (hasPartitionByWriters && dirty) {
        metrics.incrementMasterLockWriteForced(reason)
        indexManager.updateMasterLock(topicPartition, Offset(globalSafeOffset)) match {
          case Left(err) =>
            metrics.incrementMasterLockFailures()
            metrics.incrementMasterLockWriteForcedFailure(reason)
            logger.warn(
              s"[${connectorTaskId.show}] Force-${reason} updateMasterLock failed for $topicPartition: " +
                s"${err.message()} — proceeding without forced persistence. The next owner / restart will " +
                s"replay from the older durable master floor (deduplicated by granular locks).",
            )
            ForceWriteOutcome.Failed(err.message())
          case Right(_) =>
            metrics.incrementMasterLockUpdates()
            lastWrittenMasterSafeOffset.put(topicPartition, globalSafeOffset)
            val activePartitionKeys: Set[String] = writers
              .filter { case (key, _) => key.topicPartition == topicPartition }
              .keys
              .flatMap(key => WriterManager.derivePartitionKey(key.partitionValues))
              .toSet
            indexManager.cleanUpObsoleteLocks(topicPartition, Offset(globalSafeOffset), activePartitionKeys) match {
              case Left(gcErr) =>
                logger.warn(
                  s"[${connectorTaskId.show}] Best-effort GC after force-${reason} failed for $topicPartition: " +
                    s"${gcErr.message()}",
                )
              case Right(_) =>
            }
            ForceWriteOutcome.Succeeded
        }
      } else if (hasPartitionByWriters) {
        logger.debug(
          s"[${connectorTaskId.show}] Force-${reason} skipped for $topicPartition: " +
            s"globalSafeOffset=$globalSafeOffset already persisted (lastWritten=$lastWritten).",
        )
        ForceWriteOutcome.NotNeeded
      } else {
        // Non-PARTITIONBY — master lock is managed inside Writer.commit(); nothing to force here.
        ForceWriteOutcome.NotNeeded
      }
    }
  }

  /** Reads the current HWM for the TP without seeding (returns 0 if absent). */
  private def currentHighWatermark(topicPartition: TopicPartition): Long =
    safeOffsetHighWatermarks.getOrElse(topicPartition, 0L)

  /**
   * Reads `lastWrittenMasterSafeOffset(tp)` if already seeded; otherwise lazy-seeds from the
   * durable master-lock floor (`getSeekedOffsetForTopicPartition + 1`, or 0 if absent).
   * Mirrors the lazy-seed used by `safeOffsetHighWatermarks` so the dirty-flag gate cannot
   * fire on a fresh ownership episode before the master lock has been read.
   */
  private def currentLastWrittenMaster(topicPartition: TopicPartition): Long =
    lastWrittenMasterSafeOffset.getOrElseUpdate(
      topicPartition,
      indexManager.getSeekedOffsetForTopicPartition(topicPartition)
        .map(_.value + 1)
        .getOrElse(0L),
    )

  /**
   * Rebalance / shutdown close: runs `closePartition` for every owned TP then bulk-clears
   * all per-TP state. The default `Revoke` tag matches `CloudSinkTask.close(partitions)`.
   */
  def close(reason: ForcedWriteReason = ForcedWriteReason.Revoke): Unit = {
    logger.debug(s"[{}] Received call to WriterManager.close", connectorTaskId.show)
    val topicPartitions = writers.keys.map(_.topicPartition).toSet
    topicPartitions.foreach(closePartition(_, reason))
    // Defensive bulk clear: a `cleanUp(tp)` immediately before `close()` with no intervening
    // `put()` leaves the TP absent from `writers.keys`, so `closePartition` does not visit it.
    // The bulk clear is a hard reset of all per-TP state (a no-op for TPs already drained by
    // `closePartition`). Safety: see docs/datalake-exactly-once-partitionby.md
    // ("`lastReturnedSafeOffset` Role by Mode").
    safeOffsetHighWatermarks.clear()
    lastWrittenMasterSafeOffset.clear()
    lastReturnedSafeOffset.clear()
    forceWriteAfterCleanUp.clear()
    maxBufferedOffset.clear()
    metrics.clearAllMasterLockDirty()
    // After every per-TP closePartition has run, refresh the writer-count and oldest-file
    // gauges for the final values (in the steady-state path writers are now empty).
    metrics.setWriterCount(writers.size)
  }

  /** `close(Stop)` — uses the `Stop` reason tag so `masterLockWriteForcedStop` is incremented. */
  def closeForStop(): Unit = close(ForcedWriteReason.Stop)

  def write(topicPartitionOffset: TopicPartitionOffset, messageDetail: MessageDetail): Either[SinkError, Unit] = {

    logger.debug(
      s"[${connectorTaskId.show}] Received call to WriterManager.write for ${topicPartitionOffset.topic}-${topicPartitionOffset.partition}:${topicPartitionOffset.offset}",
    )
    for {
      writer <- writer(topicPartitionOffset.toTopicPartition, messageDetail)
      shouldSkip <- commitMode match {
        case CommitMode.Granular => writer.shouldSkip(topicPartitionOffset.offset).asRight[SinkError]
        case CommitMode.Batch    =>
          // Routing-independent floor: max(masterW, maxBuffered). Deliberately ignores which key
          // the record routed to (see `IndexManager.batchDedupFloor`) -- a replayed record is not
          // guaranteed to route back to the key that originally handled it.
          indexManager.batchDedupFloor(topicPartitionOffset.toTopicPartition).map {
            imFloor =>
              val floor =
                (imFloor.map(_.value).toList ++ maxBufferedOffset.get(
                  topicPartitionOffset.toTopicPartition,
                ).toList).maxOption
              floor.exists(_ >= topicPartitionOffset.offset.value)
          }
      }
      resultIfNotSkipped <-
        if (!shouldSkip) {
          transformerF(messageDetail).leftMap(ex =>
            new FatalCloudSinkError(ex.getMessage, ex.some, topicPartitionOffset.toTopicPartition),
          ).flatMap { transformed =>
            writeAndCommit(topicPartitionOffset, transformed, writer)
          }
        } else {
          metrics.incrementDuplicateRecordsSkippedTotal()
          ().asRight
        }
    } yield resultIfNotSkipped
  }

  private def writeAndCommit(
    topicPartitionOffset: TopicPartitionOffset,
    messageDetail:        MessageDetail,
    writer:               Writer[SM],
  ): Either[SinkError, Unit] =
    for {
      // commitException can not be recovered from
      _ <- rollOverTopicPartitionWriters(writer, topicPartitionOffset.toTopicPartition, messageDetail)
      // a processErr can potentially be recovered from in the next iteration.  Can be due to network problems
      _ <- writer.write(messageDetail)
      _ = if (commitMode == CommitMode.Batch) {
        val tp      = topicPartitionOffset.toTopicPartition
        val current = maxBufferedOffset.getOrElse(tp, Long.MinValue)
        if (topicPartitionOffset.offset.value > current) {
          val _ = maxBufferedOffset.put(tp, topicPartitionOffset.offset.value)
        }
      }
      commitRes <- writerCommitManager.commitFlushableWritersForTopicPartition(topicPartitionOffset.toTopicPartition)
    } yield commitRes

  private def rollOverTopicPartitionWriters(
    writer:         Writer[SM],
    topicPartition: TopicPartition,
    message:        MessageDetail,
  ): Either[BatchCloudSinkError, Unit] =
    //TODO: fix this; it cannot always be VALUE and it depends on writer requiring a roll over to new file
    message.value.schema() match {
      case Some(value: Schema) if writer.shouldRollover(value) =>
        // Schema rollover: flush all writers for the TP together to keep the format boundary
        // consistent. This is the one full-fan-out path; do NOT weaken to selective commit.
        metrics.incrementSchemaRolloversTotal()
        writerCommitManager.commitForTopicPartition(topicPartition)
      case _ => ().asRight
    }

  private def processPartitionValues(
    messageDetail:  MessageDetail,
    keyNamer:       KeyNamer,
    topicPartition: TopicPartition,
  ): Either[SinkError, immutable.Map[PartitionField, String]] =
    keyNamer.processPartitionValues(messageDetail, topicPartition)

  /**
   * Returns a writer that can write records for a particular topic and partition.
   * The writer will create a file inside the given directory if there is no open writer.
   */
  private def writer(topicPartition: TopicPartition, messageDetail: MessageDetail): Either[SinkError, Writer[SM]] =
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      keyNamer        <- keyNamerFn(topicPartition)
      partitionValues <- processPartitionValues(messageDetail, keyNamer, topicPartition)
      key              = MapKey(topicPartition, partitionValues)
      maybeWriter      = writers.get(key)
      writer <- maybeWriter match {
        case Some(w) => w.asRight
        case None =>
          createWriter(bucketAndPrefix, topicPartition, partitionValues)
            .map { w =>
              writers.put(key, w)
              addToTpIndex(key, w)
              evictIdleWriters(key.topicPartition, Some(key))
              metrics.setWriterCount(writers.size)
              w
            }
      }
    } yield writer

  private def createWriter(
    bucketAndPrefix: CloudLocation,
    topicPartition:  TopicPartition,
    partitionValues: Map[PartitionField, String],
  ): Either[SinkError, Writer[SM]] = {
    logger.debug(s"[${connectorTaskId.show}] Creating new writer for bucketAndPrefix:$bucketAndPrefix")
    val partitionKey = WriterManager.derivePartitionKey(partitionValues)
    for {
      commitPolicy <- commitPolicyFn(topicPartition)
      // Batch mode never reads or writes a granular lock: dedup comes from the TP-level floor, so
      // creating one per key would be pure API cost.
      _ <-
        if (commitMode == CommitMode.Batch) ().asRight[SinkError]
        else partitionKey.fold(().asRight[SinkError])(pk => indexManager.ensureGranularLock(topicPartition, pk))
      lastSeekedOffset <-
        if (commitMode == CommitMode.Batch) {
          // Populates `getCommittedOffset` so `preCommit`'s `committedOffsets.isEmpty` check behaves
          // exactly as the granular master-lock fallback does.
          indexManager.getSeekedOffsetForTopicPartition(topicPartition).asRight[SinkError]
        } else partitionKey match {
          case Some(pk) =>
            // Granular-lock-first, master-lock-fallback: prevents duplication when a lock is GC'd
            // and a new writer is later created for the same partition key.
            // When globalSafeOffset == 0, `updateMasterLock` stores None, so the fallback
            // produces None.orElse(None) = None — no false skip of offset 0.
            indexManager.getSeekedOffsetForPartitionKey(topicPartition, pk).map {
              granularOffset =>
                // max, not orElse: for a granular lock K below the master floor M, K.lock < M
                // means K has no records in (K.lock, M] (any buffered one would have pinned
                // globalSafeOffset <= firstBuffered_K <= M). So max(K.lock, M) never skips an
                // uncommitted record and fixes the duplication when a lock is GC'd below the
                // master floor.
                val master = indexManager.getSeekedOffsetForTopicPartition(topicPartition)
                (granularOffset.toList ++ master.toList) match {
                  case Nil  => None
                  case list => Some(list.maxBy(_.value))
                }
            }
          case None =>
            indexManager.getSeekedOffsetForTopicPartition(topicPartition).asRight[SinkError]
        }
    } yield {
      new Writer(
        topicPartition,
        commitPolicy,
        indexManager,
        () => stagingFilenameFn(topicPartition, partitionValues),
        objKeyBuilderFn(topicPartition, partitionValues),
        formatWriterFn.curried(topicPartition),
        schemaChangeDetector,
        pendingOperationsProcessors,
        partitionKey,
        lastSeekedOffset,
        metrics,
      )
    }
  }

  def preCommit(
    currentOffsets: immutable.Map[TopicPartition, OffsetAndMetadata],
  ): immutable.Map[TopicPartition, OffsetAndMetadata] =
    currentOffsets
      .flatMap { case (tp, offAndMeta) =>
        getOffsetAndMeta(tp, offAndMeta).map(tp -> _)
      }

  private def writersForTopicPartition(topicPartition: TopicPartition): Seq[Writer[SM]] =
    tpIndex.get(topicPartition).fold(Seq.empty[Writer[SM]])(_.values.toSeq)

  private def getOffsetAndMeta(
    topicPartition:    TopicPartition,
    offsetAndMetadata: OffsetAndMetadata,
  ): Option[OffsetAndMetadata] = {
    val tpWriters = writersForTopicPartition(topicPartition)
    if (tpWriters.isEmpty) {
      None
    } else {
      val firstBufferedOffsets = tpWriters.flatMap(_.getFirstBufferedOffset)
      val committedOffsets     = tpWriters.flatMap(_.getCommittedOffset)

      metrics.setSafeOffsetBarrierWriters(firstBufferedOffsets.size)

      if (committedOffsets.isEmpty) {
        None
      } else {
        val calculatedSafeOffset =
          if (firstBufferedOffsets.nonEmpty) {
            firstBufferedOffsets.map(_.value).min
          } else {
            committedOffsets.map(_.value).max + 1
          }

        // HWM lazy-seed: max of the durable master-lock floor and `lastReturnedSafeOffset(tp)`
        // to stay monotonic across `cleanUp(tp)` rollbacks. See docs/datalake-exactly-once-partitionby.md
        // ("`lastReturnedSafeOffset` Role by Mode") for the PARTITIONBY vs non-PARTITIONBY rationale.
        val previousHighWatermark = safeOffsetHighWatermarks.getOrElseUpdate(
          topicPartition,
          math.max(
            indexManager.getSeekedOffsetForTopicPartition(topicPartition).map(_.value + 1).getOrElse(0L),
            lastReturnedSafeOffset.getOrElse(topicPartition, 0L),
          ),
        )
        val globalSafeOffset = math.max(calculatedSafeOffset, previousHighWatermark)

        require(
          globalSafeOffset >= previousHighWatermark,
          s"globalSafeOffset regression for $topicPartition: $globalSafeOffset < previousHWM $previousHighWatermark",
        )
        require(
          globalSafeOffset >= calculatedSafeOffset,
          s"globalSafeOffset below calculated floor for $topicPartition: $globalSafeOffset < $calculatedSafeOffset",
        )

        val hasPartitionByWriters =
          writers.keys.exists(k => k.topicPartition == topicPartition && k.partitionValues.nonEmpty)

        val shouldReturnOffset: Boolean =
          if (commitMode == CommitMode.Batch) {
            // Batch mode: `preCommit` is a pure read. `commitBatch`'s CAS is the only thing that
            // advances the durable floor, and granular-lock GC has nothing to collect because
            // batch mode never creates granular locks. `firstBufferedOffsets` already includes
            // `Staged` writers, so the barrier below is the same one granular mode computes.
            metrics.clearMasterLockDirty(topicPartition)
            safeOffsetHighWatermarks.put(topicPartition, globalSafeOffset)
            advanceLastReturned(topicPartition, globalSafeOffset)
            forceWriteAfterCleanUp.remove(topicPartition)
            true
          } else if (hasPartitionByWriters) {
            val lastWritten = currentLastWrittenMaster(topicPartition)
            val dirty       = globalSafeOffset > lastWritten

            if (dirty) {
              metrics.incrementMasterLockDirtyWindowCycle()
              metrics.markMasterLockDirty(topicPartition)
            } else {
              metrics.clearMasterLockDirty(topicPartition)
            }

            val decision: WriteDecision =
              if (forceWriteAfterCleanUp.contains(topicPartition))
                WriteDecision.WriteForced(ForcedWriteReason.PostCleanUp)
              else if (dirty) WriteDecision.WriteRoutine
              else WriteDecision.Skip

            decision match {
              case WriteDecision.Skip =>
                require(
                  globalSafeOffset == lastWritten,
                  s"Skip branch invariant violated for $topicPartition: globalSafeOffset=$globalSafeOffset, " +
                    s"lastWritten=$lastWritten — the dirty-flag gate requires equality here.",
                )
                metrics.incrementMasterLockWriteSkipped()
                safeOffsetHighWatermarks.put(topicPartition, globalSafeOffset)
                advanceLastReturned(topicPartition, globalSafeOffset)
                logger.debug(
                  s"[${connectorTaskId.show}] Master-lock write skipped for $topicPartition: " +
                    s"globalSafeOffset=$globalSafeOffset already persisted.",
                )
                true

              case WriteDecision.WriteRoutine =>
                attemptMasterLockWrite(topicPartition, globalSafeOffset, forcedReason = None)

              case WriteDecision.WriteForced(reason) =>
                // Clear before the write so a failure falls back to the routine dirty-flag path.
                if (reason == ForcedWriteReason.PostCleanUp) forceWriteAfterCleanUp.remove(topicPartition)
                attemptMasterLockWrite(topicPartition, globalSafeOffset, forcedReason = Some(reason))
            }
          } else {
            // Non-PARTITIONBY: Writer.commit() maintains the master lock; only advance in-memory state.
            // Defensively clear any dirty-window entry so a TP that transitions from PARTITIONBY
            // to non-PARTITIONBY mid-life cannot leak stale dirty state.
            metrics.clearMasterLockDirty(topicPartition)
            safeOffsetHighWatermarks.put(topicPartition, globalSafeOffset)
            advanceLastReturned(topicPartition, globalSafeOffset)
            forceWriteAfterCleanUp.remove(topicPartition)
            true
          }

        if (shouldReturnOffset)
          Some(new OffsetAndMetadata(
            globalSafeOffset,
            offsetAndMetadata.leaderEpoch(),
            offsetAndMetadata.metadata(),
          ))
        else
          None
      }
    }
  }

  /**
   * Attempt a single `updateMasterLock` write for the TP. Returns `true` on success
   * (offset returned to Connect), `false` on failure (no offset returned; dirty flag stays
   * true for the next retry). GC runs on success using the same `globalSafeOffset` local
   * (same-value GC threshold invariant).
   */
  private def attemptMasterLockWrite(
    topicPartition:   TopicPartition,
    globalSafeOffset: Long,
    forcedReason:     Option[ForcedWriteReason],
  ): Boolean = {
    val priorLastWritten = currentLastWrittenMaster(topicPartition)
    require(
      globalSafeOffset >= priorLastWritten,
      s"attemptMasterLockWrite invariant violated for $topicPartition: " +
        s"globalSafeOffset=$globalSafeOffset < lastWritten=$priorLastWritten — " +
        s"writing a regression would drive same-value GC past locks the next owner needs.",
    )
    forcedReason.foreach(metrics.incrementMasterLockWriteForced)
    indexManager.updateMasterLock(topicPartition, Offset(globalSafeOffset)) match {
      case Left(err) =>
        metrics.incrementMasterLockFailures()
        forcedReason.foreach(metrics.incrementMasterLockWriteForcedFailure)
        logger.error(
          s"[${connectorTaskId.show}] Master lock update failed for $topicPartition " +
            s"(forced=${forcedReason.map(_.toString).getOrElse("no")}): ${err.message()}. " +
            s"Returning no offset to prevent consumer advance.",
        )
        false
      case Right(_) =>
        metrics.incrementMasterLockUpdates()
        metrics.clearMasterLockDirty(topicPartition)
        safeOffsetHighWatermarks.put(topicPartition, globalSafeOffset)
        advanceLastReturned(topicPartition, globalSafeOffset)
        lastWrittenMasterSafeOffset.put(topicPartition, globalSafeOffset)
        logger.debug(
          s"[${connectorTaskId.show}] Updated master lock for $topicPartition with globalSafeOffset=$globalSafeOffset" +
            forcedReason.map(r => s" (forced=$r)").getOrElse(""),
        )
        val activePartitionKeys: Set[String] = writers
          .filter { case (key, _) => key.topicPartition == topicPartition }
          .keys
          .flatMap(key => WriterManager.derivePartitionKey(key.partitionValues))
          .toSet
        indexManager.cleanUpObsoleteLocks(topicPartition, Offset(globalSafeOffset), activePartitionKeys) match {
          case Left(err) =>
            logger.warn(s"[${connectorTaskId.show}] Best-effort GC failed for $topicPartition: ${err.message()}")
          case Right(_) =>
        }
        true
    }
  }

  /**
   * Monotonic-max update of `lastReturnedSafeOffset(tp)`. Must always be immediately preceded
   * by `safeOffsetHighWatermarks.put(tp, globalSafeOffset)` for the same value — the `require`
   * below enforces this so the `close()` bulk-clear safety argument remains valid.
   * See docs/datalake-exactly-once-partitionby.md ("`lastReturnedSafeOffset` Role by Mode").
   */
  private def advanceLastReturned(topicPartition: TopicPartition, globalSafeOffset: Long): Unit = {
    require(
      safeOffsetHighWatermarks.get(topicPartition).contains(globalSafeOffset),
      s"advanceLastReturned must follow safeOffsetHighWatermarks.put for $topicPartition: " +
        s"globalSafeOffset=$globalSafeOffset, HWM=${safeOffsetHighWatermarks.get(topicPartition)} — " +
        s"the two fields must advance in lockstep so the close() bulk-clear safety argument " +
        s"(lastReturned(tp) <= durableFloor(tp) + 1) survives any future refactor.",
    )
    val previous = lastReturnedSafeOffset.getOrElse(topicPartition, 0L)
    if (globalSafeOffset > previous) {
      val _ = lastReturnedSafeOffset.put(topicPartition, globalSafeOffset)
    }
  }

  def cleanUp(topicPartition: TopicPartition): Unit = {
    val keysToRemove = writers
      .view.filterKeys(_.topicPartition == topicPartition)
      .keys
      .toList
    keysToRemove.foreach { key =>
      writers.get(key).foreach(_.close())
      writers.remove(key)
      removeFromTpIndex(key)
    }
    safeOffsetHighWatermarks.remove(topicPartition)
    // Preserve `lastReturnedSafeOffset(tp)` across `cleanUp(tp)` so the HWM re-seed on the
    // next `preCommit` stays monotonic. See docs/datalake-exactly-once-partitionby.md
    // ("`lastReturnedSafeOffset` Role by Mode").
    lastWrittenMasterSafeOffset.remove(topicPartition)
    // The buffered records this floor covered have just been discarded, so they must be
    // re-delivered rather than skipped.
    maxBufferedOffset.remove(topicPartition)
    forceWriteAfterCleanUp.add(topicPartition)
    metrics.clearMasterLockDirty(topicPartition)
    // Evict cache so a fresh writer reloads its dedup floor from storage.
    indexManager.evictAllGranularLocks(topicPartition)
    // `clearTopicPartitionState` is NOT called: the partition is still owned by this task,
    // the master lock in storage is unchanged, and clearing `seekedOffsets` / the master eTag
    // would break the eTag fence on the next `updateMasterLock`. Only `IndexManagerV2.open()`
    // (stalePartitions / failure-rollback branches) calls `clearTopicPartitionState`.
  }

  private def evictIdleWriters(topicPartition: TopicPartition, exclude: Option[MapKey]): Unit = {
    val idleEntries = writers.iterator
      .filter { case (k, writer) => k.topicPartition == topicPartition && !exclude.contains(k) && writer.isIdle }
      .toList
    if (idleEntries.nonEmpty) {
      logger.debug(
        s"[${connectorTaskId.show}] Evicting ${idleEntries.size} idle writer(s) (map size ${writers.size})",
      )
    }
    idleEntries.foreach { case (key, writer) =>
      writer.close()
      writers.remove(key)
      removeFromTpIndex(key)
      WriterManager.derivePartitionKey(key.partitionValues).foreach { pk =>
        indexManager.evictGranularLock(key.topicPartition, pk)
      }
      metrics.incrementIdleWriterEvictions()
    }
    metrics.setWriterCount(writers.size)
  }

  private def addToTpIndex(key: MapKey, writer: Writer[SM]): Unit = {
    val bucket = tpIndex.getOrElseUpdate(key.topicPartition, mutable.LinkedHashMap.empty)
    val _      = bucket.put(key, writer)
  }

  private def removeFromTpIndex(key: MapKey): Unit =
    tpIndex.get(key.topicPartition).foreach { bucket =>
      bucket.remove(key)
      if (bucket.isEmpty) tpIndex.remove(key.topicPartition)
    }

  private[writer] def writerCount: Int = writers.size

  private[writer] def writerFor(key: MapKey): Option[Writer[SM]] = writers.get(key)

  private[writer] def putWriter(key: MapKey, writer: Writer[SM]): Unit = {
    val _ = writers.put(key, writer)
    addToTpIndex(key, writer)
  }

  private[writer] def evictIdleWritersNow(topicPartition: TopicPartition): Unit =
    evictIdleWriters(topicPartition, None)

  def shouldSkipNullValues(): Boolean = skipNullValues

}

object WriterManager {

  def sanitize(value: String): String =
    URLEncoder.encode(value, "UTF-8").replace("_", "%5F")

  def derivePartitionKey(partitionValues: Map[PartitionField, String]): Option[String] =
    if (partitionValues.isEmpty) None
    else {
      val key = partitionValues.toSeq
        .sortBy(_._1.name())
        .map { case (field, value) => s"${sanitize(field.name())}=${sanitize(value)}" }
        .mkString("_")
      Some(key)
    }
}
