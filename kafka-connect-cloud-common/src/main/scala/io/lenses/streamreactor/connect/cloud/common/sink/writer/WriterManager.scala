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
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.data.Schema

import java.io.File
import java.net.URLEncoder
import scala.collection.immutable
import scala.collection.mutable

case class MapKey(topicPartition: TopicPartition, partitionValues: immutable.Map[PartitionField, String])

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
  commitPolicyFn:              TopicPartition => Either[SinkError, CommitPolicy],
  bucketAndPrefixFn:           TopicPartition => Either[SinkError, CloudLocation],
  keyNamerFn:                  TopicPartition => Either[SinkError, KeyNamer],
  stagingFilenameFn:           (TopicPartition, Map[PartitionField, String]) => Either[SinkError, File],
  objKeyBuilderFn:             (TopicPartition, Map[PartitionField, String]) => ObjectKeyBuilder,
  formatWriterFn:              (TopicPartition, File) => Either[SinkError, FormatWriter],
  indexManager:                IndexManager,
  transformerF:                MessageDetail => Either[RuntimeException, MessageDetail],
  schemaChangeDetector:        SchemaChangeDetector,
  skipNullValues:              Boolean,
  pendingOperationsProcessors: PendingOperationsProcessors,
  metrics:                     CloudSinkMetrics = new CloudSinkMetrics(),
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends StrictLogging {

  private val writers             = mutable.LinkedHashMap.empty[MapKey, Writer[SM]]
  private val writerCommitManager = new WriterCommitManager[SM](() => writers.toMap)

  // High-watermark per TopicPartition: the highest globalSafeOffset ever reported to Kafka Connect.
  // Prevents regression when idle-writer eviction removes the writer that defined the previous
  // high watermark (see "globalSafeOffset regression" in docs/exactly-once-partitionby.md).
  private val safeOffsetHighWatermarks = mutable.Map.empty[TopicPartition, Long]

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

  def close(): Unit = {
    logger.debug(s"[{}] Received call to WriterManager.close", connectorTaskId.show)
    val topicPartitions = writers.keys.map(_.topicPartition).toSet
    writers.values.foreach(_.close())
    writers.clear()
    // Safe to clear all watermarks, including for retained partitions on rebalance:
    // getOffsetAndMeta re-initializes each partition's watermark from the master lock's
    // seeked offset (committedOffset + 1) on first preCommit after re-opening, which
    // equals the previous globalSafeOffset. No regression is possible.
    safeOffsetHighWatermarks.clear()
    // After closing all writers, evict their granular lock cache entries so that no stale
    // offsets or eTags linger in memory after the task stops. Writer.close() deliberately
    // does NOT evict cache entries (they are left for cleanUpObsoleteLocks to GC), so this
    // bulk eviction is the sole memory cleanup path on shutdown.
    //
    // NOTE: clearTopicPartitionState is deliberately NOT called here. It removes seekedOffsets
    // entries, which are needed by IndexManagerV2.close() → drainGcQueue() to avoid discarding
    // queued GC items as "partition no longer owned." Neither CloudSinkTask.close() nor stop()
    // calls clearTopicPartitionState; the state is GC'd with the indexManager reference when
    // stop() sets it to null.
    topicPartitions.foreach { tp =>
      indexManager.evictAllGranularLocks(tp)
    }
  }

  def write(topicPartitionOffset: TopicPartitionOffset, messageDetail: MessageDetail): Either[SinkError, Unit] = {

    logger.debug(
      s"[${connectorTaskId.show}] Received call to WriterManager.write for ${topicPartitionOffset.topic}-${topicPartitionOffset.partition}:${topicPartitionOffset.offset}",
    )
    for {
      writer    <- writer(topicPartitionOffset.toTopicPartition, messageDetail)
      shouldSkip = writer.shouldSkip(topicPartitionOffset.offset)
      resultIfNotSkipped <-
        if (!shouldSkip) {
          transformerF(messageDetail).leftMap(ex =>
            new FatalCloudSinkError(ex.getMessage, ex.some, topicPartitionOffset.toTopicPartition),
          ).flatMap { transformed =>
            writeAndCommit(topicPartitionOffset, transformed, writer)
          }
        } else {
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
      _         <- writer.write(messageDetail)
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
              evictIdleWritersIfNeeded(key)
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
      _            <- partitionKey.fold(().asRight[SinkError])(pk => indexManager.ensureGranularLock(topicPartition, pk))
      lastSeekedOffset <- partitionKey match {
        case Some(pk) =>
          indexManager.getSeekedOffsetForPartitionKey(topicPartition, pk)
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
    writers
      .collect {
        case (key, writer) if key.topicPartition == topicPartition => writer
      }
      .toSeq

  private def getOffsetAndMeta(
    topicPartition:    TopicPartition,
    offsetAndMetadata: OffsetAndMetadata,
  ): Option[OffsetAndMetadata] = {
    val tpWriters = writersForTopicPartition(topicPartition)
    if (tpWriters.isEmpty) return None

    val firstBufferedOffsets = tpWriters.flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = tpWriters.flatMap(_.getCommittedOffset)

    if (committedOffsets.isEmpty) return None

    val calculatedSafeOffset =
      if (firstBufferedOffsets.nonEmpty) {
        firstBufferedOffsets.map(_.value).min
      } else {
        committedOffsets.map(_.value).max + 1
      }

    val previousHighWatermark = safeOffsetHighWatermarks.getOrElseUpdate(
      topicPartition,
      indexManager.getSeekedOffsetForTopicPartition(topicPartition)
        .map(_.value + 1)
        .getOrElse(0L),
    )
    val globalSafeOffset = math.max(calculatedSafeOffset, previousHighWatermark)

    val hasPartitionByWriters =
      writers.keys.exists(k => k.topicPartition == topicPartition && k.partitionValues.nonEmpty)

    if (hasPartitionByWriters) {
      // PARTITIONBY mode: writers update granular locks, so the master lock needs a separate write.
      indexManager.updateMasterLock(topicPartition, Offset(globalSafeOffset)) match {
        case Left(err) =>
          metrics.incrementMasterLockFailures()
          logger.error(
            s"[${connectorTaskId.show}] Master lock update failed for $topicPartition: ${err.message()}. " +
              s"Returning no offset to prevent consumer advance.",
          )
          return None
        case Right(_) =>
          metrics.incrementMasterLockUpdates()
          safeOffsetHighWatermarks.put(topicPartition, globalSafeOffset)
          logger.debug(
            s"[${connectorTaskId.show}] Updated master lock for $topicPartition with globalSafeOffset=$globalSafeOffset",
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
      }
    } else {
      // Non-PARTITIONBY mode: Writer.commit() already maintains the master lock via
      // indexManager.update(), so skip the redundant cloud write.
      safeOffsetHighWatermarks.put(topicPartition, globalSafeOffset)
    }

    Some(new OffsetAndMetadata(
      globalSafeOffset,
      offsetAndMetadata.leaderEpoch(),
      offsetAndMetadata.metadata(),
    ))
  }

  def cleanUp(topicPartition: TopicPartition): Unit = {
    val keysToRemove = writers
      .view.filterKeys(_.topicPartition == topicPartition)
      .keys
      .toList
    keysToRemove.foreach { key =>
      writers.get(key).foreach(_.close())
      writers.remove(key)
    }
    safeOffsetHighWatermarks.remove(topicPartition)
    // Evict all remaining granular lock entries for this partition from the cache. This is
    // necessary after a rebalance: if the partition is later re-assigned to this task, the
    // new writers must load fresh lock state from storage rather than reading stale cached data.
    indexManager.evictAllGranularLocks(topicPartition)
    indexManager.clearTopicPartitionState(topicPartition)
  }

  private def evictIdleWritersIfNeeded(exclude: MapKey): Unit = {
    val idleEntries = writers.iterator
      .filter { case (k, writer) => k != exclude && k.topicPartition == exclude.topicPartition && writer.isIdle }
      .toList
    if (idleEntries.nonEmpty) {
      logger.debug(
        s"[${connectorTaskId.show}] Evicting ${idleEntries.size} idle writer(s) (map size ${writers.size})",
      )
    }
    idleEntries.foreach { case (key, writer) =>
      writer.close()
      writers.remove(key)
      WriterManager.derivePartitionKey(key.partitionValues).foreach { pk =>
        indexManager.evictGranularLock(key.topicPartition, pk)
      }
      metrics.incrementIdleWriterEvictions()
    }
    metrics.setWriterCount(writers.size)
  }

  private[writer] def writerCount: Int = writers.size

  private[writer] def putWriter(key: MapKey, writer: Writer[SM]): Unit = { val _ = writers.put(key, writer) }

  private[writer] def evictIdleWritersNow(topicPartition: TopicPartition): Unit =
    evictIdleWritersIfNeeded(MapKey(topicPartition, immutable.Map.empty))

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
