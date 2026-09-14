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

import cats.data.NonEmptyList
import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata

import java.util.UUID

/**
 * Abstraction over the writer map that lets [[WriterCommitManager]] iterate writers without
 * ever materialising a full immutable snapshot.  The two methods return live iterators backed
 * directly by the mutable data structures held in [[WriterManager]]:
 *
 *  - `iterator`                       — all writers (used by the TP-agnostic paths).
 *  - `iteratorForTopicPartition(tp)`  — only writers keyed to `tp` (used by the per-TP paths,
 *                                       O(siblings on TP) rather than O(all writers)).
 *
 * Callers must not mutate the underlying structures while consuming an iterator.  This is safe
 * because [[WriterManager]] is documented as non-thread-safe and all mutations happen on the
 * same thread as commit calls.
 */
private[writer] trait WriterSource[SM <: FileMetadata] {
  def iterator: Iterator[(MapKey, Writer[SM])]
  def iteratorForTopicPartition(tp: TopicPartition): Iterator[(MapKey, Writer[SM])]
}

/**
 * Manages the commit operations for writers.
 *
 * Selective-commit contract in `CommitMode.Granular` (see the "Selective commit fan-out"
 * subsection in `docs/datalake-exactly-once-partitionby.md`):
 *
 *   - `commitFlushableWriters` / `commitFlushableWritersForTopicPartition` commit ONLY
 *     writers where `shouldFlush == true`. Sibling writers on the same `TopicPartition`
 *     stay open. The previous fan-out-on-every-flush behaviour is gone.
 *   - `commitPending` commits ONLY writers in `Uploading` state (`hasPendingUpload == true`).
 *     Writing-state siblings are not opportunistically force-flushed by the pending-retry
 *     path; this avoids dragging unrelated writers into a commit cycle that they did not
 *     trigger.
 *   - `commitForTopicPartition` is the only path that still fans out across every writer
 *     for a `TopicPartition`. It is reserved for schema rollover, where every sibling must
 *     flush together to preserve format-boundary semantics. Do NOT replace this with a selective filter.
 *
 * `CommitMode.Batch` does NOT follow the selective contract above: every entry point routes
 * through `commitBatch`, which stages and commits every non-idle writer on the topic-partition
 * together (a `Writing` sibling is not left open the way it would be in granular mode) --
 * routing-independence requires all writers on a partition to advance the master lock as one
 * unit. See `commitBatch`'s doc for the full protocol.
 *
 * Safety: `WriterManager.getOffsetAndMeta` continues to scan EVERY active writer on the
 * topic-partition when computing `globalSafeOffset`, so the consumer-committable offset
 * is bounded by the slowest active writer's `firstBufferedOffset` even when only a subset
 * of writers commit. Selective commit reduces the *CommitSet* but never the *BarrierSet*.
 *
 * @param source          Provides live iterators over the writer map without snapshot allocation.
 * @param connectorTaskId Implicit task ID for logging purposes.
 * @tparam SM Type parameter for file metadata.
 */
class WriterCommitManager[SM <: FileMetadata](
  source:                      WriterSource[SM],
  indexManager:                IndexManager,
  pendingOperationsProcessors: PendingOperationsProcessors,
  commitMode:                  CommitMode       = CommitMode.Default,
  metrics:                     CloudSinkMetrics = new CloudSinkMetrics(),
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends StrictLogging {

  /**
   * Commits writers that have pending uploads (state == `Uploading`).
   *
   * Selective: only the writers that match the predicate are committed. Sibling Writing
   * writers on the same `TopicPartition` are left open — they will commit when their own
   * flush trigger fires. Every Uploading writer still retries on every call, so there is
   * no starvation.
   *
   * In batch mode this instead re-drives `commitBatch` for every topic-partition that has a
   * writer mid-commit; the batch by construction includes that partition's `Writing` siblings.
   */
  def commitPending(): Either[SinkError, Unit] =
    commitMode match {
      case CommitMode.Granular =>
        commitFromIterator(source.iterator.filter { case (_, w) => w.hasPendingUpload })
      case CommitMode.Batch =>
        commitBatches(topicPartitionsWhere(_.hasPendingUpload))
    }

  /**
   * Commits every writer for `topicPartition`, irrespective of state or flush threshold.
   *
   * This is the schema-rollover full-fan-out path: when a record with an incompatible
   * schema arrives, every sibling writer on the topic-partition must flush together so
   * the format boundary is consistent. Do NOT replace this with a selective filter.
   */
  def commitForTopicPartition(topicPartition: TopicPartition): Either[BatchCloudSinkError, Unit] =
    commitMode match {
      case CommitMode.Granular => commitFromIterator(source.iteratorForTopicPartition(topicPartition))
      case CommitMode.Batch    => commitBatches(Set(topicPartition))
    }

  /**
   * Commits writers that should be flushed, across all topic-partitions. Selective:
   * non-flushable siblings keep buffering.
   */
  def commitFlushableWriters(): Either[BatchCloudSinkError, Unit] =
    commitMode match {
      case CommitMode.Granular => commitFromIterator(source.iterator.filter { case (_, w) => w.shouldFlush })
      case CommitMode.Batch    => commitBatches(topicPartitionsWhere(w => w.shouldFlush || w.hasPendingUpload))
    }

  /**
   * Commits writers that should be flushed for a specific topic partition. Selective:
   * non-flushable siblings on the same topic-partition keep buffering.
   *
   * Uses `iteratorForTopicPartition` so only the O(siblings on TP) entries are visited;
   * no full-map scan or snapshot allocation occurs.
   */
  def commitFlushableWritersForTopicPartition(topicPartition: TopicPartition): Either[BatchCloudSinkError, Unit] =
    commitMode match {
      case CommitMode.Granular =>
        commitFromIterator(
          source.iteratorForTopicPartition(topicPartition).filter { case (_, w) => w.shouldFlush },
        )
      case CommitMode.Batch =>
        val trigger =
          source.iteratorForTopicPartition(topicPartition).exists { case (_, w) => w.shouldFlush || w.hasPendingUpload }
        if (trigger) commitBatches(Set(topicPartition)) else ().asRight
    }

  private def topicPartitionsWhere(p: Writer[SM] => Boolean): Set[TopicPartition] =
    source.iterator.collect { case (key, w) if p(w) => key.topicPartition }.toSet

  private def commitBatches(topicPartitions: Set[TopicPartition]): Either[BatchCloudSinkError, Unit] = {
    // Flatten: `BatchCloudSinkError.apply` only recognises Fatal / NonFatal leaves. Nesting one
    // batch error inside another loses its `rollBack()` / `topicPartitions()` and gets
    // reclassified as a single opaque NonFatal leaf (swallowable under NOOP) instead of
    // preserving each constituent's own Fatal/NonFatal classification -- flattening keeps every
    // error's original severity intact.
    val errors = topicPartitions.toList.flatMap(tp => commitBatch(tp).left.toOption).flatMap {
      case b: BatchCloudSinkError => b.fatal.toList ++ b.nonFatal.toList
      case other => List(other)
    }
    Either.cond(errors.isEmpty, (), BatchCloudSinkError(errors.toSet))
  }

  /**
   * Commits every non-idle writer on `topicPartition` as one unit.
   *
   * The protocol, and why each step is where it is:
   *
   *  1. Stage all writers. Uploads happen before anything durable is recorded, so a failure here
   *     leaves no trace beyond orphan temps and every writer keeps its state and staging file.
   *     Results are collected rather than short-circuited: staging is idempotent, so doing the
   *     work now minimises what a retry has to redo.
   *  2. CAS a single `PendingState(P, [Copy x N])` onto the master lock. This is the commit point;
   *     it is also the fence, because the write is conditional on the master eTag.
   *  3. Drive the chain. Each copy that succeeds rewrites the lock with the remaining ops, so a
   *     crash anywhere is resumable by `IndexManagerV2.open`.
   *  4. Finalise the writers.
   *
   * The chain deliberately contains only `CopyOperation`s. An `UploadOperation` would make
   * `updateEtag` overwrite every per-file eTag with the last upload's, and a `DeleteOperation`
   * would put a delete mid-chain where a failure is Fatal and would block rollback recovery.
   * There is deliberately no post-chain temp delete: every `CopyOperation` is executed via
   * `storageInterface.mvFile`, which MOVES the object on all three backends (copy + delete
   * source) -- by the time the chain returns `Right`, every temp is already gone. Any temp left
   * behind by a partial failure becomes a candidate for the `.temp-upload` orphan sweep.
   */
  private[writer] def commitBatch(topicPartition: TopicPartition): Either[SinkError, Unit] = {
    val writers = source.iteratorForTopicPartition(topicPartition).map(_._2).filterNot(_.isIdle).toList
    if (writers.isEmpty) ().asRight
    else {
      val commitStartNanos = System.nanoTime()
      val batchUuid        = UUID.randomUUID().toString
      val results          = writers.map(_.stage(batchUuid))
      val stageErrors      = results.collect { case Left(err) => err }.toSet
      if (stageErrors.nonEmpty) {
        metrics.incrementBatchCommitFailures()
        metrics.incrementFilesFailedTotal()
        BatchCloudSinkError(stageErrors).asLeft
      } else {
        val staged = results.collect { case Right(Some(s)) => s }
        if (staged.isEmpty) ().asRight
        else {
          val pendingOffset = staged.map(_.uncommittedOffset).max
          // Always the durable master floor, never a writer's own committedOffset: a writer that
          // has been idle since an earlier episode can carry a stale value.
          val committed = indexManager.getSeekedOffsetForTopicPartition(topicPartition)
          val pending   = PendingState(pendingOffset, NonEmptyList.fromListUnsafe(staged.map(_.copyOp)))

          indexManager.update(topicPartition, committed, pending.some) match {
            case Left(err) =>
              // Fatal (eTag mismatch or storage failure). Temps and staging files stay put: the
              // restart replays from `committed` and the temps become sweep candidates.
              metrics.incrementBatchCommitFailures()
              metrics.incrementFilesFailedTotal()
              logger.error(
                s"[${connectorTaskId.show}] Batch commit CAS failed for $topicPartition at " +
                  s"pendingOffset=${pendingOffset.value}: ${err.message()}",
              )
              err.asLeft
            case Right(_) =>
              pendingOperationsProcessors.processPendingOperations(
                topicPartition,
                committed,
                pending,
                indexManager.update,
                escalateOnCancel = true,
                partitionKey     = None,
                stagingFile      = None,
              ).map { _ =>
                writers.foreach(_.finalizeCommit(pendingOffset))
                metrics.incrementBatchCommits()
                metrics.addBatchCommitFiles(staged.size.toLong)
                metrics.recordCommitTimer((System.nanoTime() - commitStartNanos) / 1_000_000L)
                // Legacy granular-lock purge hook -- a no-op on IndexManagerV2 (purge now
                // happens synchronously in batch-mode open()); kept so this call site does not
                // need a mode branch.
                indexManager.afterBatchCommit(topicPartition, pendingOffset)
              }.leftMap { err =>
                metrics.incrementBatchCommitFailures()
                metrics.incrementFilesFailedTotal()
                err
              }
          }
        }
      }
    }
  }

  /**
   * Drives commits for every entry in `iter`, collects errors, and returns a single
   * `BatchCloudSinkError` if any writer failed (preserving the pre-existing classification
   * semantics: `fatal` vs `nonFatal`, `rollBack()`, `topicPartitions()`).
   * A failure in one writer does not prevent the remaining writers from being committed.
   */
  private def commitFromIterator(
    iter: Iterator[(MapKey, Writer[SM])],
  ): Either[BatchCloudSinkError, Unit] = {
    val errors = iter
      .map { case (_, w) => w.commit }
      .collect { case Left(err) => err }
      .toSet
    Either.cond(errors.isEmpty, (), BatchCloudSinkError(errors))
  }
}
