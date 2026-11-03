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
package io.lenses.streamreactor.connect.http.sink

import cats.data.NonEmptySeq
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.BatchPolicyCondition
import io.lenses.streamreactor.common.batch.BatchResult
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.common.batch.Interval
import io.lenses.streamreactor.common.batch.OffsetMergeUtils
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.ConditionCommitResult
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord

import scala.collection.mutable

/**
 * Incremental, single-consumer batch builder for the HTTP sink.
 *
 * Records are pushed one at a time via [[offer]] and the configured [[BatchPolicy]] is evaluated
 * against a running commit context. This is the streaming equivalent of
 * `io.lenses.streamreactor.common.batch.RecordsQueueBatcher.takeBatch`: the per-record policy
 * evaluation is identical, but it is driven by a push of individual records.
 *
 * This type is deliberately '''mutable and not thread-safe''': it is owned by a single consumer
 * fiber (one per topic), so no synchronisation is required. Mutability keeps the hot path
 * allocation-free -- records accumulate in a reusable buffer and the policy is evaluated against a
 * single reused [[CommitContext]] whose `count`/`fileSize` are updated in place. The policy
 * conditions read only `count`/`fileSize`/`lastModified` (never `committedOffsets`), so the
 * per-partition max offsets are merged once, at flush time, rather than on every record.
 *
 * The full configurable policy surface is preserved: `Count`, `FileSize` and `Interval` (including
 * the greedy-interval semantics). The interval is measured from the last flush
 * (`HttpCommitContext.lastFlushedTimestamp`, falling back to `createdTimestamp`).
 */
final class BatchAccumulator(
  batchPolicy:    BatchPolicy,
  initialContext: HttpCommitContext,
) {

  private val recordBuffer = mutable.ArrayBuffer.empty[RenderedRecord]
  private var carried:     HttpCommitContext = initialContext
  private var recordCount: Long              = 0L
  private var recordBytes: Long              = 0L
  private var triggeredConditions = Set.empty[BatchPolicyCondition]

  private val intervalConditionOpt: Option[Interval] =
    batchPolicy.conditions.collectFirst { case interval: Interval => interval }

  private val intervalMillisOpt: Option[Long] =
    intervalConditionOpt.map { case Interval(interval, _) => interval.toMillis }

  // Reused across offer() calls; safe because a single fiber owns this accumulator.
  private val evalContext = new BatchAccumulator.MutableCommitContext
  evalContext.createdTimestamp     = carried.createdTimestamp
  evalContext.lastFlushedTimestamp = carried.lastFlushedTimestamp

  def isEmpty:  Boolean = recordBuffer.isEmpty
  def nonEmpty: Boolean = recordBuffer.nonEmpty
  def size:     Int     = recordBuffer.size

  /**
   * Offers a record and returns the policy decision for the candidate batch (current batch plus the
   * record). The record is appended only when the decision reports `fitsInBatch`.
   */
  def offer(record: RenderedRecord): BatchResult = {
    evalContext.count    = recordCount + 1L
    evalContext.fileSize = recordBytes + record.length.toLong
    val result = batchPolicy.shouldBatch(evalContext)
    triggeredConditions ++= result.triggeredConditions
    if (result.fitsInBatch) {
      recordBuffer += record
      recordCount += 1L
      recordBytes += record.length.toLong
    }
    result
  }

  /** Conditions satisfied while selecting the current batch; these are non-exclusive metrics. */
  def flushTriggers: Set[BatchPolicyCondition] = triggeredConditions

  /** Records a timer-driven flush, which occurs without evaluating a candidate record. */
  def markIntervalTrigger(): Unit =
    intervalConditionOpt.foreach(triggeredConditions += _)

  /**
   * Emits the batch policy's once-per-flush explanation for the batch currently accumulated. Unlike
   * `offer`, which leaves `evalContext` holding the last candidate values (`recordCount + 1`), this
   * sets the context from the actual accumulated totals so the logged counts match the batch that is
   * really flushed.
   */
  def logFlush(): Unit = logFlushWith(recordCount, recordBytes)

  /**
   * Emits the explanation for a single record flushed on its own (an oversized record that did not
   * fit even an empty batch).
   */
  def logFlushSingle(record: RenderedRecord): Unit = logFlushWith(1L, record.length.toLong)

  private def logFlushWith(count: Long, bytes: Long): Unit = {
    evalContext.count    = count
    evalContext.fileSize = bytes
    batchPolicy.logFlush(evalContext)
  }

  /**
   * The accumulated batch, if any records have been added.
   */
  def currentBatch: Option[NonEmptySeq[RenderedRecord]] =
    NonEmptySeq.fromSeq(recordBuffer.toVector)

  /**
   * The commit context to persist after a successful flush of the current buffer: the batch's
   * per-partition max offsets are merged into the carried committed offsets, counters are reset and
   * `lastFlushedTimestamp` is stamped.
   */
  def flushedContext(): HttpCommitContext =
    OffsetMergeUtils.updateCommitContextPostCommit(
      OffsetMergeUtils.createCommitContextForEvaluation(recordBuffer.toSeq, carried),
    )

  /**
   * The commit context to persist after flushing a single record on its own (e.g. an oversized
   * record that does not fit an empty batch).
   */
  def candidateContextFor(record: RenderedRecord): HttpCommitContext =
    OffsetMergeUtils.updateCommitContextPostCommit(
      OffsetMergeUtils.createCommitContextForEvaluation(Seq(record), carried),
    )

  /**
   * Clears the buffer and adopts the given (post-flush) commit context. Use after a successful
   * flush, or to drop a failed batch without advancing offsets.
   */
  def resetTo(context: HttpCommitContext): Unit = {
    recordBuffer.clear()
    recordCount                      = 0L
    recordBytes                      = 0L
    triggeredConditions              = Set.empty
    carried                          = context
    evalContext.createdTimestamp     = context.createdTimestamp
    evalContext.lastFlushedTimestamp = context.lastFlushedTimestamp
  }

  /**
   * The wall-clock millisecond at which a time-based flush becomes due, or `None` when the policy
   * has no `Interval` condition (the consumer then blocks until the next record arrives).
   */
  def nextFlushDeadlineMillis: Option[Long] =
    intervalMillisOpt.map(millis => carried.lastFlushedTimestamp.getOrElse(carried.createdTimestamp) + millis)
}

object BatchAccumulator {

  /**
   * A minimal mutable [[CommitContext]] used only to feed `count`/`fileSize`/`lastModified` into the
   * batch policy. `committedOffsets` and `generateLogLine` are never consulted by
   * `BatchPolicy.shouldBatch`, so they are intentionally not modelled here.
   */
  private final class MutableCommitContext extends CommitContext {
    var count:                Long         = 0L
    var fileSize:             Long         = 0L
    var createdTimestamp:     Long         = 0L
    var lastFlushedTimestamp: Option[Long] = None

    override def generateLogLine(flushing: Boolean, result: Seq[ConditionCommitResult]): String = ""
  }
}
