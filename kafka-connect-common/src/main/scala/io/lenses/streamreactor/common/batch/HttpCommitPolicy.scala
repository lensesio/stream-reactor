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
package io.lenses.streamreactor.common.batch

import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext

import java.time.Clock
import java.time.Duration

/**
 * The [[BatchPolicy]] is responsible for determining when
 * a sink partition (a single file) should be flushed (closed on disk, and moved to be visible).
 *
 * By default, it flushes based on configurable conditions such as number of records, file size,
 * or time since the file was last flushed.
 *
 * @param conditions the conditions to evaluate for flushing the partition
 */
case class BatchPolicy(logger: Logger, conditions: BatchPolicyCondition*) {

  // A policy with no conditions never triggers (`fitsInBatch` defaults to true, nothing sets a
  // trigger) and has no interval deadline, so the accumulator would grow until backpressure permits
  // are exhausted and producers fail with `RetriableException` indefinitely -- a silent stall. This
  // is unreachable in production (`BatchConfig.toBatchPolicy` falls back to `HttpBatchPolicy.Default`
  // and Cosmos `FlushSettings` always emits `FileSize` + `Interval`); the guard makes the invariant
  // explicit. Mockito mocks bypass the constructor, so this does not affect tests that mock the policy.
  require(conditions.nonEmpty, "BatchPolicy requires at least one condition")

  def shouldBatch(context: CommitContext): BatchResult = {
    // Hot path: fold the conditions with three flags and no log strings. A small set is retained
    // only when one or more conditions are satisfied, so the HTTP sink can attribute its
    // non-exclusive, per-submission trigger metrics to every satisfied condition.
    //
    // `triggerReached`/`greedyTriggerReached` are ORed (any condition reaching its limit is enough to
    // flush); `fitsInBatch` is ANDed (the record fits only if *every* condition agrees it does), so
    // the result is independent of condition order. `Count`/`FileSize` only report
    // `fitsInBatch = false` together with `triggerReached = true` (the record that overshoots the
    // limit), so a rejected record always coincides with a flush; `Interval` always fits. Taking a
    // single condition's `fitsInBatch` (e.g. only the first) would let condition order decide whether
    // a record is rejected -- the defect that made an interval-only policy send one request per
    // record.
    var triggerReached       = false
    var greedyTriggerReached = false
    var fitsInBatch          = true
    var triggeredConditions  = Set.empty[BatchPolicyCondition]
    conditions.foreach { condition =>
      val r = condition.eval(context)
      if (r.triggerReached) triggerReached             = true
      if (r.greedyTriggerReached) greedyTriggerReached = true
      if (!r.fitsInBatch) fitsInBatch                  = false
      if (r.triggerReached || r.greedyTriggerReached) triggeredConditions += condition
    }
    BatchResult(fitsInBatch, triggerReached, greedyTriggerReached, triggeredConditions)
  }

  /**
   * Emits the once-per-flush explanation at INFO, describing the batch actually being sent. Called
   * by the flush sites rather than by [[shouldBatch]], which only ever sees the candidate batch
   * (current batch plus the record under consideration) and so cannot describe what is really
   * flushed. The scala-logging macro gates both the string build and the write on `isInfoEnabled`.
   */
  def logFlush(context: CommitContext): Unit =
    logger.info(generateLogLine(conditions.map(_.explain(context))))

  def generateLogLine(explanations: Seq[String]): String =
    s"Flushing for {${explanations.mkString(", ")}}"

}

object BatchPolicy extends LazyLogging {
  def apply(conditions: BatchPolicyCondition*): BatchPolicy =
    BatchPolicy(logger, conditions: _*)
}

case class BatchResult(
  fitsInBatch:    Boolean,
  triggerReached: Boolean,
  // A soft trigger: a time-based deadline has passed, so the batch may be flushed at the next
  // convenient point (after packing anything already queued) rather than being forced immediately
  // like `triggerReached`.
  greedyTriggerReached: Boolean,
  // Conditions satisfied by this candidate. Multiple conditions can be present; HTTP submission
  // metrics deliberately count each satisfied condition rather than selecting a single winner.
  triggeredConditions: Set[BatchPolicyCondition] = Set.empty,
)

object HttpBatchPolicy extends LazyLogging {

  private val defaultFlushSize     = 500_000_000L
  private val defaultFlushInterval = Duration.ofSeconds(3600)
  private val defaultFlushCount    = 50_000L

  val Default: BatchPolicy =
    BatchPolicy(FileSize(defaultFlushSize),
                Interval(defaultFlushInterval, Clock.systemDefaultZone()),
                Count(defaultFlushCount),
    )

}
