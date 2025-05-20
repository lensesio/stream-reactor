/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.http.sink.commit

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

  def shouldBatch(context: CommitContext): BatchResult = {

    val res                  = conditions.map(_.eval(context, debugEnabled = true))
    val triggerReached       = res.exists(_.batchResult.triggerReached)
    val fitsInBatch          = res.map(_.batchResult.fitsInBatch).distinct.headOption.contains(true)
    val greedyTriggerReached = res.exists(_.batchResult.greedyTriggerReached)

    //if (triggerReached) {
    logger.info(generateLogLine(triggerReached, res))
    //}

    BatchResult(fitsInBatch, triggerReached, greedyTriggerReached)

  }
  def generateLogLine(flushing: Boolean, result: Seq[BatchConditionCommitResult]): String = {
    val flushingOrNot = if (flushing) "" else "Not "
    s"${flushingOrNot}Flushing for {${result.flatMap(_.logLine).mkString(", ")}}"
  }

}

object BatchPolicy extends LazyLogging {
  def apply(conditions: BatchPolicyCondition*): BatchPolicy =
    BatchPolicy(logger, conditions: _*)
}

case class BatchResult(
  fitsInBatch:          Boolean,
  triggerReached:       Boolean,
  greedyTriggerReached: Boolean, // room for more
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
