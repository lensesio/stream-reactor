/*
 * Copyright 2017-2024 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext

import java.time.Clock
import java.time.Duration
import java.time.Instant

trait BatchPolicyCondition {
  def eval(context: CommitContext, debugEnabled: Boolean): BatchConditionCommitResult
}

case class BatchConditionCommitResult(batchResult: BatchResult, logLine: Option[String])

case class Count(minCount: Long) extends BatchPolicyCondition {
  override def eval(commitContext: CommitContext, debugEnabled: Boolean): BatchConditionCommitResult = {

    val continueWhen = commitContext.count <= minCount
    val triggerWhen  = commitContext.count >= minCount
    val flushing     = if (triggerWhen) "*" else ""

    val log = Option.when(debugEnabled) {
      s"count$flushing: '${commitContext.count}/$minCount'"
    }

    BatchConditionCommitResult(BatchResult(continueWhen, triggerWhen, false), log)
  }
}

case class FileSize(minFileSize: Long) extends BatchPolicyCondition with LazyLogging {
  override def eval(context: CommitContext, debugEnabled: Boolean): BatchConditionCommitResult = {
    val continueWhen = context.fileSize <= minFileSize
    val triggerWhen  = context.fileSize >= minFileSize
    val logLine = Option.when(debugEnabled) {
      val flushing = if (triggerWhen) "*" else ""
      s"fileSize$flushing: '${context.fileSize}/$minFileSize'"
    }
    BatchConditionCommitResult(BatchResult(continueWhen, triggerWhen, false), logLine)
  }
}

case class Interval(interval: Duration, clock: Clock) extends BatchPolicyCondition with LazyLogging {

  override def eval(context: CommitContext, debugEnabled: Boolean): BatchConditionCommitResult = {
    val nowInstant = clock.instant()

    val lastWriteInstant = Instant.ofEpochMilli(context.lastModified)
    val nextFlushTime    = lastWriteInstant.plus(interval)
    val continueWhen     = nowInstant.isBefore(nextFlushTime) || nowInstant.equals(nextFlushTime)
    val triggerWhen      = nowInstant.isAfter(nextFlushTime) || nowInstant.equals(nextFlushTime)

    val logLine = Option.when(debugEnabled) {
      val flushing      = if (triggerWhen) "*" else ""
      val timeRemaining = nextFlushTime.getEpochSecond - nowInstant.getEpochSecond
      s"interval$flushing: {frequency:${interval.toSeconds}s, in:${timeRemaining}s, lastFlush:${lastWriteInstant.toString.substring(0,
                                                                                                                                    19,
      )}, nextFlush:${nextFlushTime.toString.substring(0, 19)}}"
    }
    BatchConditionCommitResult(BatchResult(continueWhen, triggerReached = false, greedyTriggerReached = triggerWhen),
                               logLine,
    )
  }
}
