/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.sink.commit

import com.typesafe.scalalogging.LazyLogging

import java.time.Clock
import java.time.Duration
import java.time.Instant
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

case class ConditionCommitResult(commitTriggered: Boolean, logLine: Option[String])

trait CommitPolicyCondition {
  def eval(context: CommitContext, debugEnabled: Boolean): ConditionCommitResult
}

case class FileSize(maxFileSize: Long) extends CommitPolicyCondition with LazyLogging {
  override def eval(context: CommitContext, debugEnabled: Boolean): ConditionCommitResult = {
    val cond = context.fileSize >= maxFileSize
    val logLine = Option.when(debugEnabled) {
      val flushing = if (cond) "*" else ""
      s"fileSize$flushing: '${context.fileSize}/$maxFileSize'"
    }
    ConditionCommitResult(cond, logLine)
  }
}

object Interval {

  def apply(scalaInterval: FiniteDuration): Interval = {
    val interval = scalaInterval.toJava
    val clock    = Clock.systemDefaultZone()
    Interval(interval, clock)
  }
}

case class Interval(interval: Duration, clock: Clock) extends CommitPolicyCondition {

  override def eval(context: CommitContext, debugEnabled: Boolean): ConditionCommitResult = {
    val nowInstant = clock.instant()

    val lastWriteInstant = Instant.ofEpochMilli(context.lastModified)
    val nextFlushTime    = lastWriteInstant.plus(interval)
    val nextFlushDue     = nowInstant.isAfter(nextFlushTime) || nowInstant.equals(nextFlushTime)

    val logLine = Option.when(debugEnabled) {
      val flushing      = if (nextFlushDue) "*" else ""
      val timeRemaining = nextFlushTime.getEpochSecond - nowInstant.getEpochSecond
      s"interval$flushing: {frequency:${interval.toSeconds}s, in:${timeRemaining}s, lastFlush:${lastWriteInstant.toString.substring(0,
                                                                                                                                    19,
      )}, nextFlush:${nextFlushTime.toString.substring(0, 19)}}"
    }
    ConditionCommitResult(nextFlushDue, logLine)
  }
}

case class Count(maxCount: Long) extends CommitPolicyCondition {
  override def eval(commitContext: CommitContext, debugEnabled: Boolean): ConditionCommitResult = {

    val cond     = commitContext.count >= maxCount
    val flushing = if (cond) "*" else ""

    val log = Option.when(debugEnabled) {
      s"count$flushing: '${commitContext.count}/$maxCount'"
    }

    ConditionCommitResult(cond, log)
  }
}
