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
trait CommitPolicyCondition {
  def eval(context: CommitContext, debugEnabled: Boolean): (Boolean, String)
}

case class FileSize(maxFileSize: Long) extends CommitPolicyCondition with LazyLogging {
  override def eval(context: CommitContext, debugEnabled: Boolean): (Boolean, String) = {
    val cond = context.fileSize >= maxFileSize
    val logLine = if (debugEnabled) {
      val flushing = if (cond) "*" else ""
      s"fileSize$flushing: '${context.fileSize}/$maxFileSize'"
    } else ""
    (cond, logLine)
  }
}

object Interval {
  def apply(scalaInterval: FiniteDuration): Interval = {
    val interval = scalaInterval.toJava
    Interval(interval)
  }
}

case class Interval(interval: Duration) extends CommitPolicyCondition {

  private val clock = Clock.systemDefaultZone()

  override def eval(context: CommitContext, debugEnabled: Boolean): (Boolean, String) = {
    val nowInstant = clock.instant()

    val lastWriteTimestamp: Long = context.lastFlushedTimestamp.getOrElse(context.createdTimestamp)
    val lastWriteInstant = Instant.ofEpochMilli(lastWriteTimestamp)
    val nextFlushTime    = lastWriteInstant.plus(interval)
    val nextFlushDue     = nextFlushTime.isBefore(nowInstant)

    val logLine = if (debugEnabled) {
      val flushing = if (nextFlushDue) "*" else ""

      s"interval$flushing: {every ${interval.toSeconds}s, lastFlush:${lastWriteInstant.toString.substring(0, 19)}, nextFlush:${nextFlushTime.toString.substring(0, 19)}}"
    } else ""
    (nextFlushDue, logLine)
  }
}

case class Count(maxCount: Long) extends CommitPolicyCondition {
  override def eval(commitContext: CommitContext, debugEnabled: Boolean): (Boolean, String) = {

    val cond     = commitContext.count >= maxCount
    val flushing = if (cond) "*" else ""

    val log = s"count$flushing: '${commitContext.count}/$maxCount'"

    (cond, log)
  }
}
