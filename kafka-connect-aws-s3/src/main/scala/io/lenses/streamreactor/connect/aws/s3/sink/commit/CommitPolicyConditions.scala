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

import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.sink.commit.Interval.formatter

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.ScalaDurationOps

case class ConditionCommitResult(commitTriggered: Boolean)

trait CommitPolicyCondition extends LazyLogging {
  def eval(context: CommitContext): ConditionCommitResult
}

case class FileSize(maxFileSize: Long) extends CommitPolicyCondition with LazyLogging {

  override def eval(context: CommitContext): ConditionCommitResult = {
    val cond = context.fileSize >= maxFileSize
    logger.debug(s"[${context.connectorTaskId.show}] File Size Policy:${context.fileSize}/$maxFileSize.")
    ConditionCommitResult(cond)
  }
}

object Interval {

  def apply(scalaInterval: FiniteDuration): Interval = {
    val interval = scalaInterval.toJava
    val clock    = Clock.systemDefaultZone()
    Interval(interval, clock)
  }

  private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
}

case class Interval(interval: Duration, clock: Clock) extends CommitPolicyCondition {

  override def eval(context: CommitContext): ConditionCommitResult = {
    val nowInstant = clock.instant()

    val lastWriteInstant = Instant.ofEpochMilli(context.lastModified)
    val nextFlushTime    = lastWriteInstant.plus(interval)
    val nextFlushDue     = nextFlushTime.toEpochMilli <= nowInstant.toEpochMilli

    logger.debug(
      s"[${context.connectorTaskId.show}] Interval Policy: next flush time ${formatter.format(
        LocalDateTime.ofInstant(nextFlushTime, ZoneOffset.UTC),
      )} ; last flush time=${formatter.format(LocalDateTime.ofInstant(lastWriteInstant, ZoneOffset.UTC))}",
    )
    ConditionCommitResult(nextFlushDue)
  }
}

case class Count(maxCount: Long) extends CommitPolicyCondition {
  override def eval(context: CommitContext): ConditionCommitResult = {
    val cond = context.count >= maxCount
    logger.debug(s"[${context.connectorTaskId.show}] Count Policy: ${context.count}/$maxCount.")
    ConditionCommitResult(cond)
  }
}
