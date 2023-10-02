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
package io.lenses.streamreactor.connect.cloud.common.sink.commit

import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger
import io.lenses.streamreactor.connect.cloud.common.sink.config.S3FlushSettings.defaultFlushCount
import io.lenses.streamreactor.connect.cloud.common.sink.config.S3FlushSettings.defaultFlushInterval
import io.lenses.streamreactor.connect.cloud.common.sink.config.S3FlushSettings.defaultFlushSize

import scala.util.Try

/**
  * The [[CommitPolicy]] is responsible for determining when
  * a sink partition (a single file) should be flushed (closed on disk, and moved to be visible).
  *
  * By default, it flushes based on configurable conditions such as number of records, file size,
  * or time since the file was last flushed.
  *
  * @param conditions the conditions to evaluate for flushing the partition
  */
case class CommitPolicy(logger: Logger, conditions: CommitPolicyCondition*) {

  /**
    * Checks if the output file should be flushed based on the provided `CommitContext`.
    *
    * @param context the commit context
    * @return true if the partition should be flushed, false otherwise
    */
  def shouldFlush(context: CommitContext): Boolean = {

    val debugEnabled: Boolean = Try(logger.underlying.isDebugEnabled).getOrElse(false)
    val res = conditions.map(_.eval(context, debugEnabled))
    val flush = res.exists {
      case ConditionCommitResult(true, _) => true
      case _                              => false
    }
    val flushingOrNot = if (flush) "" else "Not "

    if (debugEnabled)
      logger.debug(
        "{}Flushing '{}' for {topic:'{}', partition:{}, offset:{}, {}}",
        flushingOrNot,
        context.partitionFile,
        context.tpo.topic.value,
        context.tpo.partition,
        context.tpo.offset.value,
        res.flatMap(_.logLine).mkString(", "),
      )
    flush
  }
}

object CommitPolicy extends LazyLogging {
  val Default: CommitPolicy =
    CommitPolicy(FileSize(defaultFlushSize), Interval(defaultFlushInterval), Count(defaultFlushCount))
  def apply(conditions: CommitPolicyCondition*): CommitPolicy =
    CommitPolicy(logger, conditions: _*)
}
