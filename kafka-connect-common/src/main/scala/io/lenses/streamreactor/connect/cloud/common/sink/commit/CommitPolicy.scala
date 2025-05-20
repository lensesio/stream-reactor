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
package io.lenses.streamreactor.connect.cloud.common.sink.commit

import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.Logger

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
    val res = conditions.map(_.eval(context, true))
    val flush = res.exists {
      case ConditionCommitResult(true, _) => true
      case _                              => false
    }

    if (flush) {
      logger.info(context.generateLogLine(flush, res))
    }
    flush
  }
}

object CommitPolicy extends LazyLogging {
  def apply(conditions: CommitPolicyCondition*): CommitPolicy =
    CommitPolicy(logger, conditions: _*)
}
