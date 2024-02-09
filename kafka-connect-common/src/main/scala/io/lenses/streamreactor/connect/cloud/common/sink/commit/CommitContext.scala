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
package io.lenses.streamreactor.connect.cloud.common.sink.commit

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset

trait CommitContext {
  def count:                Long
  def fileSize:             Long
  def createdTimestamp:     Long
  def lastFlushedTimestamp: Option[Long]

  def lastModified: Long = lastFlushedTimestamp.getOrElse(createdTimestamp)

  def generateLogLine(flushing: Boolean, result: Seq[ConditionCommitResult]): String
}

/**
  * @param tpo              the [[TopicPartitionOffset]] of the last record written
  * @param count            the number of records written thus far to the file
  * @param createdTimestamp the time in milliseconds when the the file was created/accessed first time
  */
case class CloudCommitContext(
  tpo:                  TopicPartitionOffset,
  count:                Long,
  fileSize:             Long,
  createdTimestamp:     Long,
  lastFlushedTimestamp: Option[Long],
  partitionFile:        String,
) extends CommitContext
    with LazyLogging {
  override def generateLogLine(flushing: Boolean, result: Seq[ConditionCommitResult]): String = {
    val flushingOrNot = if (flushing) "" else "Not "
    s"${flushingOrNot}Flushing '$partitionFile' for {topic:'${tpo.topic.value}', partition:${tpo.partition}, offset:${tpo.offset.value}, ${result.flatMap(_.logLine).mkString(", ")}}"
  }

}
