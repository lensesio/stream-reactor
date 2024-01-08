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
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.ConditionCommitResult

object HttpCommitContext {
  def default(sinkName: String): HttpCommitContext =
    HttpCommitContext(
      sinkName,
      Map.empty,
      0L,
      0L,
      System.currentTimeMillis(),
      Option.empty,
      Map.empty,
    )
}

/**
  * @param tpo              the [[TopicPartitionOffset]] of the last record written
  * @param count            the number of records written thus far to the file
  * @param createdTimestamp the time in milliseconds when the the file was created/accessed first time
  */
case class HttpCommitContext(
  sinkName:             String,
  committedOffsets:     Map[TopicPartition, Offset],
  count:                Long,
  fileSize:             Long,
  createdTimestamp:     Long,
  lastFlushedTimestamp: Option[Long],
  errors:               Map[String, Seq[Throwable]],
) extends CommitContext
    with LazyLogging {
  override def logFlush(flushing: Boolean, result: Seq[ConditionCommitResult]): Unit = {
    val flushingOrNot = if (flushing) "" else "Not "
    val committedOffsetGroups = committedOffsets.map(tpo =>
      s"topic:${tpo._1.topic}, partition:${tpo._1.partition}, offset:${tpo._2.value}",
    ).mkString(";")
    val logLine = result.flatMap(_.logLine).mkString(", ")
    logger.debug(
      s"[$sinkName] ${flushingOrNot}Flushing for $committedOffsetGroups because {}",
      logLine,
    )
  }

  def resetErrors: HttpCommitContext = copy(errors = Map.empty)

  def addError(ex: Throwable): HttpCommitContext =
    copy(
      errors = {
        val mapKey   = ex.getClass.getSimpleName
        val mapValue = errors.get(mapKey).fold(Seq(ex))(_ :+ ex)
        errors + (mapKey -> mapValue)
      },
    )
}
