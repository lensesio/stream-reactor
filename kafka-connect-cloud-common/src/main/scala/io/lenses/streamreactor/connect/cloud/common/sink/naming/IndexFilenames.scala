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
package io.lenses.streamreactor.connect.cloud.common.sink.naming

import cats.implicits.toTraverseOps
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset

import scala.util.Try

object IndexFilenames {

  /**
    * Generate the filename for the index file.
    */
  def indexFilename(topic: String, partition: Int, offset: Long)(implicit connectorTaskId: ConnectorTaskId): String =
    f"${indexForTopicPartition(topic, partition)}$offset%020d"

  /**
    * Generate the directory of the index for a given topic and partition
    */
  def indexForTopicPartition(topic: String, partition: Int)(implicit connectorTaskId: ConnectorTaskId): String =
    f".indexes/${connectorTaskId.name}/$topic/$partition%05d/"

  /**
    * Parses the filename of the index file, converting it to a TopicPartitionOffset
    *
    * @param maybeIndex option of the index filename
    * @return either an error, or a TopicPartitionOffset
    */
  def indexToOffset(
    topicPartition: TopicPartition,
    maybeIndex:     Option[String],
  ): Either[Throwable, Option[TopicPartitionOffset]] =
    maybeIndex.map(index =>
      for {
        offset <- IndexFilenames.offsetFromIndex(index)
      } yield topicPartition.withOffset(offset),
    ).sequence

  /**
    * Parses an index filename and returns an offset
    */
  private def offsetFromIndex(indexFilename: String): Either[Throwable, Offset] = {
    val lastIndex = indexFilename.lastIndexOf("/")
    val (_, last) = indexFilename.splitAt(lastIndex + 1)

    Try(Offset(last.toLong)).toEither
  }

}
