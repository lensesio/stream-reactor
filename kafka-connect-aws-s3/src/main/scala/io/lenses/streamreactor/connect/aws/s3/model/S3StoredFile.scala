/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.model

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.sink.{CommittedFileName, S3FileNamingStrategy}

object S3StoredFile extends LazyLogging {

  def from(path: String, fileNamingStrategy: S3FileNamingStrategy): Option[S3StoredFile] = {
    CommittedFileName.from(path, fileNamingStrategy)
      .filter(_.format == fileNamingStrategy.getFormat)
      .map { committableFile =>
        S3StoredFile(
          path,
          TopicPartitionOffset(committableFile.topic, committableFile.partition, committableFile.offset),
        )
      }
  }
}

case class S3StoredFile(
                         path: String,
                         topicPartitionOffset: TopicPartitionOffset
                       )

object S3StoredFileSorter {
  implicit val ordering: Ordering[S3StoredFile] = (x: S3StoredFile, y: S3StoredFile) => {
    val topic = x.topicPartitionOffset.topic.value.compareTo(y.topicPartitionOffset.topic.value)
    if (topic == 0) {
      val partition = x.topicPartitionOffset.partition.compareTo(y.topicPartitionOffset.partition)
      if (partition == 0) {
        x.topicPartitionOffset.offset.value.compareTo(y.topicPartitionOffset.offset.value)
      } else partition
    } else topic
  }
}
