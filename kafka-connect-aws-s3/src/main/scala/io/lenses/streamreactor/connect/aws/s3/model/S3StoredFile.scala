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
  def apply(path: String)(implicit fileNamingStrategy: S3FileNamingStrategy): Option[S3StoredFile] = {
    path match {
      case originalValue@CommittedFileName(topic, partition, end, format) if format == fileNamingStrategy.getFormat =>
        Some(S3StoredFile(
          originalValue,
          TopicPartitionOffset(topic, partition, end),
        ))
      case _ => logger.debug(s"Invalid file type in S3 bucket - no match found for file $path")
        None
    }
  }
}

case class S3StoredFile(
                         path: String,
                         topicPartitionOffset: TopicPartitionOffset
                       )


object S3StoredFileSorter {

  def sort(inputFiles: List[S3StoredFile]): List[S3StoredFile] = {
    inputFiles.sortBy {
      storedFile: S3StoredFile =>
        (
          storedFile.topicPartitionOffset.topic.value,
          storedFile.topicPartitionOffset.partition,
          storedFile.topicPartitionOffset.offset.value
        )
    }
  }

}
