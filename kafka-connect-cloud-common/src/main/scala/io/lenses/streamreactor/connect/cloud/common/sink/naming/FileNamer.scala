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
package io.lenses.streamreactor.connect.cloud.common.sink.naming

import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingStrategy

trait FileNamer {
  def fileName(
    fileNamerParams: FileNamerParams,
  ): String
}
trait FileNamerFactory {
  def createFileNamer(fileNameConfig: FileNamerConfig): FileNamer
}
final case class FileNamerConfig(
  partitionPaddingStrategy: PaddingStrategy,
  offsetPaddingStrategy:    PaddingStrategy,
  extension:                String,
  suffix:                   Option[String],
)
final case class FileNamerParams(
  topicPartitionOffset:    TopicPartitionOffset,
  earliestRecordTimestamp: Long,
  latestRecordTimestamp:   Long,
)

/**
 * @param offsetPaddingStrategy
 * @param extension
 * @param suffix Allows to add a suffix to the "file" name. It covers the scenario where two connectors from two different clusters write to the same bucket when reading from the same topic.
 */
class OffsetFileNamer(
  fileNameConfig: FileNamerConfig,
) extends FileNamer {
  def fileName(
    fileNamerParams: FileNamerParams,
  ): String =
    s"${fileNameConfig.offsetPaddingStrategy.padString(
      fileNamerParams.topicPartitionOffset.offset.value.toString,
    )}_${fileNamerParams.earliestRecordTimestamp}_${fileNamerParams.latestRecordTimestamp}${fileNameConfig.suffix.getOrElse("")}.${fileNameConfig.extension}"
}

class TopicPartitionOffsetFileNamer(
  fileNameConfig: FileNamerConfig,
) extends FileNamer {
  def fileName(
    fileNamerParams: FileNamerParams,
  ): String =
    s"${fileNamerParams.topicPartitionOffset.topic.value}(${fileNameConfig.partitionPaddingStrategy.padString(
      fileNamerParams.topicPartitionOffset.partition.toString,
    )}_${fileNameConfig.offsetPaddingStrategy.padString(
      fileNamerParams.topicPartitionOffset.offset.value.toString,
    )})${fileNameConfig.suffix.getOrElse("")}.${fileNameConfig.extension}"

}
