/*
 * Copyright 2017-2026 Lenses.io Ltd
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
    topicPartitionOffset:    TopicPartitionOffset,
    earliestRecordTimestamp: Long,
    latestRecordTimestamp:   Long,
  ): String
}

/**
  * @param offsetPaddingStrategy
  * @param extension
  * @param suffix Allows to add a suffix to the "file" name. It covers the scenario where two connectors from two different clusters write to the same bucket when reading from the same topic.
  */
class OffsetFileNamer(
  offsetPaddingStrategy: PaddingStrategy,
  extension:             String,
  suffix:                Option[String],
) extends FileNamer {
  def fileName(
    topicPartitionOffset:    TopicPartitionOffset,
    earliestRecordTimestamp: Long,
    latestRecordTimestamp:   Long,
  ): String =
    s"${offsetPaddingStrategy.padString(
      topicPartitionOffset.offset.value.toString,
    )}_${earliestRecordTimestamp}_$latestRecordTimestamp${suffix.getOrElse("")}.$extension"
}

class TopicPartitionOffsetFileNamer(
  partitionPaddingStrategy: PaddingStrategy,
  offsetPaddingStrategy:    PaddingStrategy,
  extension:                String,
  suffix:                   Option[String],
) extends FileNamer {
  def fileName(
    topicPartitionOffset:    TopicPartitionOffset,
    earliestRecordTimestamp: Long,
    latestRecordTimestamp:   Long,
  ): String =
    s"${topicPartitionOffset.topic.value}(${partitionPaddingStrategy.padString(
      topicPartitionOffset.partition.toString,
    )}_${offsetPaddingStrategy.padString(topicPartitionOffset.offset.value.toString)})${suffix.getOrElse("")}.$extension"

}
