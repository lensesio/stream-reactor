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
package io.lenses.streamreactor.connect.aws.s3.sink.naming

import io.lenses.streamreactor.connect.aws.s3.model.TopicPartitionOffset

trait S3FileNamer {
  def fileName(
    fnPad:                String => String,
    extension:            String,
    topicPartitionOffset: TopicPartitionOffset,
  ): String
}
object HierarchicalS3FileNamer extends S3FileNamer {
  def fileName(
    fnPad:                String => String,
    extension:            String,
    topicPartitionOffset: TopicPartitionOffset,
  ): String =
    s"${fnPad(topicPartitionOffset.offset.value.toString)}.$extension"
}
object PartitionedS3FileNamer extends S3FileNamer {
  def fileName(
    fnPad:                String => String,
    extension:            String,
    topicPartitionOffset: TopicPartitionOffset,
  ): String =
    s"${topicPartitionOffset.topic.value}(${fnPad(
      topicPartitionOffset.partition.toString,
    )}_${fnPad(topicPartitionOffset.offset.value.toString)}).$extension"

}
