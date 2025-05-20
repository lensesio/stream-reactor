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

import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionSelection

import java.io.File

trait KeyNamer {

  def partitionSelection: PartitionSelection

  def staging(
    stagingDirectory: File,
    bucketAndPrefix:  CloudLocation,
    topicPartition:   TopicPartition,
    partitionValues:  Map[PartitionField, String],
  ): Either[FatalCloudSinkError, File]

  def value(
    bucketAndPrefix:         CloudLocation,
    topicPartitionOffset:    TopicPartitionOffset,
    partitionValues:         Map[PartitionField, String],
    earliestRecordTimestamp: Long,
    latestRecordTimestamp:   Long,
  ): Either[FatalCloudSinkError, CloudLocation]

  def processPartitionValues(
    messageDetail:  MessageDetail,
    topicPartition: TopicPartition,
  ): Either[SinkError, Map[PartitionField, String]]
}
