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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink

import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

import java.util

object OffsetJavaScalaConverter {

  def offsetMapToScala(
    currentOffsets: util.Map[KafkaTopicPartition, OffsetAndMetadata],
  ): Map[TopicPartition, OffsetAndMetadata] =
    currentOffsets.asScala.map {
      case (partition, metadata) =>
        Topic(partition.topic()).withPartition(partition.partition()) -> metadata
    }.toMap

  def offsetMapToJava(
    currentOffsets: Map[TopicPartition, OffsetAndMetadata],
  ): util.Map[KafkaTopicPartition, OffsetAndMetadata] =
    currentOffsets.map {
      case (partition, metadata) =>
        new KafkaTopicPartition(
          partition.topic.value,
          partition.partition,
        ) -> metadata
    }.toMap.asJava
}
