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

import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}

case class OffsetReaderResult(path: String, line: String)

case class PollResults(
                        resultList: Vector[_ <: SourceData],
                        bucketAndPath: BucketAndPath,
                        prefix: String,
                        targetTopic: String
                      )


case class Topic(value: String) {
  require(value != null && value.trim.nonEmpty)

  def withPartition(partition: Int): TopicPartition = {
    TopicPartition(this, partition)
  }
}

object Offset {

  implicit def orderingByOffsetValue[A <: Offset]: Ordering[A] =
    Ordering.by(_.value)

}

case class Offset(value: Long) {

  require(value >= 0)
}

object TopicPartition {
  def apply(kafkaTopicPartition: KafkaTopicPartition): TopicPartition = {
    TopicPartition(Topic(kafkaTopicPartition.topic()), kafkaTopicPartition.partition())
  }
}

case class TopicPartition(topic: Topic, partition: Int) {
  def withOffset(offset: Offset): TopicPartitionOffset = TopicPartitionOffset(topic, partition, offset)

  def withOffset(offset: Long): TopicPartitionOffset = withOffset(Offset(offset))

  def toKafka = new KafkaTopicPartition(topic.value, partition)
}

case class TopicPartitionOffset(topic: Topic, partition: Int, offset: Offset) {
  def toTopicPartition: TopicPartition = TopicPartition(topic, partition)
}
