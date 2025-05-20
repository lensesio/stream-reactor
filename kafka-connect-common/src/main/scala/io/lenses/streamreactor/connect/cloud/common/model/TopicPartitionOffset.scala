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
package io.lenses.streamreactor.connect.cloud.common.model

import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }

case class Topic(value: String) extends AnyVal {
  def withPartition(partition: Int): TopicPartition =
    TopicPartition(this, partition)
}

object Topic {

  val All: Topic = Topic("*")

  implicit def orderingByTopicValue: Ordering[Topic] =
    Ordering.by[Topic, String](_.value)
}

object Offset {

  implicit def orderingByOffsetValue[A <: Offset]: Ordering[A] =
    Ordering.by(_.value)

}

case class Offset(value: Long) extends AnyVal

object TopicPartition {
  def apply(kafkaTopicPartition: KafkaTopicPartition): TopicPartition =
    TopicPartition(Topic(kafkaTopicPartition.topic()), kafkaTopicPartition.partition())
}

case class TopicPartition(topic: Topic, partition: Int) {

  def atOffset(offset: Long): TopicPartitionOffset = withOffset(Offset(offset))

  def withOffset(offset: Offset): TopicPartitionOffset = TopicPartitionOffset(topic, partition, offset)

  def toKafka = new KafkaTopicPartition(topic.value, partition)
}

case class TopicPartitionOffset(topic: Topic, partition: Int, offset: Offset) {
  def toTopicPartition: TopicPartition = TopicPartition(topic, partition)

  def toTopicPartitionOffsetTuple: (TopicPartition, Offset) = (toTopicPartition, offset)
}
