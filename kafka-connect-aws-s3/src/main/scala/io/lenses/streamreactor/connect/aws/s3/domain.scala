
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

package io.lenses.streamreactor.connect.aws.s3

import org.apache.kafka.common.{TopicPartition => KafkaTopicPartition}

case object BucketAndPrefix {
  def apply(bucketAndPath: String): BucketAndPrefix = {
    bucketAndPath.split(":") match {
      case Array(bucket) => BucketAndPrefix(bucket, None)
      case Array(bucket, path) => BucketAndPrefix(bucket, Some(path))
      case _ => throw new IllegalArgumentException("Invalid number of arguments provided to create BucketAndPrefix")
    }
  }
}

case class BucketAndPrefix(
                            bucket: String,
                            prefix: Option[String]
                          ) {
    val filtered = prefix
      .filter(_.contains("/"))

    filtered
      .foreach(_ => throw new IllegalArgumentException("Nested prefix not currently supported"))
}

case class BucketAndPath(
                          bucket: String,
                          path: String
                        )

case class Topic(value: String) {
  require(value != null && value.trim.nonEmpty)
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

  def toKafka = new KafkaTopicPartition(topic.value, partition)
}

case class TopicPartitionOffset(topic: Topic, partition: Int, offset: Offset) {
  def toTopicPartition: TopicPartition = TopicPartition(topic, partition)
}
