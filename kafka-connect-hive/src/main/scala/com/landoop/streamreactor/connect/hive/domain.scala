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
package com.landoop.streamreactor.connect.hive

import cats.Show
import cats.data.NonEmptyList
import org.apache.hadoop.fs.Path
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }

case class Topic(value: String) {
  require(value != null && value.trim.nonEmpty)
}

case class Offset(value: Long) {
  require(value >= 0)
}

case class TopicPartition(topic: Topic, partition: Int) {
  def withOffset(offset: Offset): TopicPartitionOffset = TopicPartitionOffset(topic, partition, offset)
  def toKafka = new KafkaTopicPartition(topic.value, partition)
}

case class TopicPartitionOffset(topic: Topic, partition: Int, offset: Offset) {
  def toTopicPartition = TopicPartition(topic, partition)
}

case class DatabaseName(value: String) {
  require(value != null && value.trim.nonEmpty)
}

case class TableName(value: String) {
  require(value != null && value.trim.nonEmpty)
}

// contains all the partition keys for a particular table
case class PartitionPlan(tableName: TableName, keys: NonEmptyList[PartitionKey])

// contains a partition key, which you can think of as like a partition column name
case class PartitionKey(value: String)

// defines a partition key field
case class PartitionField(name: String, comment: Option[String] = None) {
  require(name != null && name.trim.nonEmpty)
}

// contains a single partition in a table, that is one set of unique values, one per partition key
case class Partition(entries: NonEmptyList[(PartitionKey, String)], location: Option[Path])

case class Serde(serializationLib: String, inputFormat: String, outputFormat: String, params: Map[String, String])

// generates the default hive metatstore location string for a partition
object DefaultPartitionLocation extends Show[Partition] {
  override def show(t: Partition): String =
    t.entries.map { case (key, value) => key.value + "=" + value }.toList.mkString("/")
}
