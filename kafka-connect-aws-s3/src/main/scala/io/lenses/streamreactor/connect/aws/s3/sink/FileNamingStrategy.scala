
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

package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3._
import io.lenses.streamreactor.connect.aws.s3.config.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.aws.s3.config.{Format, FormatSelection, PartitionSelection}
import org.apache.kafka.connect.data.Struct

import scala.util.matching.Regex

trait S3FileNamingStrategy {

  protected val DefaultPrefix = "streamreactor"

  def getFormat: Format

  def Prefix(bucketAndPrefix: BucketAndPrefix) = bucketAndPrefix.prefix.getOrElse(DefaultPrefix)

  def stagingFilename(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition, partitionValues: Map[String, String] = Map.empty): BucketAndPath

  def finalFilename(bucketAndPrefix: BucketAndPrefix, topicPartitionOffset: TopicPartitionOffset, partitionValues: Map[String, String] = Map.empty): BucketAndPath

  def shouldProcessPartitionValues: Boolean

  def processPartitionValues(struct: Struct): Map[String, String]

  val committedFilenameRegex: Regex
}

class HierarchicalS3FileNamingStrategy(formatSelection: FormatSelection) extends S3FileNamingStrategy {

  val format: Format = formatSelection.format

  override def stagingFilename(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition, partitionValues: Map[String, String] = Map.empty): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${Prefix(bucketAndPrefix)}/.temp/${topicPartition.topic.value}/${topicPartition.partition}.${format.entryName.toLowerCase}")

  override def finalFilename(bucketAndPrefix: BucketAndPrefix, topicPartitionOffset: TopicPartitionOffset, partitionValues: Map[String, String] = Map.empty): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${Prefix(bucketAndPrefix)}/${topicPartitionOffset.topic.value}/${topicPartitionOffset.partition}/${topicPartitionOffset.offset.value}.${format.entryName.toLowerCase}")

  override def getFormat: Format = format

  override def shouldProcessPartitionValues: Boolean = false

  override def processPartitionValues(struct: Struct): Map[String, String] = throw new UnsupportedOperationException("This should never be called for this object")

  override val committedFilenameRegex: Regex = s"(.+)/(.+)/(\\d+)/(\\d+).(.+)".r
}

class PartitionedS3FileNamingStrategy(formatSelection: FormatSelection, partitionSelection: PartitionSelection) extends S3FileNamingStrategy {

  val format: Format = formatSelection.format

  override def getFormat: Format = format

  override def stagingFilename(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition, partitionValues: Map[String, String]): BucketAndPath = {
    BucketAndPath(bucketAndPrefix.bucket, s"${Prefix(bucketAndPrefix)}/${buildPartitionPrefix(partitionValues)}/${topicPartition.topic.value}/${topicPartition.partition}/temp.${format.entryName.toLowerCase}")
  }

  private def buildPartitionPrefix(partitionValues: Map[String, String]): String = {
    partitionSelection.partitions.map(
      partition => {
        partitionValuePrefix(partition) + partitionValues.getOrElse(partition.name, "[missing]")
      }
    ).mkString("/")
  }

  private def partitionValuePrefix(partition: PartitionField): String = {
    if (partitionSelection.partitionDisplay == KeysAndValues) s"${partition.name}=" else ""
  }

  override def finalFilename(bucketAndPrefix: BucketAndPrefix, topicPartitionOffset: TopicPartitionOffset, partitionValues: Map[String, String]): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${Prefix(bucketAndPrefix)}/${buildPartitionPrefix(partitionValues)}/${topicPartitionOffset.topic.value}/${topicPartitionOffset.partition}/${topicPartitionOffset.offset.value}.${format.entryName.toLowerCase}")

  override def processPartitionValues(struct: Struct): Map[String, String] = {
    partitionSelection
      .partitions
      .map(partition => partition.name -> Option(struct.get(partition.name)).getOrElse("[missing]").toString)
      .toMap
  }

  override def shouldProcessPartitionValues: Boolean = true

  override val committedFilenameRegex: Regex = s"^([^/]+?)/(?:.+/)*(.+)/(\\d+)/(\\d+).(.+)".r
}


object CommittedFileName {

  def unapply(filename: String)(implicit s3FileNamingStrategy: S3FileNamingStrategy): Option[(String, Topic, Int, Offset, Format)] = {
    filename match {
      case s3FileNamingStrategy.committedFilenameRegex(prefix, topic, partition, end, extension) =>
        Format.withNameInsensitiveOption(extension)
          .fold(Option.empty[(String, Topic, Int, Offset, Format)]) {
            format => Some(prefix, Topic(topic), partition.toInt, Offset(end.toLong), format)
          }

      case _ => None
    }
  }
}
