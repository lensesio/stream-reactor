
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

import io.lenses.streamreactor.connect.aws.s3.config.{Format, FormatSelection}
import io.lenses.streamreactor.connect.aws.s3.model.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.SinkDataExtractor

import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}

trait S3FileNamingStrategy {

  protected val DefaultPrefix = "streamreactor"

  def getFormat: Format

  def prefix(bucketAndPrefix: BucketAndPrefix): String = bucketAndPrefix.prefix.getOrElse(DefaultPrefix)

  def stagingFilename(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition, partitionValues: Map[PartitionField, String]): BucketAndPath

  def finalFilename(bucketAndPrefix: BucketAndPrefix, topicPartitionOffset: TopicPartitionOffset, partitionValues: Map[PartitionField, String]): BucketAndPath

  def shouldProcessPartitionValues: Boolean

  def processPartitionValues(messageDetail: MessageDetail, topicPartition: TopicPartition): Map[PartitionField, String]

  def topicPartitionPrefix(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition): BucketAndPath

  val committedFilenameRegex: Regex
}

class HierarchicalS3FileNamingStrategy(formatSelection: FormatSelection) extends S3FileNamingStrategy {

  val format: Format = formatSelection.format

  override def stagingFilename(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition, partitionValues: Map[PartitionField, String]): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${prefix(bucketAndPrefix)}/.temp/${topicPartition.topic.value}/${topicPartition.partition}.${format.entryName.toLowerCase}")

  override def finalFilename(bucketAndPrefix: BucketAndPrefix, topicPartitionOffset: TopicPartitionOffset, partitionValues: Map[PartitionField, String]): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${prefix(bucketAndPrefix)}/${topicPartitionOffset.topic.value}/${topicPartitionOffset.partition}/${topicPartitionOffset.offset.value}.${format.entryName.toLowerCase}")

  override def getFormat: Format = format

  override def shouldProcessPartitionValues: Boolean = false

  override def processPartitionValues(messageDetail: MessageDetail, topicPartition: TopicPartition): Map[PartitionField, String] = throw new UnsupportedOperationException("This should never be called for this object")

  override val committedFilenameRegex: Regex = s".+/(.+)/(\\d+)/(\\d+).(.+)".r

  override def topicPartitionPrefix(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition): BucketAndPath = BucketAndPath(bucketAndPrefix.bucket, s"${prefix(bucketAndPrefix)}/${topicPartition.topic.value}/${topicPartition.partition}/")

}

class PartitionedS3FileNamingStrategy(formatSelection: FormatSelection, partitionSelection: PartitionSelection) extends S3FileNamingStrategy {

  val format: Format = formatSelection.format

  override def getFormat: Format = format

  override def stagingFilename(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition, partitionValues: Map[PartitionField, String]): BucketAndPath = {
    BucketAndPath(bucketAndPrefix.bucket, s"${prefix(bucketAndPrefix)}/${buildPartitionPrefix(partitionValues)}/${topicPartition.topic.value}/${topicPartition.partition}/temp.${format.entryName.toLowerCase}")
  }

  private def buildPartitionPrefix(partitionValues: Map[PartitionField, String]): String = {
    partitionSelection.partitions.map(
      (partition: PartitionField) => {
        partitionValuePrefix(partition) + partitionValues.getOrElse(partition, "[missing]")
      }
    ).mkString("/")
  }

  private def partitionValuePrefix(partition: PartitionField): String = {
    if (partitionSelection.partitionDisplay == KeysAndValues) s"${partition.valuePrefixDisplay()}=" else ""
  }

  override def finalFilename(bucketAndPrefix: BucketAndPrefix, topicPartitionOffset: TopicPartitionOffset, partitionValues: Map[PartitionField, String]): BucketAndPath =
    BucketAndPath(bucketAndPrefix.bucket, s"${prefix(bucketAndPrefix)}/${buildPartitionPrefix(partitionValues)}/${topicPartitionOffset.topic.value}(${topicPartitionOffset.partition}_${topicPartitionOffset.offset.value}).${format.entryName.toLowerCase}")

  override def processPartitionValues(messageDetail: MessageDetail, topicPartition: TopicPartition): Map[PartitionField, String] = {
    partitionSelection
      .partitions
      .map {
        case partition@HeaderPartitionField(name) => partition -> {
          val sinkData = messageDetail.headers.getOrElse(name.head, throw new IllegalArgumentException(s"Header '$name' not found in message"))
          getPartitionValueFromSinkData(sinkData, name.tail)
        }
        case partition@KeyPartitionField(name) => partition -> {
          val sinkData = messageDetail.keySinkData.getOrElse(throw new IllegalArgumentException(s"No key data found"))
          getPartitionValueFromSinkData(sinkData, name)
        }
        case partition@ValuePartitionField(name) => partition -> getPartitionValueFromSinkData(messageDetail.valueSinkData, name)
        case partition@WholeKeyPartitionField() => partition -> getPartitionByWholeKeyValue(messageDetail.keySinkData)
        case partition@TopicPartitionField() => partition -> topicPartition.topic.value
        case partition@PartitionPartitionField() => partition -> topicPartition.partition.toString
      }
      .toMap
  }

  private def getPartitionByWholeKeyValue(structOpt: Option[SinkData]): String = {
    val struct = structOpt
      .getOrElse(throw new IllegalArgumentException(s"No key struct found, but requested to partition by whole key"))

    Try {
      getFieldStringValue(struct, None).getOrElse("[missing]")
    } match {
      case Failure(exception) => throw new IllegalStateException("Non primitive struct provided, PARTITIONBY _key requested in KCQL", exception)
      case Success(value) => value
    }

  }

  val reservedCharacters = Set("/", "\\")

  private def getFieldStringValue(struct: SinkData, partitionName: Option[PartitionNamePath]) = {

    SinkDataExtractor.extractPathFromSinkData(struct)(partitionName)
      .fold(Option.empty[String])(fieldVal => Option(fieldVal
        .replace("/", "-")
        .replace("\\", "-"))

      )
  }

  def getPartitionValueFromSinkData(sinkData: SinkData, partitionName: PartitionNamePath): String = {
    getFieldStringValue(sinkData, Option(partitionName)).getOrElse("[missing]")
  }

  override def shouldProcessPartitionValues: Boolean = true

  override val committedFilenameRegex: Regex = s"^[^/]+?/(?:.+/)*(.+)\\((\\d+)_(\\d+)\\).(.+)".r

  override def topicPartitionPrefix(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition): BucketAndPath = BucketAndPath(bucketAndPrefix.bucket, s"${prefix(bucketAndPrefix)}/")
}


object CommittedFileName {

  def unapply(filename: String)(implicit s3FileNamingStrategy: S3FileNamingStrategy): Option[(Topic, Int, Offset, Format)] = {
    filename match {
      case s3FileNamingStrategy.committedFilenameRegex(topic, partition, end, extension) =>
        Format.withNameInsensitiveOption(extension)
          .fold(Option.empty[(Topic, Int, Offset, Format)]) {
            format => Some(Topic(topic), partition.toInt, Offset(end.toLong), format)
          }

      case _ => None
    }
  }
}
