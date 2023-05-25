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
package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.config.Format
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.aws.s3.formats.writer.SinkData
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.FileUtils.createFileAndParents
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.sink.config.DatePartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.HeaderPartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.KeyPartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionPartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionSelection
import io.lenses.streamreactor.connect.aws.s3.sink.config.TopicPartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.WholeKeyPartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.ExtractorErrorAdaptor.adaptErrorResponse
import io.lenses.streamreactor.connect.aws.s3.sink.extractors.SinkDataExtractor

import java.io.File
import java.util.UUID
import scala.util.matching.Regex
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait S3FileNamingStrategy {

  private val DefaultPrefix = "streamreactor"

  def getFormat: Format

  def prefix(bucketAndPrefix: S3Location): String = bucketAndPrefix.prefix.getOrElse(DefaultPrefix)

  def stagingFile(
    stagingDirectory: File,
    bucketAndPrefix:  S3Location,
    topicPartition:   TopicPartition,
    partitionValues:  Map[PartitionField, String],
  ): Either[FatalS3SinkError, File]

  def finalFilename(
    bucketAndPrefix:      S3Location,
    topicPartitionOffset: TopicPartitionOffset,
    partitionValues:      Map[PartitionField, String],
  ): Either[FatalS3SinkError, S3Location]

  def shouldProcessPartitionValues: Boolean

  def processPartitionValues(
    messageDetail:  MessageDetail,
    topicPartition: TopicPartition,
  ): Either[SinkError, Map[PartitionField, String]]

  def topicPartitionPrefix(bucketAndPrefix: S3Location, topicPartition: TopicPartition): S3Location

  val committedFilenameRegex: Regex

}

class HierarchicalS3FileNamingStrategy(formatSelection: FormatSelection, paddingStrategy: PaddingStrategy)
    extends S3FileNamingStrategy {

  import paddingStrategy._

  val format: Format = formatSelection.format

  override def stagingFile(
    stagingDirectory: File,
    bucketAndPrefix:  S3Location,
    topicPartition:   TopicPartition,
    partitionValues:  Map[PartitionField, String],
  ): Either[FatalS3SinkError, File] =
    Try {
      val uuid = UUID.randomUUID().toString
      val file = stagingDirectory
        .toPath
        .resolve(prefix(bucketAndPrefix))
        .resolve(padString(topicPartition.topic.value))
        .resolve(s"${padString(topicPartition.partition.toString)}.${format.entryName.toLowerCase}")
        .resolve(uuid)
        .toFile
      createFileAndParents(file)
      file
    }.toEither.left.map(ex => FatalS3SinkError(ex.getMessage, ex, topicPartition))

  override def finalFilename(
    bucketAndPrefix:      S3Location,
    topicPartitionOffset: TopicPartitionOffset,
    partitionValues:      Map[PartitionField, String],
  ): Either[FatalS3SinkError, S3Location] =
    Try(
      bucketAndPrefix.withPath(
        s"${prefix(bucketAndPrefix)}/${topicPartitionOffset.topic.value}/${padString(
          topicPartitionOffset.partition.toString,
        )}/${padString(topicPartitionOffset.offset.value.toString)}.${format.entryName.toLowerCase}",
      ),
    ).toEither.left.map(ex => FatalS3SinkError(ex.getMessage, topicPartitionOffset.toTopicPartition))

  override def getFormat: Format = format

  override def shouldProcessPartitionValues: Boolean = false

  override def processPartitionValues(
    messageDetail:  MessageDetail,
    topicPartition: TopicPartition,
  ): Either[SinkError, Map[PartitionField, String]] =
    FatalS3SinkError("This should never be called for this object", topicPartition).asLeft[Map[PartitionField, String]]

  override val committedFilenameRegex: Regex = s".+/(.+)/(\\d+)/(\\d+).(.+)".r

  override def topicPartitionPrefix(
    bucketAndPrefix: S3Location,
    topicPartition:  TopicPartition,
  ): S3Location =
    bucketAndPrefix.withPath(
      s"${prefix(bucketAndPrefix)}/${topicPartition.topic.value}/${padString(topicPartition.partition.toString)}/",
    )

}

class PartitionedS3FileNamingStrategy(
  formatSelection:    FormatSelection,
  paddingStrategy:    PaddingStrategy,
  partitionSelection: PartitionSelection,
) extends S3FileNamingStrategy {

  import paddingStrategy._

  val format: Format = formatSelection.format

  override def getFormat: Format = format

  override def stagingFile(
    stagingDirectory: File,
    bucketAndPrefix:  S3Location,
    topicPartition:   TopicPartition,
    partitionValues:  Map[PartitionField, String],
  ): Either[FatalS3SinkError, File] =
    Try {
      val uuid = UUID.randomUUID().toString
      val file = stagingDirectory
        .toPath
        .resolve(prefix(bucketAndPrefix))
        .resolve(buildPartitionPrefix(partitionValues))
        .resolve(topicPartition.topic.value)
        .resolve(padString(topicPartition.partition.toString))
        .resolve(format.entryName.toLowerCase)
        .resolve(uuid)
        .toFile
      createFileAndParents(file)
      file
    }.toEither.left.map(ex => FatalS3SinkError(ex.getMessage, ex, topicPartition))

  private def buildPartitionPrefix(partitionValues: Map[PartitionField, String]): String =
    partitionSelection.partitions.map {
      (partition: PartitionField) =>
        partitionValuePrefix(partition) + partitionValues.getOrElse(partition, "[missing]")
    }
      .mkString("/")

  private def partitionValuePrefix(partition: PartitionField): String =
    if (partitionSelection.partitionDisplay == KeysAndValues) s"${partition.valuePrefixDisplay()}=" else ""

  override def finalFilename(
    bucketAndPrefix:      S3Location,
    topicPartitionOffset: TopicPartitionOffset,
    partitionValues:      Map[PartitionField, String],
  ): Either[FatalS3SinkError, S3Location] =
    Try(
      bucketAndPrefix.withPath(
        s"${prefix(bucketAndPrefix)}/${buildPartitionPrefix(partitionValues)}/${topicPartitionOffset.topic.value}(${padString(
          topicPartitionOffset.partition.toString,
        )}_${padString(topicPartitionOffset.offset.value.toString)}).${format.entryName.toLowerCase}",
      ),
    ).toEither.left.map(ex => FatalS3SinkError(ex.getMessage, topicPartitionOffset.toTopicPartition))

  override def processPartitionValues(
    messageDetail:  MessageDetail,
    topicPartition: TopicPartition,
  ): Either[SinkError, Map[PartitionField, String]] =
    Try {
      partitionSelection
        .partitions
        .map {
          case partition @ HeaderPartitionField(name) => partition -> {
              val sinkData = messageDetail.headers.getOrElse(
                name.head,
                throw new IllegalArgumentException(s"Header '$name' not found in message"),
              )
              getPartitionValueFromSinkData(sinkData, name.tail)
            }
          case partition @ KeyPartitionField(name) => partition -> {
              val sinkData =
                messageDetail.keySinkData.getOrElse(throw new IllegalArgumentException(s"No key data found"))
              getPartitionValueFromSinkData(sinkData, name)
            }
          case partition @ ValuePartitionField(name) =>
            partition -> getPartitionValueFromSinkData(messageDetail.valueSinkData, name)
          case partition @ WholeKeyPartitionField() =>
            partition -> getPartitionByWholeKeyValue(messageDetail.keySinkData)
          case partition @ TopicPartitionField()     => partition -> topicPartition.topic.value
          case partition @ PartitionPartitionField() => partition -> padString(topicPartition.partition.toString)
          case partition @ DatePartitionField(_) => partition ->
              messageDetail.time.fold(
                throw new IllegalArgumentException(
                  "No valid timestamp parsed from kafka connect message, however date partitioning was requested",
                ),
              )(partition.formatter.format)
        }
        .toMap[PartitionField, String]
    }.toEither.left.map(ex => FatalS3SinkError(ex.getMessage, ex, topicPartition))

  private def getPartitionByWholeKeyValue(structOpt: Option[SinkData]): String = {
    val struct = structOpt
      .getOrElse(throw new IllegalArgumentException(s"No key struct found, but requested to partition by whole key"))

    Try {
      getFieldStringValue(struct, None).getOrElse("[missing]")
    } match {
      case Failure(exception) =>
        throw new IllegalStateException("Non primitive struct provided, PARTITIONBY _key requested in KCQL", exception)
      case Success(value) => value
    }

  }

  val reservedCharacters = Set("/", "\\")

  private def getFieldStringValue(struct: SinkData, partitionName: Option[PartitionNamePath]) =
    adaptErrorResponse(SinkDataExtractor.extractPathFromSinkData(struct)(partitionName)).fold(Option.empty[String])(
      fieldVal =>
        Option(fieldVal
          .replace("/", "-")
          .replace("\\", "-")),
    )

  def getPartitionValueFromSinkData(sinkData: SinkData, partitionName: PartitionNamePath): String =
    getFieldStringValue(sinkData, Option(partitionName)).getOrElse("[missing]")

  override def shouldProcessPartitionValues: Boolean = true

  override val committedFilenameRegex: Regex = s"^[^/]+?/(?:.+/)*(.+)\\((\\d+)_(\\d+)\\).(.+)".r

  override def topicPartitionPrefix(
    bucketAndPrefix: S3Location,
    topicPartition:  TopicPartition,
  ): S3Location = bucketAndPrefix.withPath(s"${prefix(bucketAndPrefix)}/")
}

object CommittedFileName {

  def unapply(
    filename: String,
  )(
    implicit
    s3FileNamingStrategy: S3FileNamingStrategy,
  ): Option[(Topic, Int, Offset, Format)] =
    filename match {
      case s3FileNamingStrategy.committedFilenameRegex(topic, partition, end, extension) =>
        Format.withNameInsensitiveOption(extension)
          .fold(Option.empty[(Topic, Int, Offset, Format)]) {
            format => Some((Topic(topic), partition.toInt, Offset(end.toLong), format))
          }

      case _ => None
    }
}
