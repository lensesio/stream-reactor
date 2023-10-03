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
package io.lenses.streamreactor.connect.cloud.common.sink.naming

import cats.implicits.catsSyntaxEitherId
import cats.implicits.toTraverseOps
import io.lenses.streamreactor.connect.cloud.common.config.FormatSelection
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.SinkData
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.FileUtils.createFileAndParents
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingService
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionDisplay.KeysAndValues
import io.lenses.streamreactor.connect.cloud.common.sink.config.DatePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.HeaderPartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.KeyPartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionPartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionSelection
import io.lenses.streamreactor.connect.cloud.common.sink.config.TopicPartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.WholeKeyPartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.extractors.ExtractorErrorAdaptor.adaptErrorResponse
import io.lenses.streamreactor.connect.cloud.common.sink.extractors.SinkDataExtractor

import java.io.File
import java.util.UUID
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object CloudKeyNamer {

  def apply(
    formatSelection:    FormatSelection,
    partitionSelection: PartitionSelection,
    fileNamer:          FileNamer,
    paddingService:     PaddingService,
  ): CloudKeyNamer =
    new CloudKeyNamer(
      formatSelection,
      partitionSelection,
      fileNamer,
      paddingService,
    )
}
class CloudKeyNamer(
  formatSelection:    FormatSelection,
  partitionSelection: PartitionSelection,
  fileNamer:          FileNamer,
  paddingService:     PaddingService,
) extends KeyNamer {

  private val DefaultPrefix = ""

  private def addTrailingSlash(s: String): String = if (s.last == '/') s else s + '/'

  private def prefix(bucketAndPrefix: CloudLocation): String =
    bucketAndPrefix.prefix.map(addTrailingSlash).getOrElse(DefaultPrefix)

  override def stagingFile(
    stagingDirectory: File,
    bucketAndPrefix:  CloudLocation,
    topicPartition:   TopicPartition,
    partitionValues:  Map[PartitionField, String],
  ): Either[FatalCloudSinkError, File] =
    Try {
      val uuid = UUID.randomUUID().toString
      val file = stagingDirectory
        .toPath
        .resolve(prefix(bucketAndPrefix))
        .resolve(buildPartitionPrefix(partitionValues))
        .resolve(formatSelection.extension)
        .resolve(uuid)
        .toFile
      createFileAndParents(file)
      file
    }.toEither.left.map(ex => FatalCloudSinkError(ex.getMessage, ex, topicPartition))

  private def buildPartitionPrefix(partitionValues: Map[PartitionField, String]): String =
    partitionSelection.partitions.map {
      (partition: PartitionField) =>
        partitionValues.get(partition) match {
          case Some(partVal) if partition.supportsPadding =>
            partitionValuePrefix(partition) + paddingService.padderFor(partition.name()).padString(partVal)
          case Some(partVal) => partitionValuePrefix(partition) + partVal
          case None          => "[missing]"
        }
    }
      .mkString("/")

  private def partitionValuePrefix(partition: PartitionField): String =
    if (partitionSelection.partitionDisplay == KeysAndValues) s"${partition.name()}=" else ""

  override def finalFilename(
    bucketAndPrefix:      CloudLocation,
    topicPartitionOffset: TopicPartitionOffset,
    partitionValues:      Map[PartitionField, String],
  ): Either[FatalCloudSinkError, CloudLocation] =
    Try(
      bucketAndPrefix.withPath(
        s"${prefix(bucketAndPrefix)}${buildPartitionPrefix(partitionValues)}/${fileNamer.fileName(topicPartitionOffset)}",
      ),
    ).toEither.left.map(ex => FatalCloudSinkError(ex.getMessage, topicPartitionOffset.toTopicPartition))

  override def processPartitionValues(
    messageDetail:  MessageDetail,
    topicPartition: TopicPartition,
  ): Either[SinkError, Map[PartitionField, String]] =
    partitionSelection
      .partitions
      .traverse {
        case partition @ HeaderPartitionField(name) =>
          messageDetail.headers.get(name.head) match {
            case Some(value) =>
              partitionValueOrError(value, s"Header '${name.head}' is null.", topicPartition, partition)(
                getPartitionValueFromSinkData(_, name.tail),
              )
            case None =>
              FatalCloudSinkError(s"Header '$name' not found in message", topicPartition).asLeft[(
                PartitionField,
                String,
              )]
          }

        case partition @ KeyPartitionField(name) =>
          partitionValueOrError(messageDetail.key, s"Key is null.", topicPartition, partition)(
            getPartitionValueFromSinkData(_, name),
          )

        case partition @ ValuePartitionField(name) =>
          partitionValueOrError(messageDetail.value, s"Value is null.", topicPartition, partition)(
            getPartitionValueFromSinkData(_, name),
          )

        case partition @ WholeKeyPartitionField =>
          getPartitionByWholeKeyValue(messageDetail.key, topicPartition).map(partition -> _)
        case partition @ TopicPartitionField => (partition -> topicPartition.topic.value).asRight[SinkError]
        case partition @ PartitionPartitionField =>
          val partitionPaddingStrategy = paddingService.padderFor("partition")
          (partition -> partitionPaddingStrategy.padString(topicPartition.partition.toString)).asRight[SinkError]
        case partition @ DatePartitionField(_) =>
          messageDetail.timestamp match {
            case Some(value) => (partition -> partition.formatter.format(value)).asRight[SinkError]
            case None =>
              FatalCloudSinkError(s"Timestamp not found in message", topicPartition).asLeft[(
                PartitionField,
                String,
              )]
          }
      }
      .map(_.toMap)

  private def partitionValueOrError(
    data:           SinkData,
    errorMsg:       String,
    topicPartition: TopicPartition,
    partition:      PartitionField,
  )(f:              SinkData => String,
  ): Either[SinkError, (PartitionField, String)] =
    data match {
      case NullSinkData(_) => FatalCloudSinkError(errorMsg, topicPartition).asLeft[(PartitionField, String)]
      case other           => (partition -> f(other)).asRight
    }
  private def getPartitionByWholeKeyValue(data: SinkData, topicPartition: TopicPartition): Either[SinkError, String] =
    data match {
      case NullSinkData(_) =>
        FatalCloudSinkError(s"Key is null, but requested to partition by whole key", topicPartition).asLeft[String]
      case other =>
        Try {
          getFieldStringValue(other, None).getOrElse("[missing]")
        } match {
          case Failure(_) =>
            FatalCloudSinkError("Non primitive struct provided, PARTITIONBY _key requested in KCQL",
                                topicPartition,
            ).asLeft[String]
          case Success(value) => value.asRight
        }
    }

  private def getFieldStringValue(struct: SinkData, partitionName: Option[PartitionNamePath]) =
    adaptErrorResponse(SinkDataExtractor.extractPathFromSinkData(struct)(partitionName)).fold(Option.empty[String])(
      fieldVal =>
        Option(fieldVal
          .replace("/", "-")
          .replace("\\", "-")),
    )

  private def getPartitionValueFromSinkData(sinkData: SinkData, partitionName: PartitionNamePath): String =
    getFieldStringValue(sinkData, Option(partitionName)).getOrElse("[missing]")

}
