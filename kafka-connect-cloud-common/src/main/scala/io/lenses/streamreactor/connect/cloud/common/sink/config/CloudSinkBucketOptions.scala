/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.config

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.cloud.common.config.BytesFormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.config.FormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushInterval
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushSize
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.PartitionIncludeKeys
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.KeyNamerVersion
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CloudCommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.cloud.common.sink.config.kcqlprops.CloudSinkProps
import io.lenses.streamreactor.connect.cloud.common.sink.config.kcqlprops.SinkPropsSchema
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingService
import io.lenses.streamreactor.connect.cloud.common.sink.naming._
import org.apache.kafka.common.config.ConfigException

object CloudSinkBucketOptions extends LazyLogging {

  val WithFlushErrorMessage = s"""
                                 |The KCQL keywords WITH_FLUSH_COUNT, WITH_FLUSH_SIZE, and WITH_FLUSH_INTERVAL have been replaced by
                                 |entries in the PROPERTIES section.
                                 |
                                 |Replace WITH_FLUSH_COUNT with: PROPERTIES('${FlushCount.entryName}'=123).
                                 |Replace WITH_FLUSH_SIZE with: PROPERTIES('${FlushSize.entryName}'=456).
                                 |Replace WITH_FLUSH_Interval with: PROPERTIES('${FlushInterval}'=789).""".stripMargin

  val WithPartitionerError = s"""
                                |Invalid KCQL setting. WITHPARTITIONER feature has been replaced.
                                |Adapt the KCQL to use `PROPERTIES`.
                                |
                                | INSERT INTO ...
                                | ....
                                | PROPERTIES ('${PartitionIncludeKeys.entryName}'=true/false)
                                |
                                |'WITHPARTITIONER Values' should translate to: PROPERTIES ('${PartitionIncludeKeys.entryName}'=true).
                                |'WITHPARTITIONER KeysAndValues' should translate to: PROPERTIES ('${PartitionIncludeKeys.entryName}'=true).""".stripMargin
  def apply(
    connectorTaskId: ConnectorTaskId,
    config:          CloudSinkConfigDefBuilder,
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, Seq[CloudSinkBucketOptions]] =
    config.getKCQL.map { kcql: Kcql =>
      for {
        _                 <- validateWithFlush(kcql)
        _                 <- validateNoMoreWithPartitioner(kcql)
        formatSelection   <- FormatSelection.fromKcql(kcql, SinkPropsSchema.schema)
        fileExtension      = FileExtensionNamer.fileExtension(config.getCompressionCodec(), formatSelection)
        sinkProps          = CloudSinkProps.fromKcql(kcql)
        partitionSelection = PartitionSelection(kcql, sinkProps)
        paddingService    <- PaddingService.fromConfig(config, sinkProps)

        keyNameVersion = KeyNamerVersion(sinkProps, KeyNamerVersion.V0)
        fileNamer = if (partitionSelection.isCustom) {
          keyNameVersion match {
            case KeyNamerVersion.V0 =>
              new TopicPartitionOffsetFileNamerV0(
                paddingService.padderFor("partition"),
                paddingService.padderFor("offset"),
                fileExtension,
              )
            case KeyNamerVersion.V1 =>
              new TopicPartitionOffsetFileNamerV1(
                paddingService.padderFor("partition"),
                paddingService.padderFor("offset"),
                fileExtension,
              )
          }
        } else {
          keyNameVersion match {
            case KeyNamerVersion.V0 =>
              new OffsetFileNamerV0(
                paddingService.padderFor("offset"),
                fileExtension,
              )
            case KeyNamerVersion.V1 =>
              new OffsetFileNamerV1(
                paddingService.padderFor("offset"),
                fileExtension,
              )
          }
        }
        keyNamer         = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)
        stagingArea     <- config.getLocalStagingArea()(connectorTaskId)
        target          <- CloudLocation.splitAndValidate(kcql.getTarget)
        storageSettings <- DataStorageSettings.from(sinkProps)
        _               <- validateEnvelopeAndFormat(formatSelection, storageSettings)
        commitPolicy     = config.commitPolicy(kcql)
        _               <- validateCommitPolicyForBytesFormat(formatSelection, commitPolicy)
      } yield {
        CloudSinkBucketOptions(
          Option(kcql.getSource).filterNot(Set("*", "`*`").contains(_)),
          target,
          formatSelection  = formatSelection,
          keyNamer         = keyNamer,
          commitPolicy     = commitPolicy,
          localStagingArea = stagingArea,
          dataStorage      = storageSettings,
        )
      }
    }.toSeq.traverse(identity)

  private def validateWithFlush(kcql: Kcql): Either[Throwable, Unit] = {
    val sql = kcql.getQuery.toUpperCase()
    if (
      sql.contains("WITH_FLUSH_COUNT") || sql.contains("WITH_FLUSH_INTERVAL") ||
      sql.contains("WITH_FLUSH_SIZE")
    ) {
      new IllegalArgumentException(WithFlushErrorMessage).asLeft
    } else {
      ().asRight
    }
  }

  private def validateNoMoreWithPartitioner(kcql: Kcql): Either[ConfigException, Unit] =
    if (kcql.getQuery.toUpperCase().contains("WITHPARTITIONER")) {
      new ConfigException(
        WithPartitionerError,
      ).asLeft
    } else {
      ().asRight
    }

  private def validateCommitPolicyForBytesFormat(
    formatSelection: FormatSelection,
    commitPolicy:    CommitPolicy,
  ): Either[Throwable, Unit] =
    formatSelection match {
      case BytesFormatSelection if commitPolicy.conditions.contains(Count(1L)) => ().asRight
      case BytesFormatSelection =>
        new IllegalArgumentException(
          s"${FlushCount.entryName} > 1 is not allowed for BYTES. If you want to store N records as raw bytes use AVRO or PARQUET. If you are using BYTES but not specified a ${FlushCount.entryName}, then do so by adding ${FlushCount.entryName} = 1 to your KCQL PROPERTIES section.",
        ).asLeft
      case _ => ().asRight
    }

  private def validateEnvelopeAndFormat(
    format:   FormatSelection,
    settings: DataStorageSettings,
  ): Either[Throwable, Unit] =
    if (!settings.envelope) ().asRight
    else {
      if (format.supportsEnvelope) ().asRight
      else
        new IllegalArgumentException(s"Envelope is not supported for format ${format.extension.toUpperCase()}.").asLeft
    }
}

case class CloudSinkBucketOptions(
  sourceTopic:      Option[String],
  bucketAndPrefix:  CloudLocation,
  formatSelection:  FormatSelection,
  keyNamer:         KeyNamer,
  commitPolicy:     CommitPolicy = CloudCommitPolicy.Default,
  localStagingArea: LocalStagingArea,
  dataStorage:      DataStorageSettings,
) extends WithTransformableDataStorage
