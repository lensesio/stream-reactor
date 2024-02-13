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
import io.lenses.kcql.Kcql
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.BytesFormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.config.FormatSelection
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CloudCommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.Count
import io.lenses.streamreactor.connect.cloud.common.sink.config.kcqlprops.CloudSinkProps
import io.lenses.streamreactor.connect.cloud.common.sink.config.kcqlprops.SinkPropsSchema
import io.lenses.streamreactor.connect.cloud.common.sink.config.padding.PaddingService
import io.lenses.streamreactor.connect.cloud.common.sink.naming.CloudKeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.OffsetFileNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.TopicPartitionOffsetFileNamer

object CloudSinkBucketOptions extends LazyLogging {

  def apply(
    config: CloudSinkConfigDefBuilder,
  )(
    implicit
    connectorTaskId:        ConnectorTaskId,
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, Seq[CloudSinkBucketOptions]] =
    config.getKCQL.map { kcql: Kcql =>
      for {
        formatSelection   <- FormatSelection.fromKcql(kcql, SinkPropsSchema.schema)
        sinkProps          = CloudSinkProps.fromKcql(kcql)
        partitionSelection = PartitionSelection(kcql, sinkProps)
        paddingService    <- PaddingService.fromConfig(config, sinkProps)

        fileNamer = if (partitionSelection.isCustom) {
          new TopicPartitionOffsetFileNamer(
            paddingService.padderFor("partition"),
            paddingService.padderFor("offset"),
            formatSelection.extension,
          )
        } else {
          new OffsetFileNamer(
            paddingService.padderFor("offset"),
            formatSelection.extension,
          )
        }
        keyNamer         = CloudKeyNamer(formatSelection, partitionSelection, fileNamer, paddingService)
        stagingArea     <- config.getLocalStagingArea()
        target          <- CloudLocation.splitAndValidate(kcql.getTarget)
        storageSettings <- DataStorageSettings.from(sinkProps)
        _               <- validateEnvelopeAndFormat(formatSelection, storageSettings)
        commitPolicy     = config.commitPolicy(kcql)
        _               <- validateCommitPolicyForBytesFormat(formatSelection, commitPolicy)
      } yield {
        CloudSinkBucketOptions(
          Option(kcql.getSource).filterNot(Set("*", "`*`").contains(_)),
          target,
          formatSelection    = formatSelection,
          keyNamer           = keyNamer,
          partitionSelection = partitionSelection,
          commitPolicy       = commitPolicy,
          localStagingArea   = stagingArea,
          dataStorage        = storageSettings,
        )
      }
    }.toSeq.traverse(identity)

  private def validateCommitPolicyForBytesFormat(
    formatSelection: FormatSelection,
    commitPolicy:    CommitPolicy,
  ): Either[Throwable, Unit] =
    formatSelection match {
      case BytesFormatSelection if commitPolicy.conditions.contains(Count(1L)) => ().asRight
      case BytesFormatSelection =>
        new IllegalArgumentException(
          "FLUSH_COUNT > 1 is not allowed for BYTES. If you want to store N records as raw bytes use AVRO or PARQUET. If you are using BYTES but not specified a FLUSH_COUNT, then do so by adding WITH_FLUSH_COUNT = 1 to your KCQL.",
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
  sourceTopic:        Option[String],
  bucketAndPrefix:    CloudLocation,
  formatSelection:    FormatSelection,
  keyNamer:           KeyNamer,
  partitionSelection: PartitionSelection,
  commitPolicy:       CommitPolicy = CloudCommitPolicy.Default,
  localStagingArea:   LocalStagingArea,
  dataStorage:        DataStorageSettings,
) extends WithTransformableDataStorage
