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
package io.lenses.streamreactor.connect.aws.s3.sink.config
import cats.syntax.all._
import com.datamountaineer.kcql.Kcql
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.DataStorageSettings
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SEEK_MAX_INDEX_FILES
import io.lenses.streamreactor.connect.aws.s3.config._
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodec
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.aws.s3.sink.commit.Count
import io.lenses.streamreactor.connect.aws.s3.sink.naming.HierarchicalS3FileNamer
import io.lenses.streamreactor.connect.aws.s3.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.aws.s3.sink.naming.PartitionedS3FileNamer
import io.lenses.streamreactor.connect.aws.s3.sink.naming.S3KeyNamer

import java.util
import scala.jdk.CollectionConverters._

object S3SinkConfig {

  def fromProps(
    props: util.Map[String, String],
  )(
    implicit
    connectorTaskId: ConnectorTaskId,
  ): Either[Throwable, S3SinkConfig] =
    S3SinkConfig(S3SinkConfigDefBuilder(props))

  def apply(
    s3ConfigDefBuilder: S3SinkConfigDefBuilder,
  )(
    implicit
    connectorTaskId: ConnectorTaskId,
  ): Either[Throwable, S3SinkConfig] =
    for {
      sinkBucketOptions <- SinkBucketOptions(s3ConfigDefBuilder)
      offsetSeekerOptions = OffsetSeekerOptions(
        s3ConfigDefBuilder.getInt(SEEK_MAX_INDEX_FILES),
      )
    } yield S3SinkConfig(
      S3Config(s3ConfigDefBuilder.getParsedValues),
      sinkBucketOptions,
      offsetSeekerOptions,
      s3ConfigDefBuilder.getCompressionCodec(),
      s3ConfigDefBuilder.batchDelete(),
    )

}

case class S3SinkConfig(
  s3Config:            S3Config,
  bucketOptions:       Seq[SinkBucketOptions] = Seq.empty,
  offsetSeekerOptions: OffsetSeekerOptions,
  compressionCodec:    CompressionCodec,
  batchDelete:         Boolean,
)

object SinkBucketOptions extends LazyLogging {

  def apply(
    config: S3SinkConfigDefBuilder,
  )(
    implicit
    connectorTaskId: ConnectorTaskId,
  ): Either[Throwable, Seq[SinkBucketOptions]] =
    config.getKCQL.map { kcql: Kcql =>
      for {
        formatSelection   <- FormatSelection.fromKcql(kcql)
        partitionSelection = PartitionSelection(kcql)

        fileNamer = if (partitionSelection.isCustom) {
          PartitionedS3FileNamer
        } else {
          HierarchicalS3FileNamer
        }
        keyNamer     = new S3KeyNamer(formatSelection, config.getPaddingStrategy().padString, partitionSelection, fileNamer)
        stagingArea <- LocalStagingArea(config)
        target      <- S3Location.splitAndValidate(kcql.getTarget, allowSlash = false)
        storageSettings <- DataStorageSettings.from(
          Option(kcql.getProperties).map(_.asScala.toMap).getOrElse(Map.empty),
        )
        _           <- validateEnvelopeAndFormat(formatSelection, storageSettings)
        commitPolicy = config.commitPolicy(kcql)
        _           <- validateCommitPolicyForBytesFormat(formatSelection, commitPolicy)
      } yield {
        SinkBucketOptions(
          Option(kcql.getSource).filterNot(Set("*", "`*`").contains(_)),
          target,
          formatSelection    = formatSelection,
          keyNamer           = keyNamer,
          partitionSelection = partitionSelection,
          commitPolicy       = config.commitPolicy(kcql),
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

case class SinkBucketOptions(
  sourceTopic:        Option[String],
  bucketAndPrefix:    S3Location,
  formatSelection:    FormatSelection,
  keyNamer:           KeyNamer,
  partitionSelection: PartitionSelection,
  commitPolicy:       CommitPolicy = CommitPolicy.Default,
  localStagingArea:   LocalStagingArea,
  dataStorage:        DataStorageSettings,
)

case class OffsetSeekerOptions(
  maxIndexFiles: Int,
)
