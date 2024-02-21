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
package io.lenses.streamreactor.connect.aws.s3.source.config

import cats.implicits.toTraverseOps
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SOURCE_ORDERING_TYPE
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SOURCE_PARTITION_EXTRACTOR_REGEX
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.SOURCE_PARTITION_EXTRACTOR_TYPE
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.config.ConfigParse.getString
import io.lenses.streamreactor.connect.cloud.common.config.FormatSelection
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEntry
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.OrderingType
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionExtractor
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.CloudSourceProps
import io.lenses.streamreactor.connect.cloud.common.source.config.kcqlprops.CloudSourcePropsSchema
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.config.kcqlprops.KcqlProperties

import scala.util.Try

object S3SourceConfig {

  def fromProps(
    props: Map[String, String],
  ): Either[Throwable, S3SourceConfig] =
    S3SourceConfig(S3SourceConfigDefBuilder(props))

  def apply(s3ConfigDefBuilder: S3SourceConfigDefBuilder): Either[Throwable, S3SourceConfig] = {
    val parsedValues = s3ConfigDefBuilder.getParsedValues
    for {
      sbo <- SourceBucketOptions(
        s3ConfigDefBuilder,
        PartitionExtractor(
          getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_TYPE).getOrElse("none"),
          getString(parsedValues, SOURCE_PARTITION_EXTRACTOR_REGEX),
        ),
      )
    } yield S3SourceConfig(
      S3Config(parsedValues),
      sbo,
      s3ConfigDefBuilder.getCompressionCodec(),
      s3ConfigDefBuilder.getPartitionSearcherOptions(parsedValues),
      s3ConfigDefBuilder.batchDelete(),
    )

  }
}

case class S3SourceConfig(
  s3Config:          S3Config,
  bucketOptions:     Seq[SourceBucketOptions] = Seq.empty,
  compressionCodec:  CompressionCodec,
  partitionSearcher: PartitionSearcherOptions,
  batchDelete:       Boolean,
)

case class SourceBucketOptions(
  sourceBucketAndPrefix: CloudLocation,
  targetTopic:           String,
  format:                FormatSelection,
  recordsLimit:          Int,
  filesLimit:            Int,
  partitionExtractor:    Option[PartitionExtractor],
  orderingType:          OrderingType,
  hasEnvelope:           Boolean,
) {
  def createBatchListerFn(
    storageInterface: StorageInterface[S3FileMetadata],
  ): Option[S3FileMetadata] => Either[FileListError, Option[ListOfKeysResponse[S3FileMetadata]]] =
    orderingType
      .getBatchLister
      .listBatch(
        storageInterface = storageInterface,
        bucket           = sourceBucketAndPrefix.bucket,
        prefix           = sourceBucketAndPrefix.prefix,
        numResults       = filesLimit,
      )

  def getPartitionExtractorFn: String => Option[Int] =
    partitionExtractor.fold((_: String) => Option.empty[Int])(_.extract)

}

object SourceBucketOptions {
  private val DEFAULT_RECORDS_LIMIT = 10000
  private val DEFAULT_FILES_LIMIT   = 1000

  implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator

  def apply(
    config:             S3SourceConfigDefBuilder,
    partitionExtractor: Option[PartitionExtractor],
  ): Either[Throwable, Seq[SourceBucketOptions]] =
    config.getKCQL.map {
      kcql: Kcql =>
        for {
          source     <- CloudLocation.splitAndValidate(kcql.getSource)
          format     <- FormatSelection.fromKcql(kcql, CloudSourcePropsSchema.schema)
          sourceProps = CloudSourceProps.fromKcql(kcql)

          //extract the envelope. of not present default to false
          hasEnvelope <- extractEnvelope(sourceProps)

        } yield SourceBucketOptions(
          source,
          kcql.getTarget,
          format             = format,
          recordsLimit       = if (kcql.getLimit < 1) DEFAULT_RECORDS_LIMIT else kcql.getLimit,
          filesLimit         = if (kcql.getBatchSize < 1) DEFAULT_FILES_LIMIT else kcql.getBatchSize,
          partitionExtractor = partitionExtractor,
          orderingType = Try(config.getString(SOURCE_ORDERING_TYPE)).toOption.flatMap(
            OrderingType.withNameInsensitiveOption,
          ).getOrElse(OrderingType.AlphaNumeric),
          hasEnvelope = hasEnvelope.getOrElse(false),
        )
    }.toSeq.traverse(identity)

  private def extractEnvelope(
    properties: KcqlProperties[PropsKeyEntry, PropsKeyEnum.type],
  ): Either[Throwable, Option[Boolean]] =
    properties.getOptionalBoolean(PropsKeyEnum.StoreEnvelope)
}
