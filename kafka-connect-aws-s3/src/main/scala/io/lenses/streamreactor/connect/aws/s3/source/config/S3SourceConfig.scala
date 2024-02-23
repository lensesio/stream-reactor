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

import io.lenses.streamreactor.connect.aws.s3.config.S3ConnectionConfig
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSourceConfig
import io.lenses.streamreactor.connect.cloud.common.config.traits.PropsToConfigConverter
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceBucketOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions

object S3SourceConfig extends PropsToConfigConverter[S3SourceConfig] {

  implicit val CloudLocationValidator: CloudLocationValidator = S3LocationValidator

  override def fromProps(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, String],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, S3SourceConfig] =
    apply(S3SourceConfigDefBuilder(props))

  def apply(s3ConfigDefBuilder: S3SourceConfigDefBuilder): Either[Throwable, S3SourceConfig] = {
    val parsedValues = s3ConfigDefBuilder.getParsedValues
    for {
      sbo <- CloudSourceBucketOptions[S3FileMetadata](
        s3ConfigDefBuilder,
        s3ConfigDefBuilder.getPartitionExtractor(parsedValues),
      )
    } yield S3SourceConfig(
      S3ConnectionConfig(parsedValues),
      sbo,
      s3ConfigDefBuilder.getCompressionCodec(),
      s3ConfigDefBuilder.getPartitionSearcherOptions(parsedValues),
      s3ConfigDefBuilder.batchDelete(),
    )

  }

}

case class S3SourceConfig(
  connectionConfig:  S3ConnectionConfig,
  bucketOptions:     Seq[CloudSourceBucketOptions[S3FileMetadata]] = Seq.empty,
  compressionCodec:  CompressionCodec,
  partitionSearcher: PartitionSearcherOptions,
  batchDelete:       Boolean,
) extends CloudSourceConfig[S3FileMetadata]
