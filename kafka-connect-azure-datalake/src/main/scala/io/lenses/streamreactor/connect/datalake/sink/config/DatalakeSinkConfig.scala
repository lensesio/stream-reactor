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
package io.lenses.streamreactor.connect.datalake.sink.config

import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.config.traits.PropsToConfigConverter
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.OffsetSeekerOptions
import io.lenses.streamreactor.connect.datalake.config.AzureConnectionConfig
import io.lenses.streamreactor.connect.datalake.config.AzureConfigSettings.SEEK_MAX_INDEX_FILES

object DatalakeSinkConfig extends PropsToConfigConverter[DatalakeSinkConfig] {

  def fromProps(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, AnyRef],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, DatalakeSinkConfig] =
    DatalakeSinkConfig(connectorTaskId, DatalakeSinkConfigDefBuilder(props))

  def apply(
    connectorTaskId:    ConnectorTaskId,
    s3ConfigDefBuilder: DatalakeSinkConfigDefBuilder,
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, DatalakeSinkConfig] =
    for {
      authMode          <- s3ConfigDefBuilder.getAuthMode
      sinkBucketOptions <- CloudSinkBucketOptions(connectorTaskId, s3ConfigDefBuilder)
      offsetSeekerOptions = OffsetSeekerOptions(
        s3ConfigDefBuilder.getInt(SEEK_MAX_INDEX_FILES),
      )
    } yield DatalakeSinkConfig(
      AzureConnectionConfig(s3ConfigDefBuilder.getParsedValues, authMode),
      sinkBucketOptions,
      offsetSeekerOptions,
      s3ConfigDefBuilder.getCompressionCodec(),
      s3ConfigDefBuilder.getErrorPolicyOrDefault,
      s3ConfigDefBuilder.getRetryConfig,
    )

}

case class DatalakeSinkConfig(
  connectionConfig:     AzureConnectionConfig,
  bucketOptions:        Seq[CloudSinkBucketOptions] = Seq.empty,
  offsetSeekerOptions:  OffsetSeekerOptions,
  compressionCodec:     CompressionCodec,
  errorPolicy:          ErrorPolicy,
  connectorRetryConfig: RetryConfig,
) extends CloudSinkConfig[AzureConnectionConfig]
