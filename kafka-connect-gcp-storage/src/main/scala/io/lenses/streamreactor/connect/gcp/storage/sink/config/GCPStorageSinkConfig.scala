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
package io.lenses.streamreactor.connect.gcp.storage.sink.config

import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.config.traits.PropsToConfigConverter
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.OffsetSeekerOptions
import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.SEEK_MAX_INDEX_FILES

object GCPStorageSinkConfig extends PropsToConfigConverter[GCPStorageSinkConfig] {

  def fromProps(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, AnyRef],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, GCPStorageSinkConfig] =
    GCPStorageSinkConfig(connectorTaskId, GCPStorageSinkConfigDefBuilder(props))

  def apply(
    connectorTaskId:     ConnectorTaskId,
    gcpConfigDefBuilder: GCPStorageSinkConfigDefBuilder,
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, GCPStorageSinkConfig] =
    for {
      gcpConnectionSettings <- gcpConfigDefBuilder.getGcpConnectionSettings(gcpConfigDefBuilder.props)
      sinkBucketOptions     <- CloudSinkBucketOptions(connectorTaskId, gcpConfigDefBuilder)
      offsetSeekerOptions = OffsetSeekerOptions(
        gcpConfigDefBuilder.getInt(SEEK_MAX_INDEX_FILES),
      )
    } yield GCPStorageSinkConfig(
      gcpConnectionSettings,
      sinkBucketOptions,
      offsetSeekerOptions,
      gcpConfigDefBuilder.getCompressionCodec(),
      avoidResumableUpload = gcpConfigDefBuilder.isAvoidResumableUpload,
      errorPolicy          = gcpConfigDefBuilder.getErrorPolicyOrDefault,
      connectorRetryConfig = gcpConfigDefBuilder.getRetryConfig,
    )

}

case class GCPStorageSinkConfig(
  connectionConfig:     GCPConnectionConfig,
  bucketOptions:        Seq[CloudSinkBucketOptions] = Seq.empty,
  offsetSeekerOptions:  OffsetSeekerOptions,
  compressionCodec:     CompressionCodec,
  avoidResumableUpload: Boolean,
  connectorRetryConfig: RetryConfig,
  errorPolicy:          ErrorPolicy,
) extends CloudSinkConfig[GCPConnectionConfig]
