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
import io.lenses.streamreactor.common.config.source.ConfigWrapperSource
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.config.traits.PropsToConfigConverter
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.IndexOptions
import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.LOG_METRICS_CONFIG

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
  ): Either[Throwable, GCPStorageSinkConfig] = {
    val configSource = new ConfigWrapperSource(gcpConfigDefBuilder)
    for {
      gcpConnectionSettings <- gcpConfigDefBuilder.getGcpConnectionSettings(configSource)
      sinkBucketOptions     <- CloudSinkBucketOptions(connectorTaskId, gcpConfigDefBuilder)
      indexOptions           = gcpConfigDefBuilder.getIndexSettings
      logMetrics             = gcpConfigDefBuilder.getBoolean(LOG_METRICS_CONFIG)
    } yield GCPStorageSinkConfig(
      gcpConnectionSettings,
      sinkBucketOptions,
      indexOptions,
      gcpConfigDefBuilder.getCompressionCodec(),
      avoidResumableUpload          = gcpConfigDefBuilder.isAvoidResumableUpload,
      errorPolicy                   = gcpConfigDefBuilder.getErrorPolicyOrDefault,
      connectorRetryConfig          = gcpConfigDefBuilder.getRetryConfig,
      logMetrics                    = logMetrics,
      rolloverOnSchemaChangeEnabled = gcpConfigDefBuilder.shouldRollOverOnSchemaChange(),
    )
  }

}

case class GCPStorageSinkConfig(
  connectionConfig:              GCPConnectionConfig,
  bucketOptions:                 Seq[CloudSinkBucketOptions] = Seq.empty,
  indexOptions:                  Option[IndexOptions],
  compressionCodec:              CompressionCodec,
  avoidResumableUpload:          Boolean,
  connectorRetryConfig:          RetryConfig,
  errorPolicy:                   ErrorPolicy,
  logMetrics:                    Boolean,
  rolloverOnSchemaChangeEnabled: Boolean,
) extends CloudSinkConfig[GCPConnectionConfig]
