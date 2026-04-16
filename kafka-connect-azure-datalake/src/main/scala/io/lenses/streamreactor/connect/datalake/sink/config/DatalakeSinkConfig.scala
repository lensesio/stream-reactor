/*
 * Copyright 2017-2026 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.IndexOptions
import io.lenses.streamreactor.connect.datalake.config.AzureConnectionConfig
import io.lenses.streamreactor.connect.datalake.config.AzureConfigSettings.LOG_METRICS_CONFIG

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
      authMode               <- s3ConfigDefBuilder.getAuthMode
      sinkBucketOptions      <- CloudSinkBucketOptions(connectorTaskId, s3ConfigDefBuilder)
      indexOptions            = s3ConfigDefBuilder.getIndexSettings
      logMetrics              = s3ConfigDefBuilder.getBoolean(LOG_METRICS_CONFIG)
      schemaChangeDetector    = s3ConfigDefBuilder.schemaChangeDetector()
      useLatestSchemaForWrite = s3ConfigDefBuilder.getEnableLatestSchemaOptimization()
    } yield DatalakeSinkConfig(
      AzureConnectionConfig(s3ConfigDefBuilder.getParsedValues, authMode),
      sinkBucketOptions,
      indexOptions,
      s3ConfigDefBuilder.getCompressionCodec(),
      s3ConfigDefBuilder.getErrorPolicyOrDefault,
      s3ConfigDefBuilder.getRetryConfig,
      logMetrics,
      schemaChangeDetector,
      skipNullValues              = s3ConfigDefBuilder.skipNullValues(),
      latestSchemaForWriteEnabled = useLatestSchemaForWrite,
    )

}

case class DatalakeSinkConfig(
  connectionConfig:            AzureConnectionConfig,
  bucketOptions:               Seq[CloudSinkBucketOptions] = Seq.empty,
  indexOptions:                Option[IndexOptions],
  compressionCodec:            CompressionCodec,
  errorPolicy:                 ErrorPolicy,
  connectorRetryConfig:        RetryConfig,
  logMetrics:                  Boolean,
  schemaChangeDetector:        SchemaChangeDetector,
  skipNullValues:              Boolean,
  latestSchemaForWriteEnabled: Boolean                     = false,
) extends CloudSinkConfig[AzureConnectionConfig]
