/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.LOG_METRICS_CONFIG
import io.lenses.streamreactor.connect.aws.s3.config.S3ConnectionConfig
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.config.traits.PropsToConfigConverter
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.IndexOptions

import scala.util.Try

object S3SinkConfig extends PropsToConfigConverter[S3SinkConfig] {

  override def fromProps(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, AnyRef],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, S3SinkConfig] =
    for {
      conf         <- Try(S3SinkConfigDefBuilder(props)).toEither
      s3SinkConfig <- S3SinkConfig.apply(connectorTaskId, conf)
    } yield s3SinkConfig

  private def apply(
    connectorTaskId:    ConnectorTaskId,
    s3ConfigDefBuilder: S3SinkConfigDefBuilder,
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, S3SinkConfig] =
    for {
      sinkBucketOptions <- CloudSinkBucketOptions(connectorTaskId, s3ConfigDefBuilder)
      indexOptions       = s3ConfigDefBuilder.getIndexSettings
      logMetrics         = s3ConfigDefBuilder.getBoolean(LOG_METRICS_CONFIG)
    } yield S3SinkConfig(
      S3ConnectionConfig(s3ConfigDefBuilder.getParsedValues),
      sinkBucketOptions,
      indexOptions,
      s3ConfigDefBuilder.getCompressionCodec(),
      s3ConfigDefBuilder.batchDelete(),
      errorPolicy          = s3ConfigDefBuilder.getErrorPolicyOrDefault,
      connectorRetryConfig = s3ConfigDefBuilder.getRetryConfig,
      logMetrics           = logMetrics,
      s3ConfigDefBuilder.shouldRollOverOnSchemaChange(),
    )

}

case class S3SinkConfig(
  connectionConfig:              S3ConnectionConfig,
  bucketOptions:                 Seq[CloudSinkBucketOptions] = Seq.empty,
  indexOptions:                  Option[IndexOptions],
  compressionCodec:              CompressionCodec,
  batchDelete:                   Boolean,
  errorPolicy:                   ErrorPolicy,
  connectorRetryConfig:          RetryConfig,
  logMetrics:                    Boolean,
  rolloverOnSchemaChangeEnabled: Boolean,
) extends CloudSinkConfig[S3ConnectionConfig]
