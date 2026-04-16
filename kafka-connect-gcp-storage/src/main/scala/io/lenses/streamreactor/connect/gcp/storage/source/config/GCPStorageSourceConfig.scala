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
package io.lenses.streamreactor.connect.gcp.storage.source.config

import io.lenses.streamreactor.common.config.source.ConfigWrapperSource
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSourceConfig
import io.lenses.streamreactor.connect.cloud.common.config.traits.PropsToConfigConverter
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceBucketOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.EmptySourceBackoffSettings
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.cloud.common.storage.ExtensionFilter
import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.storage.model.location.GCPStorageLocationValidator
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata

import scala.util.Try

object GCPStorageSourceConfig extends PropsToConfigConverter[GCPStorageSourceConfig] {

  implicit val CloudLocationValidator: CloudLocationValidator = GCPStorageLocationValidator

  override def fromProps(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, AnyRef],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, GCPStorageSourceConfig] =
    Try(GCPStorageSourceConfig(GCPStorageSourceConfigDefBuilder(props))).toEither.flatten

  def apply(gcpConfigDefBuilder: GCPStorageSourceConfigDefBuilder): Either[Throwable, GCPStorageSourceConfig] = {
    val parsedValues     = gcpConfigDefBuilder.getParsedValues
    val configMapVersion = new ConfigWrapperSource(gcpConfigDefBuilder)
    for {
      gcpConnectionSettings <- gcpConfigDefBuilder.getGcpConnectionSettings(configMapVersion)
      sbo <- CloudSourceBucketOptions[GCPStorageFileMetadata](
        gcpConfigDefBuilder,
        gcpConfigDefBuilder.getPartitionExtractor(parsedValues),
      )
      backoff = gcpConfigDefBuilder.getEmptySourceBackoffSettings(parsedValues)
    } yield GCPStorageSourceConfig(
      gcpConnectionSettings,
      sbo,
      gcpConfigDefBuilder.getCompressionCodec(),
      gcpConfigDefBuilder.getPartitionSearcherOptions(parsedValues),
      gcpConfigDefBuilder.getSourceExtensionFilter,
      backoff,
      gcpConfigDefBuilder.getWriteWatermarkToHeaders,
      gcpConfigDefBuilder.getLateArrivalInterval,
    )

  }

}

case class GCPStorageSourceConfig(
  connectionConfig:           GCPConnectionConfig,
  bucketOptions:              Seq[CloudSourceBucketOptions[GCPStorageFileMetadata]] = Seq.empty,
  compressionCodec:           CompressionCodec,
  partitionSearcher:          PartitionSearcherOptions,
  extensionFilter:            Option[ExtensionFilter],
  emptySourceBackoffSettings: EmptySourceBackoffSettings,
  writeWatermarkToHeaders:    Boolean,
  lateArrivalInterval:        Int,
) extends CloudSourceConfig[GCPStorageFileMetadata]
