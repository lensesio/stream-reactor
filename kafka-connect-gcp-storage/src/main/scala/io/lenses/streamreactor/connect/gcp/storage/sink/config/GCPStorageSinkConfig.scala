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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.OffsetSeekerOptions
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.SEEK_MAX_INDEX_FILES

object GCPStorageSinkConfig {

  def fromProps(
    props: Map[String, String],
  )(
    implicit
    connectorTaskId:        ConnectorTaskId,
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, GCPStorageSinkConfig] =
    GCPStorageSinkConfig(GCPStorageSinkConfigDefBuilder(props))

  def apply(
    gcpConfigDefBuilder: GCPStorageSinkConfigDefBuilder,
  )(
    implicit
    connectorTaskId:        ConnectorTaskId,
    cloudLocationValidator: CloudLocationValidator,
  ): Either[Throwable, GCPStorageSinkConfig] =
    for {
      authMode          <- gcpConfigDefBuilder.getAuthMode
      sinkBucketOptions <- CloudSinkBucketOptions(gcpConfigDefBuilder)
      offsetSeekerOptions = OffsetSeekerOptions(
        gcpConfigDefBuilder.getInt(SEEK_MAX_INDEX_FILES),
      )
    } yield GCPStorageSinkConfig(
      GCPConnectionConfig(gcpConfigDefBuilder.getParsedValues, authMode),
      sinkBucketOptions,
      offsetSeekerOptions,
      gcpConfigDefBuilder.getCompressionCodec(),
      avoidResumableUpload = gcpConfigDefBuilder.isAvoidResumableUpload,
    )

}

case class GCPStorageSinkConfig(
                                 gcpConfig:            GCPConnectionConfig,
                                 bucketOptions:        Seq[CloudSinkBucketOptions] = Seq.empty,
                                 offsetSeekerOptions:  OffsetSeekerOptions,
                                 compressionCodec:     CompressionCodec,
                                 avoidResumableUpload: Boolean,
) extends CloudSinkConfig
