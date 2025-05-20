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
package io.lenses.streamreactor.connect.gcp.storage.sink

import com.google.cloud.storage.Storage
import io.lenses.streamreactor.common.util.EitherUtils
import io.lenses.streamreactor.common.util.JarManifest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface

import io.lenses.streamreactor.connect.gcp.common.auth.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.storage.auth.GCPStorageClientCreator
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings
import io.lenses.streamreactor.connect.gcp.storage.model.location.GCPStorageLocationValidator
import io.lenses.streamreactor.connect.gcp.storage.sink.config.GCPStorageSinkConfig
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageStorageInterface

object GCPStorageSinkTask {}
class GCPStorageSinkTask
    extends CloudSinkTask[
      GCPStorageFileMetadata,
      GCPStorageSinkConfig,
      GCPConnectionConfig,
      Storage,
    ](
      GCPConfigSettings.CONNECTOR_PREFIX,
      "/gcpstorage-sink-ascii.txt",
      EitherUtils.unpackOrThrow(JarManifest.produceFromClass(GCPStorageSinkTask.getClass)),
    ) {

  override def createClient(config: GCPConnectionConfig): Either[Throwable, Storage] =
    GCPStorageClientCreator.make(config)

  override def createStorageInterface(
    connectorTaskId: ConnectorTaskId,
    config:          GCPStorageSinkConfig,
    cloudClient:     Storage,
  ): StorageInterface[GCPStorageFileMetadata] =
    new GCPStorageStorageInterface(connectorTaskId,
                                   storage             = cloudClient,
                                   avoidReumableUpload = config.avoidResumableUpload,
                                   extensionFilter     = Option.empty,
    )

  override def convertPropsToConfig(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, String],
  ): Either[Throwable, GCPStorageSinkConfig] =
    GCPStorageSinkConfig.fromProps(connectorTaskId, props)(GCPStorageLocationValidator)

}
