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
package io.lenses.streamreactor.connect.gcp.storage.source

import com.google.cloud.storage.Storage
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.CloudSourceTask
import io.lenses.streamreactor.connect.cloud.common.storage.DirectoryLister
import io.lenses.streamreactor.connect.gcp.storage.auth.GCPStorageClientCreator
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.gcp.storage.model.location.GCPStorageLocationValidator
import io.lenses.streamreactor.connect.gcp.storage.source.config.GCPStorageSourceConfig
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageDirectoryLister
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageStorageInterface

class GCPStorageSourceTask
    extends CloudSourceTask[
      GCPStorageFileMetadata,
      GCPStorageSourceConfig,
      Storage,
    ]("/gcpstorage-sink-ascii.txt")
    with LazyLogging {

  val validator: CloudLocationValidator = GCPStorageLocationValidator

  override def createStorageInterface(
    connectorTaskId: ConnectorTaskId,
    config:          GCPStorageSourceConfig,
    client:          Storage,
  ): GCPStorageStorageInterface =
    new GCPStorageStorageInterface(connectorTaskId,
                                   storage             = client,
                                   avoidReumableUpload = false,
                                   extensionFilter     = config.extensionFilter,
    )

  override def createClient(config: GCPStorageSourceConfig): Either[Throwable, Storage] =
    GCPStorageClientCreator.make(config.connectionConfig)

  override def convertPropsToConfig(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, String],
  ): Either[Throwable, GCPStorageSourceConfig] = GCPStorageSourceConfig.fromProps(connectorTaskId, props)(validator)

  override def connectorPrefix: String = CONNECTOR_PREFIX

  override def createDirectoryLister(connectorTaskId: ConnectorTaskId, client: Storage): DirectoryLister =
    new GCPStorageDirectoryLister(connectorTaskId, client)
}
