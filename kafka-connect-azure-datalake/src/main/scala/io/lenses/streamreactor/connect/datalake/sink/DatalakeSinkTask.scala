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
package io.lenses.streamreactor.connect.datalake.sink

import com.azure.storage.file.datalake.DataLakeServiceClient
import io.lenses.streamreactor.common.util.JarManifest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.datalake.auth.DatalakeClientCreator
import io.lenses.streamreactor.connect.datalake.config.AzureConfigSettings
import io.lenses.streamreactor.connect.datalake.config.AzureConnectionConfig
import io.lenses.streamreactor.connect.datalake.model.location.DatalakeLocationValidator
import io.lenses.streamreactor.connect.datalake.sink.config.DatalakeSinkConfig
import io.lenses.streamreactor.connect.datalake.storage.DatalakeFileMetadata
import io.lenses.streamreactor.connect.datalake.storage.DatalakeStorageInterface
object DatalakeSinkTask {}
class DatalakeSinkTask
    extends CloudSinkTask[
      DatalakeFileMetadata,
      DatalakeSinkConfig,
      AzureConnectionConfig,
      DataLakeServiceClient,
    ](
      AzureConfigSettings.CONNECTOR_PREFIX,
      "/datalake-sink-ascii.txt",
      new JarManifest(DatalakeSinkTask.getClass.getProtectionDomain.getCodeSource.getLocation),
    ) {

  override def createClient(config: AzureConnectionConfig): Either[Throwable, DataLakeServiceClient] =
    DatalakeClientCreator.make(config)

  override def createStorageInterface(
    connectorTaskId: ConnectorTaskId,
    config:          DatalakeSinkConfig,
    cloudClient:     DataLakeServiceClient,
  ): StorageInterface[DatalakeFileMetadata] = new DatalakeStorageInterface(connectorTaskId, cloudClient)

  override def convertPropsToConfig(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, String],
  ): Either[Throwable, DatalakeSinkConfig] =
    DatalakeSinkConfig.fromProps(connectorTaskId, props)(DatalakeLocationValidator)

}
