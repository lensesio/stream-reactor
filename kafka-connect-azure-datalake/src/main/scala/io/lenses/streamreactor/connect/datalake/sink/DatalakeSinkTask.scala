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
package io.lenses.streamreactor.connect.datalake.sink

import com.azure.storage.file.datalake.DataLakeServiceClient
import io.lenses.streamreactor.common.util.{EitherUtils, JarManifest}
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.guard.KafkaGuardSupport
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.sink.config.GuardConfigKeys
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.datalake.auth.DatalakeClientCreator
import io.lenses.streamreactor.connect.datalake.config.{AzureConfigSettings, AzureConnectionConfig}
import io.lenses.streamreactor.connect.datalake.model.location.DatalakeLocationValidator
import io.lenses.streamreactor.connect.datalake.sink.config.DatalakeSinkConfig
import io.lenses.streamreactor.connect.datalake.storage.{DatalakeFileMetadata, DatalakeStorageInterface}
import org.apache.kafka.common.TopicPartition

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
      EitherUtils.unpackOrThrow(JarManifest.produceFromClass(DatalakeSinkTask.getClass)),
    )
    with KafkaGuardSupport {

  override def guardConfigKeys: GuardConfigKeys =
    new GuardConfigKeys { override def connectorPrefix: String = AzureConfigSettings.CONNECTOR_PREFIX }

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

  override def open(partitions: java.util.Collection[TopicPartition]): Unit = {
    super.open(partitions)
    onOpenGuard(partitions)
  }

  override def close(partitions: java.util.Collection[TopicPartition]): Unit =
    try { onCloseGuard(partitions) }
    finally super.close(partitions)

  override def stop(): Unit =
    try { onStopGuard() }
    finally super.stop()

  override protected def preWriteHeartbeat(
    records: java.util.Collection[org.apache.kafka.connect.sink.SinkRecord],
  ): Unit =
    preWriteHeartbeatGuard()
}
