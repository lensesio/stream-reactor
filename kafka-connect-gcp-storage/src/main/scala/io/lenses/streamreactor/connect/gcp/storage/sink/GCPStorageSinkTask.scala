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
package io.lenses.streamreactor.connect.gcp.storage.sink

import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.sink.WriterManagerCreator
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.gcp.storage.auth.GCPStorageClientCreator
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConnectionConfig
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings
import io.lenses.streamreactor.connect.gcp.storage.model.location.GCPStorageLocationValidator
import io.lenses.streamreactor.connect.gcp.storage.sink.config.GCPStorageSinkConfig
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageStorageInterface

import scala.util.Try

object GCPStorageSinkTask {}
class GCPStorageSinkTask
    extends CloudSinkTask[GCPStorageFileMetadata](
      GCPConfigSettings.CONNECTOR_PREFIX,
      "/gcpstorage-sink-ascii.txt",
      JarManifest(GCPStorageSinkTask.getClass.getProtectionDomain.getCodeSource.getLocation),
    )(
      GCPStorageLocationValidator,
    ) {

  private val writerManagerCreator = new WriterManagerCreator[GCPStorageFileMetadata, GCPStorageSinkConfig]()

  def createWriterMan(props: Map[String, String]): Either[Throwable, WriterManager[GCPStorageFileMetadata]] =
    for {
      config          <- GCPStorageSinkConfig.fromProps(props)
      gcpClient       <- GCPStorageClientCreator.make(config.gcpConfig)
      storageInterface = new GCPStorageStorageInterface(connectorTaskId, gcpClient, config.avoidResumableUpload)
      _               <- Try(setErrorRetryInterval(config.gcpConfig)).toEither
      writerManager   <- Try(writerManagerCreator.from(config)(connectorTaskId, storageInterface)).toEither
      _ <- Try(initialize(
        config.gcpConfig.connectorRetryConfig.numberOfRetries,
        config.gcpConfig.errorPolicy,
      )).toEither
    } yield writerManager

  private def setErrorRetryInterval(gcpConfig: GCPConnectionConfig): Unit =
    //if error policy is retry set retry interval
    gcpConfig.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(gcpConfig.connectorRetryConfig.errorRetryInterval)
      case _                  =>
    }

}
