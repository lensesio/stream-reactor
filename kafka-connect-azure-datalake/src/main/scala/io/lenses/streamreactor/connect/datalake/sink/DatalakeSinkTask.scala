/*
 * Copyright 2017-2023 Lenses.io Ltd
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

import com.datamountaineer.streamreactor.common.errors.RetryErrorPolicy
import com.datamountaineer.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.datalake.auth.DatalakeClientCreator
import io.lenses.streamreactor.connect.datalake.config.AzureConfig
import io.lenses.streamreactor.connect.datalake.config.AzureConfigSettings
import io.lenses.streamreactor.connect.datalake.model.location.DatalakeLocationValidator
import io.lenses.streamreactor.connect.datalake.sink.config.DatalakeSinkConfig
import io.lenses.streamreactor.connect.datalake.storage.DatalakeFileMetadata
import io.lenses.streamreactor.connect.datalake.storage.DatalakeStorageInterface

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try
object DatalakeSinkTask {}
class DatalakeSinkTask
    extends CloudSinkTask[DatalakeFileMetadata](
      AzureConfigSettings.CONNECTOR_PREFIX,
      "/datalake-sink-ascii.txt",
      JarManifest(DatalakeSinkTask.getClass.getProtectionDomain.getCodeSource.getLocation),
    )(
      DatalakeLocationValidator,
    ) {

  def createWriterMan(props: Map[String, String]): Either[Throwable, WriterManager[DatalakeFileMetadata]] =
    for {
      config          <- DatalakeSinkConfig.fromProps(props.asJava)
      s3Client        <- DatalakeClientCreator.make(config.s3Config)
      storageInterface = new DatalakeStorageInterface(connectorTaskId, s3Client)
      _               <- Try(setErrorRetryInterval(config.s3Config)).toEither
      writerManager   <- Try(DatalakeWriterManagerCreator.from(config)(connectorTaskId, storageInterface)).toEither
      _ <- Try(initialize(
        config.s3Config.connectorRetryConfig.numberOfRetries,
        config.s3Config.errorPolicy,
      )).toEither
    } yield writerManager

  private def setErrorRetryInterval(s3Config: AzureConfig): Unit =
    //if error policy is retry set retry interval
    s3Config.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(s3Config.connectorRetryConfig.errorRetryInterval)
      case _                  =>
    }

}
