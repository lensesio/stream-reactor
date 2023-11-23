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
package io.lenses.streamreactor.connect.aws.s3.sink

import com.datamountaineer.streamreactor.common.errors.RetryErrorPolicy
import com.datamountaineer.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.S3Config
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.sink.WriterManagerCreator
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try

object S3SinkTask {}

class S3SinkTask
    extends CloudSinkTask[S3FileMetadata](
      S3ConfigSettings.CONNECTOR_PREFIX,
      "/aws-s3-sink-ascii.txt",
      JarManifest(S3SinkTask.getClass.getProtectionDomain.getCodeSource.getLocation),
    )(
      S3LocationValidator,
    ) {

  private val writerManagerCreator = new WriterManagerCreator[S3FileMetadata, S3SinkConfig]()

  def createWriterMan(props: Map[String, String]): Either[Throwable, WriterManager[S3FileMetadata]] =
    for {
      config          <- S3SinkConfig.fromProps(props.asJava)
      s3Client        <- AwsS3ClientCreator.make(config.s3Config)
      storageInterface = new AwsS3StorageInterface(connectorTaskId, s3Client, config.batchDelete)
      _               <- Try(setErrorRetryInterval(config.s3Config)).toEither
      writerManager   <- Try(writerManagerCreator.from(config)(connectorTaskId, storageInterface)).toEither
      _ <- Try(initialize(
        config.s3Config.connectorRetryConfig.numberOfRetries,
        config.s3Config.errorPolicy,
      )).toEither
    } yield writerManager

  private def setErrorRetryInterval(s3Config: S3Config): Unit =
    //if error policy is retry set retry interval
    s3Config.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(s3Config.connectorRetryConfig.errorRetryInterval)
      case _                  =>
    }

}
