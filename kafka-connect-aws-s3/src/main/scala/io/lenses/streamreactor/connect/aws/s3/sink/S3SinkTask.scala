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
package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.common.util.JarManifest
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.sink.WriterManagerCreator
import software.amazon.awssdk.services.s3.S3Client

object S3SinkTask {}

class S3SinkTask
    extends CloudSinkTask[S3FileMetadata, S3SinkConfig, S3Client](
      S3ConfigSettings.CONNECTOR_PREFIX,
      "/aws-s3-sink-ascii.txt",
      new JarManifest(S3SinkTask.getClass.getProtectionDomain.getCodeSource.getLocation),
    ) {

  val writerManagerCreator = new WriterManagerCreator[S3FileMetadata, S3SinkConfig]()

  override def createStorageInterface(
    connectorTaskId: ConnectorTaskId,
    config:          S3SinkConfig,
    cloudClient:     S3Client,
  ): AwsS3StorageInterface =
    new AwsS3StorageInterface(connectorTaskId, cloudClient, config.batchDelete)

  override def createClient(config: S3SinkConfig): Either[Throwable, S3Client] =
    AwsS3ClientCreator.make(config.connectionConfig)

  override def convertPropsToConfig(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, String],
  ): Either[Throwable, S3SinkConfig] = S3SinkConfig.fromProps(connectorTaskId, props)(S3LocationValidator)

}
