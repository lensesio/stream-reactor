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
package io.lenses.streamreactor.connect.aws.s3.source
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.auth.AwsS3ClientCreator
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.CONNECTOR_PREFIX
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.aws.s3.source.config.S3SourceConfig
import io.lenses.streamreactor.connect.aws.s3.source.distribution.S3PartitionSearcher
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.storage.S3FileMetadata
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.source.CloudSourceTask
import io.lenses.streamreactor.connect.cloud.common.source.state.PartitionSearcher
import software.amazon.awssdk.services.s3.S3Client

import scala.jdk.CollectionConverters.IteratorHasAsScala

class S3SourceTask
    extends CloudSourceTask[
      S3FileMetadata,
      S3SourceConfig,
      S3Client,
    ]
    with LazyLogging {

  implicit val validator: CloudLocationValidator = S3LocationValidator

  override def createStorageInterface(
    connectorTaskId: ConnectorTaskId,
    config:          S3SourceConfig,
    s3Client:        S3Client,
  ): AwsS3StorageInterface =
    new AwsS3StorageInterface(connectorTaskId, s3Client, config.batchDelete)

  override def createClient(config: S3SourceConfig): Either[Throwable, S3Client] =
    AwsS3ClientCreator.make(config.connectionConfig)

  override def convertPropsToConfig(
    connectorTaskId: ConnectorTaskId,
    props:           Map[String, String],
  ): Either[Throwable, S3SourceConfig] = S3SourceConfig.fromProps(connectorTaskId, props)

  override def createPartitionSearcher(
    connectorTaskId: ConnectorTaskId,
    config:          S3SourceConfig,
    client:          S3Client,
  ): PartitionSearcher =
    new S3PartitionSearcher(
      config.bucketOptions.map(_.sourceBucketAndPrefix),
      config.partitionSearcher,
      connectorTaskId,
      client.listObjectsV2Paginator(_).iterator().asScala,
    )

  override def connectorPrefix: String = CONNECTOR_PREFIX
}
