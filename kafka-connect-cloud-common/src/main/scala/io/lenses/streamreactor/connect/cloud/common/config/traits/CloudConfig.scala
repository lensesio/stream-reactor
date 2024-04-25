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
package io.lenses.streamreactor.connect.cloud.common.config.traits

import io.lenses.streamreactor.common.config.base.RetryConfig
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.OffsetSeekerOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceBucketOptions
import io.lenses.streamreactor.connect.cloud.common.source.config.PartitionSearcherOptions
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata

/**
  * Trait representing a generic cloud configuration.
  * This trait serves as a marker trait for cloud-specific configuration implementations.
  */
sealed trait CloudConfig

/**
  * Trait representing configuration for a cloud sink.
  * Extends [[CloudConfig]].
  */
trait CloudSinkConfig[CC] extends CloudConfig {

  /**
    * Retrieves the connection configuration for the cloud sink.
    *
    * @return The connection configuration for the cloud sink.
    */
  def connectionConfig: CC

  /**
    * Retrieves the bucket options for the cloud sink.
    *
    * @return The bucket options for the cloud sink.
    */
  def bucketOptions: Seq[CloudSinkBucketOptions]

  /**
    * Retrieves the offset seeker options for the cloud sink.
    *
    * @return The offset seeker options for the cloud sink.
    */
  def offsetSeekerOptions: OffsetSeekerOptions

  /**
    * Retrieves the compression codec for the cloud sink.
    *
    * @return The compression codec for the cloud sink.
    */
  def compressionCodec: CompressionCodec

  def connectorRetryConfig: RetryConfig

  def errorPolicy: ErrorPolicy
}

/**
  * Trait representing configuration for a cloud source.
  * Extends [[CloudConfig]].
  *
  * @tparam MD The type of file metadata associated with the cloud source.
  */
trait CloudSourceConfig[MD <: FileMetadata] extends CloudConfig {

  /**
    * Retrieves the bucket options for the cloud source.
    *
    * @return The bucket options for the cloud source.
    */
  def bucketOptions: Seq[CloudSourceBucketOptions[MD]]

  /**
    * Retrieves the compression codec for the cloud source.
    *
    * @return The compression codec for the cloud source.
    */
  def compressionCodec: CompressionCodec

  /**
    * Retrieves the partition searcher options for the cloud source.
    *
    * @return The partition searcher options for the cloud source.
    */
  def partitionSearcher: PartitionSearcherOptions
}
