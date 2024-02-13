package io.lenses.streamreactor.connect.cloud.common.config.traits

import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.sink.config.{CloudSinkBucketOptions, OffsetSeekerOptions}

/**
 * The object representing the configuration of a sink or source connector.
 */
trait CloudConfig

trait CloudSinkConfig extends CloudConfig {

  def bucketOptions: Seq[CloudSinkBucketOptions]

  def offsetSeekerOptions: OffsetSeekerOptions

  def compressionCodec: CompressionCodec
}

trait CloudSourceConfig extends CloudConfig



