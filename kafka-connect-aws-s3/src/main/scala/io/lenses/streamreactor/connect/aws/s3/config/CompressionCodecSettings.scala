package io.lenses.streamreactor.connect.aws.s3.config

import com.datamountaineer.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.COMPRESSION_CODEC
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings.COMPRESSION_LEVEL
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodec

trait CompressionCodecSettings extends BaseSettings {

  def getCompressionCodec(): CompressionCodec = {
    val codec =
      CompressionCodecName.withNameInsensitiveOption(getString(COMPRESSION_CODEC)).getOrElse(
        CompressionCodecName.UNCOMPRESSED,
      )
    val level    = getInt(COMPRESSION_LEVEL)
    val levelOpt = Option.when(level != -1)(level.toInt)

    CompressionCodec(codec, levelOpt)
  }
}
