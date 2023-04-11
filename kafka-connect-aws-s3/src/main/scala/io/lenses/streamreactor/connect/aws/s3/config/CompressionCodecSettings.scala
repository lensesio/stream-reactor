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
