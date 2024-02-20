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
package io.lenses.streamreactor.connect.cloud.common.config

import io.lenses.streamreactor.common.config.base.traits.BaseSettings
import io.lenses.streamreactor.common.config.base.traits.WithConnectorPrefix
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED

trait CompressionCodecConfigKeys extends WithConnectorPrefix {

  def COMPRESSION_CODEC = s"$connectorPrefix.compression.codec"

  val COMPRESSION_CODEC_DOC = "Compression codec to use for Avro, Parquet or JSON."
  val COMPRESSION_CODEC_DEFAULT: String = UNCOMPRESSED.entryName

  def COMPRESSION_LEVEL = s"$connectorPrefix.compression.level"

  val COMPRESSION_LEVEL_DOC = "Certain compression codecs require a level specified."
  val COMPRESSION_LEVEL_DEFAULT: Int = -1

}
trait CompressionCodecSettings extends BaseSettings with CompressionCodecConfigKeys {

  def getCompressionCodec(): CompressionCodec = {
    val codec =
      CompressionCodecName.withNameInsensitiveOption(getString(COMPRESSION_CODEC)).getOrElse(
        CompressionCodecName.UNCOMPRESSED,
      )

    val level    = getInt(COMPRESSION_LEVEL)
    val levelOpt = Option.when(level != -1)(level.toInt)

    CompressionCodec(codec, levelOpt, CompressionCodecName.toFileExtension(codec))
  }
}
