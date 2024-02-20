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
package io.lenses.streamreactor.connect.cloud.common.model

import cats.implicits.catsSyntaxOptionId
import enumeratum.Enum
import enumeratum.EnumEntry

sealed trait CompressionCodecName extends EnumEntry {
  def withLevel(level: Int): CompressionCodec =
    CompressionCodec(this, level.some, CompressionCodecName.toFileExtension(this))

  def toCodec(): CompressionCodec = CompressionCodec(
    compressionCodec = this,
    extension        = CompressionCodecName.toFileExtension(this),
  )
}

object CompressionCodecName extends Enum[CompressionCodecName] {

  case object UNCOMPRESSED extends CompressionCodecName
  case object SNAPPY       extends CompressionCodecName
  case object GZIP         extends CompressionCodecName
  case object LZO          extends CompressionCodecName
  case object BROTLI       extends CompressionCodecName
  case object LZ4          extends CompressionCodecName
  case object BZIP2        extends CompressionCodecName
  case object ZSTD         extends CompressionCodecName
  case object DEFLATE      extends CompressionCodecName
  case object XZ           extends CompressionCodecName

  override def values: IndexedSeq[CompressionCodecName] = findValues

  def toFileExtension(codecName: CompressionCodecName): Option[String] = codecName match {
    case CompressionCodecName.UNCOMPRESSED => None
    case CompressionCodecName.SNAPPY       => Some("sz")
    case CompressionCodecName.GZIP         => Some("gz")
    case CompressionCodecName.LZO          => Some("lzo")
    case CompressionCodecName.BROTLI       => Some("br")
    case CompressionCodecName.LZ4          => Some("lz4")
    case CompressionCodecName.BZIP2        => Some("bz2")
    case CompressionCodecName.ZSTD         => Some("zst")
    case CompressionCodecName.DEFLATE      => Some("gz")
    case CompressionCodecName.XZ           => Some("xz")
  }
}

/**
  * @param extension
  *   Some format selections have compression built-in, such as Avro and Parquet.
  *   Text formats like CSV and JSON do not, and require an update to the file extension when compressed.
  */
case class CompressionCodec(
  compressionCodec: CompressionCodecName,
  level:            Option[Int]    = Option.empty,
  extension:        Option[String] = None,
)
