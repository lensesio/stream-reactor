package io.lenses.streamreactor.connect.aws.s3.model

import cats.implicits.catsSyntaxOptionId
import enumeratum.EnumEntry
import enumeratum.Enum

sealed trait CompressionCodecName extends EnumEntry {
  def withLevel(level: Int): CompressionCodec = CompressionCodec(this, level.some)

  def toCodec(): CompressionCodec = CompressionCodec(this)
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
}

case class CompressionCodec(compressionCodec: CompressionCodecName, level: Option[Int] = Option.empty)
