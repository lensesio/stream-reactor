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

import com.datamountaineer.kcql.Kcql
import io.lenses.streamreactor.connect.aws.s3.config.FormatOptions.WithHeaders
import io.lenses.streamreactor.connect.aws.s3.formats.reader._
import io.lenses.streamreactor.connect.aws.s3.formats.writer.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName._
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.config.ReadTextMode
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3PropsSchema

import java.io.InputStream
import scala.jdk.CollectionConverters.MapHasAsScala

sealed trait FormatSelection {
  def toStreamReader(
    inputStream:          InputStream,
    fileSize:             Long,
    bucketAndPath:        S3Location,
    recreateInputStreamF: () => Either[Throwable, InputStream],
  ): S3FormatStreamReader[_ <: SourceData]

  def availableCompressionCodecs: Map[CompressionCodecName, Boolean] = Map(UNCOMPRESSED -> false)

  def extension: String

  def supportsEnvelope: Boolean
}
case object FormatSelection {

  private val schema = S3PropsSchema.schema

  def fromKcql(
    kcql: Kcql,
  ): Either[Throwable, FormatSelection] =
    Option(kcql.getStoredAs) match {
      case Some(storedAs) =>
        fromString(storedAs, () => ReadTextMode(schema.readProps(kcql.getProperties.asScala.toMap)))
      case None =>
        Right(JsonFormatSelection)
    }

  def fromString(
    formatAsString: String,
    readTextMode:   () => Option[ReadTextMode],
  ): Either[Throwable, FormatSelection] = {
    val withoutTicks = formatAsString.replace("`", "")
    val split        = withoutTicks.split("_")

    val formatOptions: Set[FormatOptions] = if (split.size > 1) {
      split.splitAt(1)._2.flatMap(FormatOptions.withNameInsensitiveOption).toSet
    } else {
      Set.empty
    }

    Format.withNameInsensitiveOption(split(0)).map {
      case Format.Json    => JsonFormatSelection
      case Format.Avro    => AvroFormatSelection
      case Format.Parquet => ParquetFormatSelection
      case Format.Text    => TextFormatSelection(readTextMode())
      case Format.Csv     => CsvFormatSelection(formatOptions)
      case Format.Bytes   => BytesFormatSelection(formatOptions)
    }.toRight(new IllegalArgumentException(s"Unsupported format - $formatAsString"))
  }
}

case object JsonFormatSelection extends FormatSelection {
  override def toStreamReader(
    inputStream:          InputStream,
    fileSize:             Long,
    bucketAndPath:        S3Location,
    recreateInputStreamF: () => Either[Throwable, InputStream],
  ) =
    new TextFormatStreamReader(inputStream, bucketAndPath)

  override def extension: String = "json"

  override def supportsEnvelope: Boolean = true
}

case object AvroFormatSelection extends FormatSelection {
  override def availableCompressionCodecs: Map[CompressionCodecName, Boolean] = Map(
    UNCOMPRESSED -> false,
    DEFLATE      -> true,
    BZIP2        -> false,
    SNAPPY       -> false,
    XZ           -> true,
    ZSTD         -> true,
  )

  override def toStreamReader(
    inputStream:          InputStream,
    fileSize:             Long,
    bucketAndPath:        S3Location,
    recreateInputStreamF: () => Either[Throwable, InputStream],
  ) =
    new AvroFormatStreamReader(inputStream, bucketAndPath)

  override def extension: String = "avro"

  override def supportsEnvelope: Boolean = true
}

case object ParquetFormatSelection extends FormatSelection {
  override def availableCompressionCodecs: Map[CompressionCodecName, Boolean] = Set(
    UNCOMPRESSED,
    SNAPPY,
    GZIP,
    LZO,
    BROTLI,
    LZ4,
    ZSTD,
  ).map(_ -> false).toMap

  override def toStreamReader(
    inputStream:          InputStream,
    fileSize:             Long,
    bucketAndPath:        S3Location,
    recreateInputStreamF: () => Either[Throwable, InputStream],
  ): ParquetFormatStreamReader =
    ParquetFormatStreamReader.apply(inputStream, fileSize, bucketAndPath, recreateInputStreamF)

  override def extension: String = "parquet"

  override def supportsEnvelope: Boolean = true
}
case class TextFormatSelection(readTextMode: Option[ReadTextMode]) extends FormatSelection {
  override def toStreamReader(
    inputStream:          InputStream,
    fileSize:             Long,
    bucketAndPath:        S3Location,
    recreateInputStreamF: () => Either[Throwable, InputStream],
  ): S3FormatStreamReader[StringSourceData] =
    TextFormatStreamReader(readTextMode, inputStream, bucketAndPath)

  override def extension: String = "text"

  override def supportsEnvelope: Boolean = false
}
case class CsvFormatSelection(formatOptions: Set[FormatOptions]) extends FormatSelection {
  override def toStreamReader(
    inputStream:          InputStream,
    fileSize:             Long,
    bucketAndPath:        S3Location,
    recreateInputStreamF: () => Either[Throwable, InputStream],
  ) = new CsvFormatStreamReader(inputStream, bucketAndPath, hasHeaders = formatOptions.contains(WithHeaders))

  override def extension: String = "csv"

  override def supportsEnvelope: Boolean = false
}
case class BytesFormatSelection(formatOptions: Set[FormatOptions]) extends FormatSelection {
  override def toStreamReader(
    inputStream:          InputStream,
    fileSize:             Long,
    bucketAndPath:        S3Location,
    recreateInputStreamF: () => Either[Throwable, InputStream],
  ): S3FormatStreamReader[ByteArraySourceData] = {

    val bytesWriteMode = S3FormatWriter.convertToBytesWriteMode(formatOptions)
    if (bytesWriteMode.entryName.toLowerCase.contains("size")) {
      new BytesFormatWithSizesStreamReader(inputStream, fileSize, bucketAndPath, bytesWriteMode)
    } else {
      new BytesFormatStreamFileReader(inputStream, fileSize, bucketAndPath, bytesWriteMode)
    }
  }

  override def extension: String = "bytes"

  override def supportsEnvelope: Boolean = false
}
