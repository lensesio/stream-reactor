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
import io.lenses.streamreactor.connect.aws.s3.formats.reader.converters.BytesOutputRowConverter
import io.lenses.streamreactor.connect.aws.s3.formats.reader.converters.SchemaAndValueConverter
import io.lenses.streamreactor.connect.aws.s3.formats.reader.converters.TextConverter
import io.lenses.streamreactor.connect.aws.s3.formats.writer.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName._
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.config.ReadTextMode
import io.lenses.streamreactor.connect.aws.s3.source.config.kcqlprops.S3PropsSchema

import java.io.InputStream
import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsScala

case class ObjectMetadata(size: Long, lastModified: Instant)

case class ReaderBuilderContext(
  stream:               InputStream,
  bucketAndPath:        S3Location,
  metadata:             ObjectMetadata,
  hasEnvelope:          Boolean,
  recreateInputStreamF: () => Either[Throwable, InputStream],
  targetPartition:      Integer,
  targetTopic:          Topic,
  watermarkPartition:   java.util.Map[String, String],
)

sealed trait FormatSelection {
  def toStreamReader(input: ReaderBuilderContext): S3StreamReader

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
    input: ReaderBuilderContext,
  ): S3StreamReader = {
    val inner = new TextStreamReader(input.stream)
    val converter = new TextConverter(input.watermarkPartition,
                                      input.targetTopic,
                                      input.targetPartition,
                                      input.bucketAndPath,
                                      input.metadata.lastModified,
    )
    new DelegateIteratorS3StreamReader[String](inner, converter, input.bucketAndPath)
  }

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
    input: ReaderBuilderContext,
  ): S3StreamReader = {

    val inner = new AvroStreamReader(input.stream)
    val converter = new SchemaAndValueConverter(input.watermarkPartition,
                                                input.targetTopic,
                                                input.targetPartition,
                                                input.bucketAndPath,
                                                input.metadata.lastModified,
    )
    new DelegateIteratorS3StreamReader(inner, converter, input.bucketAndPath)

  }

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
    input: ReaderBuilderContext,
  ): S3StreamReader = {
    val inner = ParquetStreamReader.apply(input.stream, input.metadata.size, input.recreateInputStreamF)
    val converter = new SchemaAndValueConverter(input.watermarkPartition,
                                                input.targetTopic,
                                                input.targetPartition,
                                                input.bucketAndPath,
                                                input.metadata.lastModified,
    )
    new DelegateIteratorS3StreamReader(inner, converter, input.bucketAndPath)
  }

  override def extension: String = "parquet"

  override def supportsEnvelope: Boolean = true
}
case class TextFormatSelection(readTextMode: Option[ReadTextMode]) extends FormatSelection {
  override def toStreamReader(
    input: ReaderBuilderContext,
  ): S3StreamReader = {
    val inner = TextStreamReader(
      readTextMode,
      input.stream,
    )
    val converter = new TextConverter(input.watermarkPartition,
                                      input.targetTopic,
                                      input.targetPartition,
                                      input.bucketAndPath,
                                      input.metadata.lastModified,
    )
    new DelegateIteratorS3StreamReader(
      inner,
      converter,
      input.bucketAndPath,
    )
  }

  override def extension: String = "text"

  override def supportsEnvelope: Boolean = false
}
case class CsvFormatSelection(formatOptions: Set[FormatOptions]) extends FormatSelection {
  override def toStreamReader(
    input: ReaderBuilderContext,
  ): S3StreamReader = {
    val inner = new CsvStreamReader(input.stream, hasHeaders = formatOptions.contains(WithHeaders))
    val converter = new TextConverter(input.watermarkPartition,
                                      input.targetTopic,
                                      input.targetPartition,
                                      input.bucketAndPath,
                                      input.metadata.lastModified,
    )
    new DelegateIteratorS3StreamReader(
      inner,
      converter,
      input.bucketAndPath,
    )
  }

  override def extension: String = "csv"

  override def supportsEnvelope: Boolean = false
}
case class BytesFormatSelection(formatOptions: Set[FormatOptions]) extends FormatSelection {
  override def toStreamReader(
    input: ReaderBuilderContext,
  ): S3StreamReader = {

    val bytesWriteMode = S3FormatWriter.convertToBytesWriteMode(formatOptions)
    val inner = if (bytesWriteMode.entryName.toLowerCase.contains("size")) {
      new BytesWithSizesStreamReader(input.stream, input.metadata.size, bytesWriteMode)
    } else {
      new BytesStreamFileReader(input.stream, input.metadata.size, bytesWriteMode)
    }
    val converter = new BytesOutputRowConverter(input.watermarkPartition,
                                                input.targetTopic,
                                                input.targetPartition,
                                                input.bucketAndPath,
                                                input.metadata.lastModified,
    )
    new DelegateIteratorS3StreamReader(
      inner,
      converter,
      input.bucketAndPath,
    )
  }

  override def extension: String = "bytes"

  override def supportsEnvelope: Boolean = false
}
