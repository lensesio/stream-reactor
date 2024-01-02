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
package io.lenses.streamreactor.connect.cloud.common.formats.writer

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.formats.writer.parquet.ParquetOutputFile
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.BROTLI
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.GZIP
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.LZ4
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.LZO
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.SNAPPY
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.ZSTD
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.ToAvroDataConverter
import io.lenses.streamreactor.connect.cloud.common.stream.CloudOutputStream
import org.apache.avro.Schema
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE
import org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE
import org.apache.parquet.hadoop.metadata.{ CompressionCodecName => ParquetCompressionCodecName }

import scala.util.Try

class ParquetFormatWriter(outputStream: CloudOutputStream)(implicit compressionCodec: CompressionCodec)
    extends FormatWriter
    with LazyLogging {

  private val parquetCompressionCodec: ParquetCompressionCodecName = {
    compressionCodec.compressionCodec match {
      case UNCOMPRESSED => ParquetCompressionCodecName.UNCOMPRESSED
      case SNAPPY       => ParquetCompressionCodecName.SNAPPY
      case GZIP         => ParquetCompressionCodecName.GZIP
      case LZO          => ParquetCompressionCodecName.LZO
      case BROTLI       => ParquetCompressionCodecName.BROTLI
      case LZ4          => ParquetCompressionCodecName.LZ4
      case ZSTD         => ParquetCompressionCodecName.ZSTD
      case _            => throw new IllegalArgumentException("No or invalid compressionCodec specified")
    }
  }

  private var writer: ParquetWriter[Any] = _

  override def write(messageDetail: MessageDetail): Either[Throwable, Unit] =
    Try {

      logger.debug("ParquetFormatWriter - write")

      val genericRecord = ToAvroDataConverter.convertToGenericRecord(messageDetail.value)
      if (writer == null) {
        writer = init(messageDetail.value.schema())
      }

      writer.write(genericRecord)
      outputStream.flush()
    }.toEither

  private def init(connectSchema: Option[ConnectSchema]): ParquetWriter[Any] = {
    val schema: Schema = ToAvroDataConverter.convertSchema(connectSchema)

    val outputFile = new ParquetOutputFile(outputStream)

    AvroParquetWriter
      .builder[Any](outputFile)
      .withRowGroupSize(DEFAULT_BLOCK_SIZE.toLong)
      .withPageSize(DEFAULT_PAGE_SIZE)
      .withSchema(schema)
      .withCompressionCodec(parquetCompressionCodec)
      .build()

  }

  override def rolloverFileOnSchemaChange() = true

  override def complete(): Either[SinkError, Unit] =
    for {
      _      <- Suppress(writer.close())
      _      <- Suppress(outputStream.flush())
      closed <- outputStream.complete()
    } yield closed

  override def getPointer: Long = writer.getDataSize

}
