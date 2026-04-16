/*
 * Copyright 2017-2026 Lenses.io Ltd
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

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName._
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.ToAvroDataConverter
import io.lenses.streamreactor.connect.cloud.common.stream.CloudOutputStream
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }

import scala.util.Try

class AvroFormatWriter(
  outputStream: CloudOutputStream,
)(
  implicit
  compressionCodec: CompressionCodec,
) extends FormatWriter
    with LazyLogging {

  private val avroCompressionCodec: CodecFactory = {
    compressionCodec match {
      case CompressionCodec(UNCOMPRESSED, _, _)      => CodecFactory.nullCodec()
      case CompressionCodec(SNAPPY, _, _)            => CodecFactory.snappyCodec()
      case CompressionCodec(BZIP2, _, _)             => CodecFactory.bzip2Codec()
      case CompressionCodec(ZSTD, Some(level), _)    => CodecFactory.zstandardCodec(level)
      case CompressionCodec(DEFLATE, Some(level), _) => CodecFactory.deflateCodec(level)
      case CompressionCodec(XZ, Some(level), _)      => CodecFactory.xzCodec(level)
      case _ =>
        throw new IllegalArgumentException("No or invalid compressionCodec specified - does codec require a level?")
    }
  }

  private var avroWriterState: Option[AvroWriterState] = None

  override def rolloverFileOnSchemaChange() = true

  override def write(message: MessageDetail): Either[Throwable, Unit] =
    Try {
      val writerState = avroWriterState match {
        case Some(state) => state
        case None =>
          val state = new AvroWriterState(outputStream, message.value.schema())
          avroWriterState = Some(state)
          state
      }
      writerState.write(message.value)
    }.toEither.leftMap { c =>
      logger.error(
        s"""Failed to write message to Avro format.
            Message: ${message.value}
            Avro File Schema:${this.avroWriterState.map(
          _.schema.toString,
        ).getOrElse("<N/A>")}
            Message Avro Schema: ${message.value.schema().map(ToAvroDataConverter.convertSchema).getOrElse("<N/A>")}
            State Connect Schema version: ${this.avroWriterState.flatMap(_.connectSchema).map(_.version()).getOrElse(
          "<N/A>",
        )}
            Message Connect Schema version: ${message.value.schema().map(_.version()).getOrElse("<N/A>")}
        """,
        c,
      )
      c
    }

  override def complete(): Either[SinkError, Unit] =
    avroWriterState.fold {
      logger.debug("Requesting close (with args) when there's nothing to close")
      ().asRight[SinkError]
    }(_.close())

  override def getPointer: Long = avroWriterState.fold(0L)(_.pointer)

  private class AvroWriterState(outputStream: CloudOutputStream, val connectSchema: Option[ConnectSchema]) {
    val schema: Schema = connectSchema.map(ToAvroDataConverter.convertSchema).getOrElse(
      throw new IllegalArgumentException("Schema-less data is not supported for Avro/Parquet"),
    )
    private val writer: GenericDatumWriter[Any] = new GenericDatumWriter[Any](schema)
    private val fileWriter: DataFileWriter[Any] =
      new DataFileWriter[Any](writer).setCodec(avroCompressionCodec).create(schema, outputStream)

    def write(valueStruct: SinkData): Unit = {
      val record: Any = ToAvroDataConverter.convertToGenericRecord(valueStruct)
      fileWriter.append(record)
      fileWriter.flush()
    }

    def close(): Either[SinkError, Unit] =
      for {
        _      <- Suppress(fileWriter.flush())
        closed <- outputStream.complete()
        _      <- Suppress(fileWriter.close())
      } yield closed

    def pointer: Long = outputStream.getPointer

  }
}
