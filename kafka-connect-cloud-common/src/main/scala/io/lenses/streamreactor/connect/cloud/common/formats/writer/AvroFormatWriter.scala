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
package io.lenses.streamreactor.connect.cloud.common.formats.writer

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.BZIP2
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.DEFLATE
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.SNAPPY
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.XZ
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.ZSTD
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.ToAvroDataConverter
import io.lenses.streamreactor.connect.cloud.common.stream.CloudOutputStream
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }

import scala.util.Try

class AvroFormatWriter(outputStream: CloudOutputStream)(implicit compressionCodec: CompressionCodec)
    extends FormatWriter
    with LazyLogging {

  private val avroCompressionCodec: CodecFactory = {
    compressionCodec match {
      case CompressionCodec(UNCOMPRESSED, _)      => CodecFactory.nullCodec()
      case CompressionCodec(SNAPPY, _)            => CodecFactory.snappyCodec()
      case CompressionCodec(BZIP2, _)             => CodecFactory.bzip2Codec()
      case CompressionCodec(ZSTD, Some(level))    => CodecFactory.zstandardCodec(level)
      case CompressionCodec(DEFLATE, Some(level)) => CodecFactory.deflateCodec(level)
      case CompressionCodec(XZ, Some(level))      => CodecFactory.xzCodec(level)
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
    }.toEither

  override def complete(): Either[SinkError, Unit] =
    avroWriterState.fold {
      logger.debug("Requesting close (with args) when there's nothing to close")
      ().asRight[SinkError]
    }(_.close())

  override def getPointer: Long = avroWriterState.fold(0L)(_.pointer)

  private class AvroWriterState(outputStream: CloudOutputStream, connectSchema: Option[ConnectSchema]) {
    private val schema: Schema                  = ToAvroDataConverter.convertSchema(connectSchema)
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
