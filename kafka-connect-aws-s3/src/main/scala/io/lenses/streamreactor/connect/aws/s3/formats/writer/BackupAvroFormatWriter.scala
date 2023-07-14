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
package io.lenses.streamreactor.connect.aws.s3.formats.writer

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.CompressionCodecName._
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.SinkError
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.ToAvroDataConverter
import io.lenses.streamreactor.connect.aws.s3.stream.S3OutputStream
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }

import scala.util.Try

/**
  * As a Kafka backup it will store the key, value, headers and timestamp in the Avro file.
  * As a JSON representation it will look like:
  * {{{
  *   {
  *     "key": ...,
  *     "value": ...,
  *     "headers": {
  *       "header1": "value1",
  *       "header2": "value2"
  *     },
  *     metadata: {
  *       "timestamp": 123456789,
  *       "topic": "topic1",
  *       "partition": 0,
  *       "offset": 1
  *
  *     }
  *   }
  * }}}
  * @param outputStream - the output stream to write to
  * @param compressionCodec - the compression codec to use
  */
class BackupAvroFormatWriter(outputStream: S3OutputStream)(implicit compressionCodec: CompressionCodec)
    extends S3FormatWriter
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

  private var avroWriterState: Option[BackupAvroWriterState] = None

  override def rolloverFileOnSchemaChange() = true

  override def write(messageDetail: MessageDetail): Either[Throwable, Unit] =
    Try {
      val writerState = avroWriterState match {
        case Some(state) => state
        case None =>
          val state = new BackupAvroWriterState(outputStream, messageDetail.valueSinkData.schema())
          avroWriterState = Some(state)
          state
      }
      writerState.write(messageDetail.valueSinkData)
    }.toEither

  override def complete(): Either[SinkError, Unit] =
    avroWriterState.fold {
      logger.debug("Requesting close (with args) when there's nothing to close")
      ().asRight[SinkError]
    }(_.close())

  override def getPointer: Long = avroWriterState.fold(0L)(_.pointer)

  private class BackupAvroWriterState(outputStream: S3OutputStream, connectSchema: Option[ConnectSchema]) {
    private val schema: Schema                     = ToAvroDataConverter.convertSchema(connectSchema)
    private val writer: GenericDatumWriter[AnyRef] = new GenericDatumWriter[AnyRef](schema)
    private val fileWriter: DataFileWriter[AnyRef] =
      new DataFileWriter[AnyRef](writer).setCodec(avroCompressionCodec).create(schema, outputStream)

    def write(valueStruct: SinkData): Unit = {

      val genericRecord: AnyRef = ToAvroDataConverter.convertToGenericRecord(valueStruct)

      fileWriter.append(genericRecord)
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
