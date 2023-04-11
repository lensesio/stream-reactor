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
package io.lenses.streamreactor.connect.aws.s3.formats

import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.Format._
import io.lenses.streamreactor.connect.aws.s3.config.FormatOptions.WithHeaders
import io.lenses.streamreactor.connect.aws.s3.config.FormatOptions
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.FileUtils.toBufferedOutputStream
import io.lenses.streamreactor.connect.aws.s3.sink.NonFatalS3SinkError
import io.lenses.streamreactor.connect.aws.s3.sink.SinkError
import io.lenses.streamreactor.connect.aws.s3.stream.BuildLocalOutputStream
import io.lenses.streamreactor.connect.aws.s3.stream.S3OutputStream

import java.io.File
import scala.util.Try

object S3FormatWriter {

  def convertToBytesWriteMode(formatOptions: Set[FormatOptions]): BytesWriteMode = {
    require(formatOptions.size <= 1, "Cannot have more than one format option provided.")
    formatOptions
      .headOption
      .fold[BytesWriteMode](BytesWriteMode.ValueWithSize)(fo => BytesWriteMode.withName(fo.entryName))
  }

  def apply(
    formatSelection: FormatSelection,
    path:            File,
    topicPartition:  TopicPartition,
  )(
    implicit
    compressionCodec: CompressionCodec,
  ): Either[SinkError, S3FormatWriter] = {
    for {
      outputStream <- Try(() => new BuildLocalOutputStream(toBufferedOutputStream(path), topicPartition))
      writer       <- Try(S3FormatWriter(formatSelection, outputStream))
    } yield writer
  }.toEither.leftMap(ex => NonFatalS3SinkError(ex.getMessage, ex))

  def apply(
    formatInfo:     FormatSelection,
    outputStreamFn: () => S3OutputStream,
  )(
    implicit
    compressionCodec: CompressionCodec,
  ): S3FormatWriter =
    formatInfo.format match {
      case Parquet => new ParquetFormatWriter(outputStreamFn)
      case Json    => new JsonFormatWriter(outputStreamFn)
      case Avro    => new AvroFormatWriter(outputStreamFn)
      case Text    => new TextFormatWriter(outputStreamFn)
      case Csv     => new CsvFormatWriter(outputStreamFn, formatInfo.formatOptions.contains(WithHeaders))
      case Bytes   => new BytesFormatWriter(outputStreamFn, convertToBytesWriteMode(formatInfo.formatOptions))
      case _       => throw FormatWriterException(s"Unsupported S3 format $formatInfo.format")
    }

}

trait S3FormatWriter extends AutoCloseable {

  def rolloverFileOnSchemaChange(): Boolean

  def write(keySinkData: Option[SinkData], valueSinkData: SinkData, topic: Topic): Either[Throwable, Unit]

  def getPointer: Long

  def complete(): Either[SinkError, Unit]

  def close(): Unit = { val _ = complete() }
}
