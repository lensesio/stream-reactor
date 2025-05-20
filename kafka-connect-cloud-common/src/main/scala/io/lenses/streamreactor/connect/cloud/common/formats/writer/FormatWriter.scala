/*
 * Copyright 2017-2025 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.cloud.common.config.FormatOptions.WithHeaders
import io.lenses.streamreactor.connect.cloud.common.config._
import io.lenses.streamreactor.connect.cloud.common.formats.FormatWriterException
import io.lenses.streamreactor.connect.cloud.common.model.location.FileUtils.toBufferedOutputStream
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.stream.BuildLocalOutputStream

import java.nio.file.Path
import scala.util.Try

object FormatWriter {

  def apply(
    formatSelection: FormatSelection,
    path:            Path,
    topicPartition:  TopicPartition,
  )(
    implicit
    compressionCodec: CompressionCodec,
  ): Either[SinkError, FormatWriter] = {
    for {
      outputStream <- Try(new BuildLocalOutputStream(toBufferedOutputStream(path.toFile), topicPartition))
      writer <- Try {
        formatSelection match {
          case ParquetFormatSelection =>
            new ParquetFormatWriter(outputStream)
          case JsonFormatSelection    => new JsonFormatWriter(outputStream)
          case AvroFormatSelection    => new AvroFormatWriter(outputStream)
          case TextFormatSelection(_) => new TextFormatWriter(outputStream)
          case CsvFormatSelection(formatOptions) =>
            new CsvFormatWriter(outputStream, formatOptions.contains(WithHeaders))
          case BytesFormatSelection => new BytesFormatWriter(outputStream)
          case _                    => throw FormatWriterException(s"Unsupported cloud format $formatSelection.format")
        }
      }
    } yield writer
  }.toEither.leftMap(ex => new NonFatalCloudSinkError(ex.getMessage, ex.some))

}

trait FormatWriter extends AutoCloseable {

  /**
   * Determines if the file should be rolled over when a schema change is detected.
   *
   * @return True if the file should be rolled over on schema change, false otherwise.
   */
  def rolloverFileOnSchemaChange(): Boolean

  def write(message: MessageDetail): Either[Throwable, Unit]

  def getPointer: Long

  def complete(): Either[SinkError, Unit]

  def close(): Unit = { val _ = complete() }

}
