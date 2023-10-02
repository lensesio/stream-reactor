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

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.stream.S3OutputStream

import scala.util.Try

class BytesFormatWriter(outputStream: S3OutputStream) extends S3FormatWriter with LazyLogging {

  private var soiled = false

  override def write(messageDetail: MessageDetail): Either[Throwable, Unit] =
    if (soiled) {
      new IllegalStateException(
        "Output stream already written to, can only write a single record for BYTES type.",
      ).asLeft
    } else {
      soiled = true
      for {
        value <- convertToBytes(messageDetail.value)
        _ <- Try {
          outputStream.write(value)
          outputStream.flush()
        }.toEither
      } yield ()
    }

  private def convertToBytes(sinkData: SinkData): Either[Throwable, Array[Byte]] =
    sinkData match {
      case NullSinkData(_)             => Array.emptyByteArray.asRight
      case ByteArraySinkData(array, _) => array.asRight
      case v =>
        new IllegalStateException(
          s"Non-binary content received: ${v.getClass.getName}. Please check your configuration. It is required the converter to be set to [org.apache.kafka.connect.converters.ByteArrayConverter].",
        ).asLeft
    }

  override def rolloverFileOnSchemaChange(): Boolean = false

  override def complete(): Either[SinkError, Unit] =
    for {
      closed <- outputStream.complete()
      _      <- Suppress(outputStream.flush())
      _      <- Suppress(outputStream.close())
    } yield closed

  override def getPointer: Long = outputStream.getPointer

}
