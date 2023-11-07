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

import LineSeparatorUtil.LineSeparatorBytes
import io.lenses.streamreactor.connect.cloud.common.formats.FormatWriterException
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.stream.CloudOutputStream

import scala.util.Failure
import scala.util.Success
import scala.util.Try

class TextFormatWriter(outputStream: CloudOutputStream) extends FormatWriter {

  override def write(messageDetail: MessageDetail): Either[Throwable, Unit] =
    Try {

      val dataBytes: Array[Byte] = Try {
        messageDetail.value match {
          case data: PrimitiveSinkData => data.safeValue.toString.getBytes
          case _ => throw FormatWriterException("Not a string")
        }
      } match {
        case Failure(exception: Throwable) =>
          throw FormatWriterException(
            "Unable to retrieve text field value.  Text format is only for output of kafka string values.",
            Some(exception),
          )
        case Success(value) => value
      }

      outputStream.write(dataBytes)
      outputStream.write(LineSeparatorBytes)
      outputStream.flush()
    }.toEither

  override def rolloverFileOnSchemaChange(): Boolean = false

  override def complete(): Either[SinkError, Unit] =
    for {
      closed <- outputStream.complete()
      _      <- Suppress(outputStream.flush())
      _      <- Suppress(outputStream.close())
    } yield closed

  override def getPointer: Long = outputStream.getPointer

}
