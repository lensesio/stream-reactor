
/*
 * Copyright 2020 Lenses.io
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

import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.storage.S3OutputStream

import java.nio.charset.StandardCharsets
import scala.util.{Failure, Success, Try}

class TextFormatWriter(outputStreamFn: () => S3OutputStream) extends S3FormatWriter {

  private val LineSeparatorBytes: Array[Byte] = System.lineSeparator.getBytes(StandardCharsets.UTF_8)

  private val outputStream: S3OutputStream = outputStreamFn()

  override def write(keySinkData: Option[SinkData], valueSinkData: SinkData, topic: Topic): Either[Throwable, Unit] = {

    Try {

      val dataBytes: Array[Byte] = Try {
        valueSinkData match {
          case data: PrimitiveSinkData => data.primVal().toString.getBytes
          case _ => throw FormatWriterException("Not a string")
        }
      } match {
        case Failure(exception: Throwable) => throw FormatWriterException("Unable to retrieve text field value.  Text format is only for output of kafka string values.", Some(exception))
        case Success(value) => value
      }

      outputStream.write(dataBytes)
      outputStream.write(LineSeparatorBytes)
      outputStream.flush()
    }.toEither
  }

  override def rolloverFileOnSchemaChange(): Boolean = false

  override def close(newName: RemotePathLocation, offset: Offset, updateOffsetFn: () => Unit): Unit = {
    Try(outputStream.complete(newName, offset))

    Try(outputStream.flush())
    Try(outputStream.close())
  }

  override def getPointer: Long = outputStream.getPointer

  override def close(): Unit = Try(outputStream.close())

}
