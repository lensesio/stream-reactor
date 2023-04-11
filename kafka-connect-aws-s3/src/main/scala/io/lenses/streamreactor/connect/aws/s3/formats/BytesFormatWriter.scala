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

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.SinkError
import io.lenses.streamreactor.connect.aws.s3.stream.S3OutputStream

class BytesFormatWriter(outputStreamFn: () => S3OutputStream, bytesWriteMode: BytesWriteMode)
    extends S3FormatWriter
    with LazyLogging {

  private val outputStream: S3OutputStream = outputStreamFn()

  override def write(keySinkData: Option[SinkData], valueSinkData: SinkData, topic: Topic): Either[Throwable, Unit] = {

    val writeKeys   = bytesWriteMode.entryName.contains("Key")
    val writeValues = bytesWriteMode.entryName.contains("Value")
    val writeSizes  = bytesWriteMode.entryName.contains("Size")

    var byteOutputRow = BytesOutputRow(
      None,
      None,
      Array.empty,
      Array.empty,
    )

    if (writeKeys) {
      keySinkData.fold(throw FormatWriterException("No key supplied however requested to write key.")) { keyStruct =>
        convertToBytes(keyStruct) match {
          case Left(exception) => return exception.asLeft
          case Right(keyDataBytes) => byteOutputRow = byteOutputRow.copy(
              keySize = if (writeSizes) Some(keyDataBytes.length.longValue()) else None,
              key     = keyDataBytes,
            )
        }
      }
    }

    if (writeValues) {
      convertToBytes(valueSinkData) match {
        case Left(exception) => return exception.asLeft
        case Right(valueDataBytes) => byteOutputRow = byteOutputRow.copy(
            valueSize = if (writeSizes) Some(valueDataBytes.length.longValue()) else None,
            value     = valueDataBytes,
          )
      }

    }

    outputStream.write(byteOutputRow.toByteArray)
    outputStream.flush()
    ().asRight
  }

  def convertToBytes(sinkData: SinkData): Either[Throwable, Array[Byte]] =
    sinkData match {
      case ByteArraySinkData(array, _) => array.asRight
      case v =>
        new IllegalStateException(
          s"Non-binary content received: ${v.getClass.getName} .  Please check your configuration.  It may be advisable to ensure you are using org.apache.kafka.connect.converters.ByteArrayConverter.",
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
