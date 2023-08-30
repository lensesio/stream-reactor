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

import io.lenses.streamreactor.connect.aws.s3.formats.writer.JsonFormatWriter._
import io.lenses.streamreactor.connect.aws.s3.formats.writer.LineSeparatorUtil.LineSeparatorBytes
import io.lenses.streamreactor.connect.aws.s3.sink._
import io.lenses.streamreactor.connect.aws.s3.stream.S3OutputStream
import org.apache.kafka.connect.json.JsonConverter

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.util.Try
class JsonFormatWriter(outputStream: S3OutputStream) extends S3FormatWriter {

  override def write(messageDetail: MessageDetail): Either[Throwable, Unit] = {
    val topic         = messageDetail.topic
    val valueSinkData = messageDetail.value
    Try {

      val dataBytes = messageDetail.value match {
        case data: PrimitiveSinkData =>
          Converter.fromConnectData(topic.value, valueSinkData.schema().orNull, data.safeValue)
        case StructSinkData(structVal) =>
          Converter.fromConnectData(topic.value, valueSinkData.schema().orNull, structVal)
        case MapSinkData(map, schema) =>
          Converter.fromConnectData(topic.value, schema.orNull, map)
        case ArraySinkData(array, schema) =>
          Converter.fromConnectData(topic.value, schema.orNull, array)
        case ByteArraySinkData(_, _) => throw new IllegalStateException("Cannot currently write byte array as json")
        case NullSinkData(schema)    => Converter.fromConnectData(topic.value, schema.orNull, null)
      }

      outputStream.write(dataBytes)
      outputStream.write(LineSeparatorBytes)
      outputStream.flush()
    }.toEither
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

object JsonFormatWriter {

  private val Converter = new JsonConverter()

  Converter.configure(
    Map("schemas.enable" -> false).asJava,
    false,
  )
}
