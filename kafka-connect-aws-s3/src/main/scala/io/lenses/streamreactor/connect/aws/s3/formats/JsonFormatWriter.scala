
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
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.sink.conversion.ToJsonDataConverter
import io.lenses.streamreactor.connect.aws.s3.storage.S3OutputStream
import org.apache.kafka.connect.json.JsonConverter

import java.nio.charset.StandardCharsets
import scala.collection.JavaConverters._
import scala.util.Try

class JsonFormatWriter(outputStreamFn: () => S3OutputStream) extends S3FormatWriter {

  private val LineSeparatorBytes: Array[Byte] = System.lineSeparator.getBytes(StandardCharsets.UTF_8)

  private val outputStream: S3OutputStream = outputStreamFn()
  private val jsonConverter = new JsonConverter

  jsonConverter.configure(
    Map("schemas.enable" -> false).asJava, false
  )

  override def write(keySinkData: Option[SinkData], valueSinkData: SinkData, topic: Topic): Either[Throwable, Unit] = {

    Try {

      val dataBytes = valueSinkData match {
        case data: PrimitiveSinkData => jsonConverter.fromConnectData(topic.value, valueSinkData.schema().orNull, data.primVal())
        case StructSinkData(structVal) => jsonConverter.fromConnectData(topic.value, valueSinkData.schema().orNull, structVal)
        case MapSinkData(map, schema) =>
          jsonConverter.fromConnectData(topic.value, schema.orNull, ToJsonDataConverter.convertMap(map))
        case ArraySinkData(array, schema) => jsonConverter.fromConnectData(topic.value, schema.orNull, ToJsonDataConverter.convertArray(array))
        case ByteArraySinkData(_, _) => throw new IllegalStateException("Cannot currently write byte array as json")
        case NullSinkData(schema) => jsonConverter.fromConnectData(topic.value, schema.orNull, null)
      }

      outputStream.write(dataBytes)
      outputStream.write(LineSeparatorBytes)
      outputStream.flush()
    }.toEither
  }

  override def rolloverFileOnSchemaChange(): Boolean = false

  override def close(newName: RemoteS3PathLocation, offset: Offset, updateOffsetFn: () => Unit): Unit = {
    Try(outputStream.complete(newName, offset))

    Try(outputStream.flush())
    Try(outputStream.close())
  }

  override def getPointer: Long = outputStream.getPointer

  override def close(): Unit = Try(outputStream.close())

}
