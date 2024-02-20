/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.conversion

import io.lenses.streamreactor.connect.cloud.common.formats.writer.ArraySinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.ByteArraySinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MapSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.PrimitiveSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.SinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import org.apache.kafka.connect.json.JsonConverter

import java.nio.ByteBuffer

object ToJsonDataConverter {

  def convertMessageValueToByteArray(converter: JsonConverter, topic: Topic, data: SinkData): Array[Byte] =
    data match {
      case data: PrimitiveSinkData => converter.fromConnectData(topic.value, data.schema().orNull, data.safeValue)
      case StructSinkData(structVal)    => converter.fromConnectData(topic.value, data.schema().orNull, structVal)
      case MapSinkData(map, schema)     => converter.fromConnectData(topic.value, schema.orNull, map)
      case ArraySinkData(array, schema) => converter.fromConnectData(topic.value, schema.orNull, array)
      case ByteArraySinkData(_, _)      => throw new IllegalStateException("Cannot currently write byte array as json")
      case NullSinkData(schema)         => converter.fromConnectData(topic.value, schema.orNull, null)
      case other                        => throw new IllegalStateException(s"Unknown SinkData type, ${other.getClass.getSimpleName}")
    }

  def convert(data: SinkData): Any = data match {
    case data: PrimitiveSinkData => data.safeValue
    case ByteArraySinkData(bArray, _) => ByteBuffer.wrap(bArray)
    case data                         => data.value
  }
}
