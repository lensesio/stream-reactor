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
package io.lenses.streamreactor.connect.aws.s3.sink.conversion

import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException

import java.nio.ByteBuffer
import java.util
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

object ValueToSinkDataConverter {

  def apply(value: Any, schema: Option[Schema]): SinkData = value match {
    case shortVal:  Short          => ShortSinkData(shortVal, schema)
    case boolVal:   Boolean        => BooleanSinkData(boolVal, schema)
    case stringVal: String         => StringSinkData(stringVal, schema)
    case longVal:   Long           => LongSinkData(longVal, schema)
    case intVal:    Int            => IntSinkData(intVal, schema)
    case byteVal:   Byte           => ByteSinkData(byteVal, schema)
    case doubleVal: Double         => DoubleSinkData(doubleVal, schema)
    case floatVal:  Float          => FloatSinkData(floatVal, schema)
    case structVal: Struct         => StructSinkData(structVal)
    case mapVal:    Map[_, _]      => MapSinkData(mapVal.asJava, schema)
    case mapVal:    util.Map[_, _] => MapSinkData(mapVal, schema)
    case bytesVal:  Array[Byte]    => ByteArraySinkData(bytesVal, schema)
    case bytesVal:  ByteBuffer     => ByteArraySinkData(bytesVal.array(), schema)
    case arrayVal:  Array[_]       => ArraySinkData(arrayVal.toList.asJava, schema)
    case listVal:   util.List[_]   => ArraySinkData(listVal, schema)
    case null     => NullSinkData(schema)
    case otherVal => throw new ConnectException(s"Unsupported record $otherVal:${otherVal.getClass.getCanonicalName}")
  }
}
