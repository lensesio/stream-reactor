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

package io.lenses.streamreactor.connect.aws.s3.sink.conversion

import java.util

import io.lenses.streamreactor.connect.aws.s3.model._
import org.apache.kafka.connect.data.{Schema, Struct}

import scala.collection.JavaConverters._

object ValueToSinkDataConverter {

  def apply(value: Any, schema: Option[Schema]): SinkData = value match {
    case boolVal: Boolean => BooleanSinkData(boolVal, schema)
    case stringVal: String => StringSinkData(stringVal, schema)
    case longVal: Long => LongSinkData(longVal, schema)
    case intVal: Int => IntSinkData(intVal, schema)
    case byteVal: Byte => ByteSinkData(byteVal, schema)
    case doubleVal: Double => DoubleSinkData(doubleVal, schema)
    case floatVal: Float => FloatSinkData(floatVal, schema)
    case structVal: Struct => StructSinkData(structVal)
    case mapVal: Map[_, _] => MapSinkDataConverter(mapVal, schema)
    case mapVal: util.Map[_, _] => MapSinkDataConverter(mapVal.asScala.toMap, schema)
    case bytesVal: Array[Byte] => ByteArraySinkData(bytesVal, schema)
    case arrayVal: Array[_] => ArraySinkDataConverter(arrayVal, schema)
    case listVal: util.List[_] => ArraySinkDataConverter(listVal.toArray, schema)
    case null => NullSinkData(schema)
    case otherVal => sys.error(s"Unsupported record $otherVal:${otherVal.getClass.getCanonicalName}")
  }
}

object ArraySinkDataConverter {
  def apply(array: Array[_], schema: Option[Schema]): SinkData = {
    ArraySinkData(array.map(e => ValueToSinkDataConverter(e, None)), schema)
  }
}

object MapSinkDataConverter {
  def apply(map: Map[_, _], schema: Option[Schema]): SinkData = {
    MapSinkData(map.map {
      case (k: String, v) => StringSinkData(k, None) -> ValueToSinkDataConverter(v, None)
      case (k, _) => sys.error(s"Non-string map values including (${k.getClass.getCanonicalName}) are not currently supported")
    }, schema)
  }
}
