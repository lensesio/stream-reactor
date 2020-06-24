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

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model._
import org.apache.kafka.connect.data.Schema.Type._
import org.apache.kafka.connect.data.Struct

object FieldValueToStringConverter extends LazyLogging {

  /**
    * Returns the value of a struct as a String for text output
    */
  def lookupFieldValueFromSinkData(sinkData: SinkData)(fieldNameOpt: Option[String]): Option[String] = {
    sinkData match {
      case data: PrimitiveSinkData => Some(data.primVal().toString)
      case ByteArraySinkData(array, _) => Some(new String(array))
      case other => fieldNameOpt.fold(throw new IllegalArgumentException("FieldName not specified"))(fieldName =>
        other match {
          case StructSinkData(structVal) => lookupFieldValueFromStruct(structVal, fieldName)
          case MapSinkData(map, _) => lookupFieldValueFromMap(map, fieldName)
          case ArraySinkData(array, schema) => throw new IllegalArgumentException("Cannot retrieve a named field from an Array")
          case _ => throw new IllegalArgumentException("Unknown type")
        }
      )
    }
  }

  def lookupFieldValueFromMap(map: Map[SinkData, SinkData], fieldName: String): Option[String] = {
    map.get(StringSinkData(fieldName, None)).fold(throw new IllegalArgumentException("Cannot field from specified map"))(
      e => e match {
        case data: PrimitiveSinkData => Some(data.primVal().toString)
        case ByteArraySinkData(array, _) => Some(new String(array.array))
        case StructSinkData(_) => throw new IllegalArgumentException("Unable to represent a struct as a string value")
        case MapSinkData(_, _) => throw new IllegalArgumentException("Unable to represent a map as a string value")
        case ArraySinkData(_, _) => throw new IllegalArgumentException("Unable to represent an array as a string value")
        case other => throw new IllegalArgumentException(s"Unable to represent a complex object as a string value ${other.getClass.getCanonicalName}")
      }
    )
  }

  def lookupFieldValueFromStruct(struct: Struct, fieldName: String) = {

    Option(struct.schema().field(fieldName))
      .fold(Option.empty[String]) {
        _.schema().`type`() match {
          case INT8 => convertToOption(struct.getInt8(fieldName))
          case INT16 => convertToOption(struct.getInt16(fieldName))
          case INT32 => convertToOption(struct.getInt32(fieldName))
          case INT64 => convertToOption(struct.getInt64(fieldName))
          case FLOAT32 => convertToOption(struct.getFloat32(fieldName))
          case FLOAT64 => convertToOption(struct.getFloat64(fieldName))
          case BOOLEAN => convertToOption(struct.getBoolean(fieldName))
          case STRING => convertToOption(struct.getString(fieldName))
          case BYTES => Option(struct.getBytes(fieldName)).fold(Option.empty[String])(byteVal => Some(new String(byteVal)))
          case other => logger.error("Non-primitive values not supported: " + other);
            throw new IllegalArgumentException("Non-primitive values not supported: " + other)
        }

      }
  }

  private def convertToOption(any: Any): Option[String] = Option(any).fold(Option.empty[String])(e => Option(e.toString))

}
