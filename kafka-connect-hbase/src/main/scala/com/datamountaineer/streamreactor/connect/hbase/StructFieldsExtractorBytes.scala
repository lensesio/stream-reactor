/**
  * Copyright 2016 Datamountaineer.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/

package com.datamountaineer.streamreactor.connect.hbase

import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import org.apache.kafka.connect.data.{Field, Schema, Struct}

import scala.collection.JavaConversions._

trait FieldsValuesExtractor {
  def get(struct: Struct): Seq[(String, Array[Byte])]
}

case class StructFieldsExtractorBytes(includeAllFields: Boolean, fieldsAliasMap: Map[String, String]) extends FieldsValuesExtractor {

  def get(struct: Struct): Seq[(String, Array[Byte])] = {
    val schema = struct.schema()
    val fields: Seq[Field] = if (includeAllFields) {
      schema.fields()
    }
    else {
      schema.fields().filter(f => fieldsAliasMap.contains(f.name()))
    }

    val fieldsAndValues = fields.flatMap(field =>
      getFieldBytes(field, struct).map(bytes => fieldsAliasMap.getOrElse(field.name(), field.name()) -> bytes))

    fieldsAndValues
  }

  private def getFieldBytes(field: Field, struct: Struct): Option[Array[Byte]] = {
    Option(struct.get(field)) match {
      case None => None
      case Some(value) =>
        val bytes = field.schema() match {
          case Schema.BOOLEAN_SCHEMA | Schema.OPTIONAL_BOOLEAN_SCHEMA => value.fromBoolean()
          case Schema.BYTES_SCHEMA | Schema.OPTIONAL_BYTES_SCHEMA => value.fromBytes()
          case Schema.FLOAT32_SCHEMA | Schema.OPTIONAL_FLOAT32_SCHEMA => value.fromFloat()
          case Schema.FLOAT64_SCHEMA | Schema.OPTIONAL_FLOAT64_SCHEMA => value.fromDouble()
          case Schema.INT8_SCHEMA | Schema.OPTIONAL_INT8_SCHEMA => value.fromByte()
          case Schema.INT16_SCHEMA | Schema.OPTIONAL_INT16_SCHEMA => value.fromShort()
          case Schema.INT32_SCHEMA | Schema.OPTIONAL_INT32_SCHEMA => value.fromInt()
          case Schema.INT64_SCHEMA | Schema.OPTIONAL_INT64_SCHEMA => value.fromLong()
          case Schema.STRING_SCHEMA | Schema.OPTIONAL_STRING_SCHEMA => value.fromString()
        }
        Some(bytes)
    }
  }
}


