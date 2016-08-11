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

package com.datamountaineer.streamreactor.connect.influx

import io.confluent.common.config.ConfigException
import org.apache.kafka.connect.data.{Field, Schema, Struct}

import scala.collection.JavaConversions._

case class RecordData(timestamp: Long, fields: Seq[(String, Any)])

trait FieldsValuesExtractor {
  def get(struct: Struct): RecordData

}

case class StructFieldsExtractor(includeAllFields: Boolean,
                                 fieldsAliasMap: Map[String, String],
                                 timestampField: Option[String]) extends FieldsValuesExtractor {

  def get(struct: Struct): RecordData = {
    val schema = struct.schema()
    val fields: Seq[Field] = {
      if (includeAllFields) schema.fields()
      else schema.fields().filter(f => fieldsAliasMap.contains(f.name()))
    }

    val timestamp = timestampField
      .map(t=>Option(schema.field(t)).getOrElse(throw new ConfigException(s"$t is not a valid field.")))
      .map(struct.get)
      .map { value =>
        value.asInstanceOf[Any] match {
          case b: Byte => b.toLong
          case s: Short => s.toLong
          case i: Int => i.toLong
          case l: Long => l
          case _ => throw new ConfigException(s"${timestampField.get} is not a valid field for the timestamp")
        }
      }.getOrElse(System.currentTimeMillis())

    val fieldsAndValues = fields.flatMap { field =>
      Option(struct.get(field))
        .map { value =>
          val schema = field.schema()
          if (schema == Schema.BYTES_SCHEMA || schema == Schema.OPTIONAL_BYTES_SCHEMA) {
            throw new RuntimeException("BYTES is not supported by InfluxDb")
          }
          fieldsAliasMap.getOrElse(field.name(), field.name()) -> value
        }
    }

    RecordData(timestamp, fieldsAndValues)
  }
}


