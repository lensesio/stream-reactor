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
package com.datamountaineer.streamreactor.connect.voltdb

import org.apache.kafka.connect.data._

import scala.collection.JavaConversions._

trait FieldsValuesExtractor {
  def get(struct: Struct): Map[String, Any]
}

case class StructFieldsExtractor(targetTable: String,
                                 includeAllFields: Boolean,
                                 fieldsAliasMap: Map[String, String],
                                 isUpsert: Boolean = false) extends FieldsValuesExtractor {
  require(targetTable != null && targetTable.trim.length > 0)

  def get(struct: Struct): Map[String, Any] = {
    val schema = struct.schema()
    val fields: Seq[Field] = {
      if (includeAllFields) {
        schema.fields()
      } else {
        schema.fields().filter(f => fieldsAliasMap.contains(f.name()))
      }
    }

    fields.flatMap { field =>
      Option(struct.get(field))
        .map { value =>
          val schema = field.schema()
          //handle specific schema
          val fieldValue = schema.name() match {
            case Decimal.LOGICAL_NAME => Decimal.toLogical(schema, value.asInstanceOf[Array[Byte]])
            case Date.LOGICAL_NAME => Date.toLogical(schema, value.asInstanceOf[Int])
            case Time.LOGICAL_NAME => Time.toLogical(schema, value.asInstanceOf[Int])
            case Timestamp.LOGICAL_NAME => Timestamp.toLogical(schema, value.asInstanceOf[Long])
            case _ => value
          }
          fieldsAliasMap.getOrElse(field.name(), field.name()) -> fieldValue
        }
    }.toMap

  }
}


