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

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import com.datamountaineer.streamreactor.connect.hbase.StructFieldsExtractorBytes._
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data._

import scala.collection.JavaConversions._

trait FieldsValuesExtractor {
  def get(struct: Struct): Seq[(String, Array[Byte])]
}

case class StructFieldsExtractorBytes(includeAllFields: Boolean, fieldsAliasMap: Map[String, String]) extends FieldsValuesExtractor with StrictLogging {

  def get(struct: Struct): Seq[(String, Array[Byte])] = {
    val schema = struct.schema()
    val fields: Seq[Field] = if (includeAllFields) {
      schema.fields()
    }
    else {
      val selectedFields = schema.fields().filter(f => fieldsAliasMap.contains(f.name()))
      val diffSet = fieldsAliasMap.keySet.diff(selectedFields.map(_.name()).toSet)
      if (diffSet.nonEmpty) {
        val errMsg = s"Following columns ${diffSet.mkString(",")} have not been found. Available columns:${fieldsAliasMap.keys.mkString(",")}"
        logger.error(errMsg)
        sys.error(errMsg)
      }
      selectedFields
    }

    val fieldsAndValues = fields.flatMap(field =>
      getFieldBytes(field, struct).map(bytes => fieldsAliasMap.getOrElse(field.name(), field.name()) -> bytes))

    fieldsAndValues
  }

  private def getFieldBytes(field: Field, struct: Struct): Option[Array[Byte]] = {
    Option(struct.get(field))
      .map { value =>
        field.schema() match {
          case Schema.BOOLEAN_SCHEMA | Schema.OPTIONAL_BOOLEAN_SCHEMA => value.fromBoolean()
          case Schema.BYTES_SCHEMA | Schema.OPTIONAL_BYTES_SCHEMA =>
            if (Decimal.LOGICAL_NAME.equals(field.schema().name())) {
              Decimal.toLogical(field.schema(), value.asInstanceOf[Array[Byte]]).fromBigDecimal()
            } else value.fromBytes()
          case Schema.FLOAT32_SCHEMA | Schema.OPTIONAL_FLOAT32_SCHEMA => value.fromFloat()
          case Schema.FLOAT64_SCHEMA | Schema.OPTIONAL_FLOAT64_SCHEMA => value.fromDouble()
          case Schema.INT8_SCHEMA | Schema.OPTIONAL_INT8_SCHEMA => value.fromByte()
          case Schema.INT16_SCHEMA | Schema.OPTIONAL_INT16_SCHEMA => value.fromShort()
          case Schema.INT32_SCHEMA | Schema.OPTIONAL_INT32_SCHEMA =>
            if (Date.LOGICAL_NAME.equals(field.schema().name())) {
              DateFormat.format(Date.toLogical(field.schema(), value.asInstanceOf[Int])).fromString()
            }
            else if (Time.LOGICAL_NAME.equals(field.schema().name())) {
              TimeFormat.format(Time.toLogical(field.schema(), value.asInstanceOf[Int])).fromString()
            }
            else value.fromInt()
          case Schema.INT64_SCHEMA | Schema.OPTIONAL_INT64_SCHEMA => value.fromLong()
          case Schema.STRING_SCHEMA | Schema.OPTIONAL_STRING_SCHEMA => value.fromString()
          case other => sys.error(s"$other is not a recognized schema!")
        }
      }
  }
}


object StructFieldsExtractorBytes {
  val DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val TimeFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")

  DateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
}