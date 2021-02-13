/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.common.schemas

/**
  * Created by andrew@datamountaineer.com on 29/05/16. 
  * kafka-connect-common
  */

import java.text.SimpleDateFormat
import java.util.TimeZone
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.JavaConverters._

trait StructFieldsValuesExtractor {
  def get(struct: Struct): Seq[(String, Any)]
}

/**
  * Extracts fields from a SinkRecord Struct based on a specified set of provided columns.
  *
  * @param includeAllFields Boolean indicating if all the fields from the SinkRecord are to be written to the sink
  * @param fieldsAliasMap   A map of fields and their alias,if provided, to extract from the SinkRecord
  **/
case class StructFieldsExtractor(includeAllFields: Boolean, fieldsAliasMap: Map[String, String]) extends StructFieldsValuesExtractor {

  /**
    * Get a sequence of columns names to column values for a given struct
    *
    * @param struct A SinkRecord struct
    * @return a Sequence of column names and values
    * */
  def get(struct: Struct): Seq[(String, AnyRef)] = {
    val schema = struct.schema()
    val fields: Seq[Field] = if (includeAllFields) schema.fields().asScala
    else schema.fields().asScala.filter(f => fieldsAliasMap.contains(f.name()))

    val fieldsAndValues = fields.flatMap(field =>
      getFieldValue(field, struct).map(value => fieldsAliasMap.getOrElse(field.name(), field.name()) -> value))
    fieldsAndValues
  }

  /**
    * For a field in a struct return the value
    *
    * @param field  A field to return the value for
    * @param struct A struct to extract the field from
    * @return an optional value for the field
    * */
  private def getFieldValue(field: Field, struct: Struct): Option[AnyRef] = {
    Option(struct.get(field)) match {
      case None => None
      case Some(value) =>
        val fieldName = field.name()
        Option(field.schema().name()).collect {
          case Decimal.LOGICAL_NAME =>
            value match {
              case bd: BigDecimal => bd
              case array: Array[Byte] => Decimal.toLogical(field.schema, array)
              case _ => throw new IllegalArgumentException(s"Can't convert $value to Decimal for schema:${field.schema().`type`()}")

            }
          case Date.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case d: java.util.Date => d
              case i: Int => Date.toLogical(field.schema, i)
              case _ => throw new IllegalArgumentException(s"Can't convert $value to Date for schema:${field.schema().`type`()}")
            }
          case Time.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case i: Int => Time.toLogical(field.schema, i)
              case d: java.util.Date => d
              case _ => throw new IllegalArgumentException(s"Can't convert $value to Date for schema:${field.schema().`type`()}")
            }
          case Timestamp.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case l: Long => Timestamp.toLogical(field.schema, l)
              case d: java.util.Date => d
              case _ => throw new IllegalArgumentException(s"Can't convert $value to Date for schema:${field.schema().`type`()}")
            }
        }.orElse {
          val v = field.schema().`type`() match {
            case Schema.Type.BOOLEAN => struct.getBoolean(fieldName)
            case Schema.Type.BYTES => struct.getBytes(fieldName)
            case Schema.Type.FLOAT32 => struct.getFloat32(fieldName)
            case Schema.Type.FLOAT64 => struct.getFloat64(fieldName)
            case Schema.Type.INT8 => struct.getInt8(fieldName)
            case Schema.Type.INT16 => struct.getInt16(fieldName)
            case Schema.Type.INT32 => struct.getInt32(fieldName)
            case Schema.Type.INT64 => struct.getInt64(fieldName)
            case Schema.Type.STRING => struct.getString(fieldName)
            case other => throw new ConnectException(s"$other is not a recognized schema")
          }
          Some(v)
        }
    }
  }
}


object StructFieldsExtractor {
  val DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val TimeFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")
  DateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
}