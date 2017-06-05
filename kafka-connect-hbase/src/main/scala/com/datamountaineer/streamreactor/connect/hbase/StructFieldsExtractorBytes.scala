/*
 * Copyright 2017 Datamountaineer.
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
 */

package com.datamountaineer.streamreactor.connect.hbase

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
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
        Option(field.schema().name()).collect {
          case Decimal.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case _:java.math.BigDecimal => value.fromBigDecimal()
              case arr: Array[Byte] => Decimal.toLogical(field.schema, arr).asInstanceOf[Any].fromBigDecimal()
              case _ => throw new IllegalArgumentException(s"${field.name()} is not handled for value:$value")
            }
          case Time.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case i: Int => StructFieldsExtractorBytes.TimeFormat.format(Time.toLogical(field.schema, i)).asInstanceOf[Any].fromString()
              case d:java.util.Date => StructFieldsExtractorBytes.TimeFormat.format(d).asInstanceOf[Any].fromString()
              case _ => throw new IllegalArgumentException(s"${field.name()} is not handled for value:$value")
            }

          case Timestamp.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case d:java.util.Date => StructFieldsExtractorBytes.DateFormat.format(d).asInstanceOf[Any].fromString()
              case l: Long => StructFieldsExtractorBytes.DateFormat.format(Timestamp.toLogical(field.schema, l)).asInstanceOf[Any].fromString()
              case _ => throw new IllegalArgumentException(s"${field.name()} is not handled for value:$value")
            }
        }.getOrElse {

          field.schema().`type`() match {
            case Schema.Type.BOOLEAN => value.fromBoolean()
            case Schema.Type.BYTES => value.fromBytes()
            case Schema.Type.FLOAT32 => value.fromFloat()
            case Schema.Type.FLOAT64 => value.fromDouble()
            case Schema.Type.INT8 => value.fromByte()
            case Schema.Type.INT16 => value.fromShort()
            case Schema.Type.INT32 => value.fromInt()
            case Schema.Type.INT64 => value.fromLong()
            case Schema.Type.STRING => value.fromString()
            case other => sys.error(s"$other is not a recognized schema!")
          }
        }
      }
  }
}


object StructFieldsExtractorBytes {
  val DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val TimeFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")

  DateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
}