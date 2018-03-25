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

package com.datamountaineer.streamreactor.connect.voltdb

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.connect.data.{Field, Struct, _}

import scala.collection.JavaConversions._

trait FieldsValuesExtractor {
  def get(struct: Struct): Map[String, Any]
}

case class StructFieldsExtractor(targetTable: String,
                                 includeAllFields: Boolean,
                                 fieldsAliasMap: Map[String, String],
                                 isUpsert: Boolean = false) extends FieldsValuesExtractor with StrictLogging {
  require(targetTable != null && targetTable.trim.length > 0)

  def get(struct: Struct): Map[String, Any] = {
    val schema = struct.schema()
    val fields: Seq[Field] = {
      if (includeAllFields) {
        schema.fields()
      } else {
        val selectedFields = schema.fields().filter(f => fieldsAliasMap.contains(f.name()))
        val diffSet = fieldsAliasMap.keySet.diff(selectedFields.map(_.name()).toSet)
        if (diffSet.nonEmpty) {
          val errMsg = s"Following columns ${diffSet.mkString(",")} have not been found. Available columns:${fieldsAliasMap.keys.mkString(",")}"
          logger.error(errMsg)
          sys.error(errMsg)
        }
        selectedFields
      }
    }

    //need to select all fields including null. the stored proc needs a fixed set of params
    fields.map { field =>
      val schema = field.schema()
      val value = Option(struct.get(field))
        .map { value =>
          //handle specific schema
          schema.name() match {
            case Decimal.LOGICAL_NAME =>
              value.asInstanceOf[Any] match {
                case _:java.math.BigDecimal => value
                case arr: Array[Byte] => Decimal.toLogical(schema, arr)
                case _ => throw new IllegalArgumentException(s"${field.name()} is not handled for value:$value")
              }
            case Time.LOGICAL_NAME =>
              value.asInstanceOf[Any] match {
                case i: Int => StructFieldsExtractor.TimeFormat.format(Time.toLogical(schema, i))
                case d:java.util.Date => StructFieldsExtractor.TimeFormat.format(d)
                case _ => throw new IllegalArgumentException(s"${field.name()} is not handled for value:$value")
              }

            case Timestamp.LOGICAL_NAME =>
              value.asInstanceOf[Any] match {
                case d:java.util.Date => StructFieldsExtractor.DateFormat.format(d)
                case l: Long => StructFieldsExtractor.DateFormat.format(Timestamp.toLogical(schema, l))
                case _ => throw new IllegalArgumentException(s"${field.name()} is not handled for value:$value")
              }

            case _ => value
          }
        }.orNull

      fieldsAliasMap.getOrElse(field.name(), field.name()) -> value
    }.toMap
  }
}


object StructFieldsExtractor {
  val DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val TimeFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")
  DateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
}