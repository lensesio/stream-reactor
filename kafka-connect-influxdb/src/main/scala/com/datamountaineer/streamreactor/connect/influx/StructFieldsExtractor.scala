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

package com.datamountaineer.streamreactor.connect.influx

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.common.config.ConfigException
import org.apache.kafka.connect.data._

import scala.collection.JavaConversions._

case class RecordData(timestamp: Long, fields: Seq[(String, Any)])

trait FieldsValuesExtractor {
  def get(struct: Struct): RecordData

}

case class StructFieldsExtractor(includeAllFields: Boolean,
                                 fieldsAliasMap: Map[String, String],
                                 timestampField: Option[String],
                                 ignoredFields: Set[String]) extends FieldsValuesExtractor with StrictLogging {

  def get(struct: Struct): RecordData = {
    val schema = struct.schema()
    val fields: Seq[Field] = {
      if (includeAllFields) {
        schema.fields()
      }
      else {
        schema.fields().filter(f => fieldsAliasMap.contains(f.name()))
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

    val timestamp = timestampField
      .map(t => Option(schema.field(t)).getOrElse(throw new ConfigException(s"$t is not a valid field.")))
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

          //decimal info comes as a logical schema. so if it's bytes and not a decimal throw an error because influxdb doesn't have
          //support bytes
          if ((schema.`type`() == Schema.Type.BYTES) && !Decimal.LOGICAL_NAME.equals(schema.name())) {
            throw new RuntimeException("BYTES schema is not supported. Cannot store bytes in InfluxDb")
          }

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

          fieldsAliasMap.getOrElse(field.name(), field.name()) -> value
        }
    }

    RecordData(timestamp, fieldsAndValues)
  }
}


object StructFieldsExtractor {
  val DateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val TimeFormat: SimpleDateFormat = new SimpleDateFormat("HH:mm:ss.SSSZ")
  DateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
}