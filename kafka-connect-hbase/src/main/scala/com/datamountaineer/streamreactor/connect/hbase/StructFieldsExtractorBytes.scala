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

import com.datamountaineer.streamreactor.connect.hbase.BytesHelper._
import com.datamountaineer.streamreactor.connect.hbase.StructFieldsExtractorBytes.UTC
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.ConnectException

import java.time.ZoneId
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.ListHasAsScala

trait FieldsValuesExtractor {
  def get(struct: Struct): Seq[(String, Array[Byte])]
}

case class StructFieldsExtractorBytes(includeAllFields: Boolean, fieldsAliasMap: Map[String, String])
    extends FieldsValuesExtractor
    with StrictLogging {

  def get(struct: Struct): Seq[(String, Array[Byte])] = {
    val schema = struct.schema()
    val fields: Seq[Field] = if (includeAllFields) {
      schema.fields().asScala.toList
    } else {
      val selectedFields = schema.fields().asScala.filter(f => fieldsAliasMap.contains(f.name()))
      val diffSet        = fieldsAliasMap.keySet.diff(selectedFields.map(_.name()).toSet)
      if (diffSet.nonEmpty) {
        val errMsg =
          s"Following columns ${diffSet.mkString(",")} have not been found. Available columns:${fieldsAliasMap.keys.mkString(",")}"
        logger.error(errMsg)
        throw new ConnectException(errMsg)
      }
      selectedFields.toList
    }

    val fieldsAndValues = fields.flatMap(field =>
      getFieldBytes(field, struct).map(bytes => fieldsAliasMap.getOrElse(field.name(), field.name()) -> bytes),
    )

    fieldsAndValues
  }

  private def getFieldBytes(field: Field, struct: Struct): Option[Array[Byte]] =
    Option(struct.get(field))
      .map { value =>
        Option(field.schema().name()).collect {
          case Decimal.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case _:   java.math.BigDecimal => value.fromBigDecimal()
              case arr: Array[Byte]          => Decimal.toLogical(field.schema, arr).asInstanceOf[Any].fromBigDecimal()
              case _ => throw new IllegalArgumentException(s"${field.name()} is not handled for value:$value")
            }
          case Time.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case d: java.util.Date =>
                val zonedTime = d.toInstant.atZone(UTC).withFixedOffsetZone()
                StructFieldsExtractorBytes.TimeFormat.format(zonedTime).asInstanceOf[Any].fromString()
              case _ => throw new IllegalArgumentException(s"${field.name()} is not handled for value:$value")
            }

          case Timestamp.LOGICAL_NAME =>
            value.asInstanceOf[Any] match {
              case d: java.util.Date =>
                StructFieldsExtractorBytes.DateFormat.format(d.toInstant).asInstanceOf[Any].fromString()
              case _ => throw new IllegalArgumentException(s"${field.name()} is not handled for value:$value")
            }
        }.getOrElse {

          field.schema().`type`() match {
            case Schema.Type.BOOLEAN => value.fromBoolean()
            case Schema.Type.BYTES   => value.fromBytes()
            case Schema.Type.FLOAT32 => value.fromFloat()
            case Schema.Type.FLOAT64 => value.fromDouble()
            case Schema.Type.INT8    => value.fromByte()
            case Schema.Type.INT16   => value.fromShort()
            case Schema.Type.INT32   => value.fromInt()
            case Schema.Type.INT64   => value.fromLong()
            case Schema.Type.STRING  => value.fromString()
            case other               => throw new ConnectException(s"$other is not a recognized schema!")
          }
        }
      }
}

object StructFieldsExtractorBytes {
  private val UTC        = ZoneId.of("UTC")
  private val DateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(UTC)
  private val TimeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSSZ").withZone(UTC)
}
