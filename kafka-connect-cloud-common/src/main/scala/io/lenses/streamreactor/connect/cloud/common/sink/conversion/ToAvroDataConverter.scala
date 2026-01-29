/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.conversion

import io.confluent.connect.avro.AvroData
import io.confluent.connect.avro.AvroDataConfig
import io.confluent.connect.schema.AbstractDataConfig
import org.apache.avro.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.{ Schema => ConnectSchema }

import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

import java.nio.ByteBuffer
import java.time.LocalDate
import java.time.ZoneId
import java.time.temporal.ChronoUnit
import java.util
import java.util.Date
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

object ToAvroDataConverter {

  private val avroDataConfig = new AvroDataConfig(
    Map(
      AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG -> "true",
      AbstractDataConfig.SCHEMAS_CACHE_SIZE_CONFIG       -> "100",
    ).asJava,
  )
  private val avroDataConverter = new AvroData(avroDataConfig)

  def convertSchema(connectSchema: ConnectSchema): Schema = avroDataConverter.fromConnectSchema(connectSchema)

  def convertToGenericRecord[A <: Any](sinkData: SinkData): Any =
    sinkData match {
      case StructSinkData(structVal) => avroDataConverter.fromConnectData(structVal.schema(), structVal)
      case other                     => convertNonStructSinkData(other)
    }

  /**
    * Converts SinkData to an Avro GenericRecord using a target Avro schema.
    * This ensures the GenericRecord uses the exact schema object provided,
    * which is critical for Parquet/Avro writers that rely on schema identity
    * for field position lookups.
    *
    * @param sinkData The SinkData to convert
    * @param targetSchema The target Avro schema to use for the GenericRecord
    * @return The converted value (GenericRecord for structs, primitive values otherwise)
    */
  def convertToGenericRecordWithSchema(sinkData: SinkData, targetSchema: Schema): Any =
    sinkData match {
      case StructSinkData(structVal) => convertStructToGenericRecord(structVal, targetSchema)
      case other                     => convertNonStructSinkData(other)
    }

  /**
    * Common handler for non-struct SinkData types.
    * This consolidates the conversion logic for all SinkData variants except StructSinkData,
    * which requires different handling depending on whether a target schema is provided.
    */
  private def convertNonStructSinkData(sinkData: SinkData): Any =
    sinkData match {
      case MapSinkData(map, _)         => convert(map)
      case ArraySinkData(array, _)     => convert(array)
      case ByteArraySinkData(array, _) => ByteBuffer.wrap(array)
      case primitive: PrimitiveSinkData => primitive.value
      case DateSinkData(value)      => convertDateToDaysFromEpoch(value)
      case TimeSinkData(value)      => value.getTime
      case TimestampSinkData(value) => value.toInstant.toEpochMilli
      case _: NullSinkData => null
      case other => throw new IllegalArgumentException(s"Unknown SinkData type, ${other.getClass.getSimpleName}")
    }

  /**
    * Converts a Connect Struct to an Avro GenericRecord using the specified target schema.
    * This handles nested structures recursively.
    */
  private def convertStructToGenericRecord(struct: Struct, targetSchema: Schema): GenericRecord = {
    val record = new GenericData.Record(targetSchema)
    targetSchema.getFields.asScala.foreach { avroField =>
      val fieldName    = avroField.name()
      val connectField = struct.schema().field(fieldName)
      if (connectField != null) {
        val value          = struct.get(connectField)
        val convertedValue = convertFieldValue(value, avroField.schema())
        record.put(fieldName, convertedValue)
      } else if (avroField.hasDefaultValue) {
        record.put(fieldName, avroField.defaultVal())
      }
    // If field doesn't exist and has no default, leave it null (for optional fields)
    }
    record
  }

  /**
    * Converts a Connect value to an Avro value using the target Avro schema.
    * Handles logical types (Date, Time, Timestamp) and nested structures.
    */
  private def convertFieldValue(value: Any, targetSchema: Schema): Any =
    if (value == null) {
      null
    } else {
      // First check for logical types (Date, Time, Timestamp) as they need special handling
      Option(targetSchema.getLogicalType).map(_.getName) match {
        case Some("date") =>
          value match {
            case d: Date => convertDateToDaysFromEpoch(d)
            case other => other
          }
        case Some("time-millis") =>
          value match {
            case d: Date => d.getTime.toInt
            case other => other
          }
        case Some("time-micros") =>
          value match {
            case d: Date => d.getTime * 1000L
            case other => other
          }
        case Some("timestamp-millis") =>
          value match {
            case d: Date => d.getTime
            case other => other
          }
        case Some("timestamp-micros") =>
          value match {
            case d: Date => d.getTime * 1000L
            case other => other
          }
        case Some("decimal") =>
          value match {
            case bd: java.math.BigDecimal =>
              ByteBuffer.wrap(bd.unscaledValue().toByteArray)
            case other => other
          }
        case _ =>
          // No logical type or unhandled logical type - convert based on physical schema type
          convertBySchemaType(value, targetSchema)
      }
    }

  /**
    * Converts a value based on the physical Avro schema type.
    */
  private def convertBySchemaType(value: Any, targetSchema: Schema): Any =
    targetSchema.getType match {
      case Schema.Type.RECORD =>
        value match {
          case s: Struct => convertStructToGenericRecord(s, targetSchema)
          case other => other
        }
      case Schema.Type.ARRAY =>
        value match {
          case list: java.util.List[_] =>
            val elementSchema = targetSchema.getElementType
            list.asScala.map(elem => convertFieldValue(elem, elementSchema)).asJava
          case other => other
        }
      case Schema.Type.MAP =>
        value match {
          case map: java.util.Map[_, _] =>
            val valueSchema = targetSchema.getValueType
            map.asScala.map {
              case (k, v) => k -> convertFieldValue(v, valueSchema)
            }.asJava
          case other => other
        }
      case Schema.Type.UNION =>
        // Handle union types (typically [null, actualType])
        val nonNullSchema = targetSchema.getTypes.asScala.find(_.getType != Schema.Type.NULL)
        nonNullSchema.map(convertFieldValue(value, _)).getOrElse(value)
      case Schema.Type.BYTES =>
        value match {
          case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
          case bb:    ByteBuffer  => bb
          case other => other
        }
      case _ => value
    }

  private def convertDateToDaysFromEpoch[A <: Any](value: Date) =
    ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), LocalDate.ofInstant(value.toInstant, ZoneId.systemDefault()))

  private def convertArray(list: java.util.List[_]) = list.asScala.map(convert).asJava
  private def convert(value:     Any): Any =
    value match {
      case map:   java.util.Map[_, _] => convertMap(map)
      case array: Array[_]            => convertArray(array.toSeq.asJava)
      case list:  java.util.List[_]   => convertArray(list)
      case s:     Struct              => avroDataConverter.fromConnectData(s.schema(), s)
      case _ => value
    }

  private def convertMap(map: java.util.Map[_, _]): util.Map[Any, Any] =
    map.asScala.map {
      case (key, value) => convert(key) -> convert(value)
    }.asJava

}
