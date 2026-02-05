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

import io.lenses.streamreactor.connect.config.AvroDataFactory
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

  private val avroDataConverter = AvroDataFactory.create(100)

  /** Schema name used by Confluent's AvroConverter for union types */
  private val ConfluentAvroUnionSchemaName = "io.confluent.connect.avro.Union"

  /**
    * Checks if a Connect Struct represents an Avro union type.
    * Confluent's Avro converter represents complex unions as STRUCTs with name "io.confluent.connect.avro.Union".
    */
  private def isConnectUnionStruct(struct: Struct): Boolean =
    Option(struct.schema().name()).contains(ConfluentAvroUnionSchemaName)

  /**
    * Extracts the active value from a Connect union struct and converts it to Avro.
    * A Connect union struct has one non-null field (the active branch) with others set to null.
    *
    * @param unionStruct The Connect union struct (e.g., Struct{string=CLOCK_OUT})
    * @param targetUnionSchema The target Avro union schema
    * @return The extracted and converted value
    */
  private def extractAndConvertUnionValue(unionStruct: Struct, targetUnionSchema: Schema): Any = {
    val structSchema = unionStruct.schema()
    // Find the non-null field in the union struct (the active branch)
    val activeFieldOpt = structSchema.fields().asScala.find { field =>
      unionStruct.get(field) != null
    }

    activeFieldOpt match {
      case Some(activeField) =>
        val fieldValue = unionStruct.get(activeField)
        val fieldName  = activeField.name()

        // Find the matching Avro schema in the union by name or type
        // Priority: exact name match, then type name match
        val matchingAvroSchema = targetUnionSchema.getTypes.asScala.find { avroType =>
          // Match by full name (for named types like enums with namespace)
          Option(avroType.getFullName).exists(fn => fn == fieldName || fn.endsWith("." + fieldName)) ||
          // Match by simple name
          Option(avroType.getName).contains(fieldName) ||
          // Match by type name (e.g., "string" matches Schema.Type.STRING)
          avroType.getType.getName.equalsIgnoreCase(fieldName)
        }

        matchingAvroSchema match {
          case Some(avroSchema) =>
            // Convert the value using the matched schema
            convertFieldValue(fieldValue, avroSchema)
          case None =>
            // Fallback: try to find by Connect type match
            val typeMatchSchema = targetUnionSchema.getTypes.asScala.find { avroType =>
              avroType.getType != Schema.Type.NULL &&
              activeField.schema().`type`().getName.toUpperCase == avroType.getType.name()
            }
            typeMatchSchema.map(convertFieldValue(fieldValue, _)).getOrElse(fieldValue)
        }

      case None =>
        // All fields are null - return null
        null
    }
  }

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
        // Handle union types - this is complex because Connect represents Avro unions in two ways:
        // 1. Simple nullable union [null, T] - value is just T's value or null
        // 2. Complex union [T1, T2, ...] - represented as a Struct named "io.confluent.connect.avro.Union"
        //    with fields for each type, where only one field is non-null
        value match {
          case unionStruct: Struct if isConnectUnionStruct(unionStruct) =>
            // This is a Connect union struct - extract the active branch value
            extractAndConvertUnionValue(unionStruct, targetSchema)
          case _ =>
            // Simple case: just find the matching non-null schema type
            val nonNullSchema = targetSchema.getTypes.asScala.find(_.getType != Schema.Type.NULL)
            nonNullSchema.map(convertFieldValue(value, _)).getOrElse(value)
        }
      case Schema.Type.BYTES =>
        value match {
          case bytes: Array[Byte] => ByteBuffer.wrap(bytes)
          case bb:    ByteBuffer  => bb
          case other => other
        }
      case Schema.Type.ENUM =>
        // For enum types, value should be a string - create an Avro GenericEnumSymbol
        value match {
          case s: String => new GenericData.EnumSymbol(targetSchema, s)
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
