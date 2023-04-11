/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.landoop.streamreactor.connect.hive.parquet

import com.landoop.streamreactor.connect.hive.UnsupportedSchemaType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema._

import scala.jdk.CollectionConverters.ListHasAsScala

/**
  * Conversion functions to/from parquet/kafka types.
  *
  * Parquet types are defined at the parquet repo:
  * https://github.com/apache/parquet-format/blob/c6d306daad4910d21927b8b4447dc6e9fae6c714/LogicalTypes.md
  */
object ParquetSchemas {

  def toKafkaStruct(group: GroupType): Schema = {
    val builder = SchemaBuilder.struct().name(group.getName)
    group.getFields.asScala.foreach { field =>
      builder.field(field.getName, toKafka(field))
    }
    builder.build()
  }

  def toKafkaArray(group: GroupType): Schema = {
    require(group.getOriginalType == OriginalType.LIST)
    // a parquet list always has an element sub group
    val elementField = group.getFields.get(0)
    // the element must be set to repeated for a valid parquet list
    require(elementField.isRepetition(Type.Repetition.REPEATED))
    // the subgroup has a single field which is the array type
    val arrayField = elementField.asGroupType().getFields.get(0)
    val builder    = SchemaBuilder.array(toKafka(arrayField))
    // the optionality is set on the outer group
    if (group.isRepetition(Type.Repetition.OPTIONAL)) builder.optional().build() else builder.required().build()
  }

  /**
    * Converts a Parquet [[GroupType]] into a kafka connect [[Schema]].
    * Each field of the parquet group is converted into a Schema Field.
    * Nullability is determined by the parquet optional flag.
    */
  def toKafka(group: GroupType): Schema =
    group.getOriginalType match {
      case OriginalType.LIST => toKafkaArray(group)
      case _                 => toKafkaStruct(group)
    }

  def toKafka(`type`: Type): Schema = `type` match {
    case pt: PrimitiveType => toKafka(pt)
    case gt: GroupType     => toKafka(gt)
  }

  def toKafka(t: PrimitiveType): Schema = {

    def int32(original: LogicalTypeAnnotation): SchemaBuilder = original match {
      case lta: LogicalTypeAnnotation.IntLogicalTypeAnnotation if lta.getBitWidth == 32 =>
        SchemaBuilder.`type`(Schema.Type.INT32)
      case lta: LogicalTypeAnnotation.IntLogicalTypeAnnotation if lta.getBitWidth == 16 =>
        SchemaBuilder.`type`(Schema.Type.INT16)
      case lta: LogicalTypeAnnotation.IntLogicalTypeAnnotation if lta.getBitWidth == 8 =>
        SchemaBuilder.`type`(Schema.Type.INT8)
      case _: LogicalTypeAnnotation.DateLogicalTypeAnnotation => SchemaBuilder.`type`(Schema.Type.INT32)
      case _ => SchemaBuilder.`type`(Schema.Type.INT32)
    }

    def int64(original: LogicalTypeAnnotation): SchemaBuilder = original match {
      case lta: LogicalTypeAnnotation.IntLogicalTypeAnnotation if lta.getBitWidth == 64 =>
        SchemaBuilder.`type`(Schema.Type.INT64)
      case _: LogicalTypeAnnotation.TimestampLogicalTypeAnnotation => SchemaBuilder.`type`(Schema.Type.INT64)
      case _ => SchemaBuilder.`type`(Schema.Type.INT64)
    }

    def binary(original: LogicalTypeAnnotation): SchemaBuilder = original match {
      case _: LogicalTypeAnnotation.EnumLogicalTypeAnnotation   => SchemaBuilder.`type`(Schema.Type.STRING)
      case _: LogicalTypeAnnotation.StringLogicalTypeAnnotation => SchemaBuilder.`type`(Schema.Type.STRING)
      case _ => SchemaBuilder.`type`(Schema.Type.BYTES)
    }

    val builder: SchemaBuilder = t.getPrimitiveTypeName match {
      case PrimitiveTypeName.BINARY  => binary(t.getLogicalTypeAnnotation())
      case PrimitiveTypeName.BOOLEAN => SchemaBuilder.`type`(Schema.Type.BOOLEAN)
      case PrimitiveTypeName.DOUBLE  => SchemaBuilder.`type`(Schema.Type.FLOAT64)
      case PrimitiveTypeName.FLOAT   => SchemaBuilder.`type`(Schema.Type.FLOAT32)
      case PrimitiveTypeName.INT32   => int32(t.getLogicalTypeAnnotation())
      case PrimitiveTypeName.INT64   => int64(t.getLogicalTypeAnnotation())
      case PrimitiveTypeName.INT96   => SchemaBuilder.`type`(Schema.Type.STRING)
      case other                     => throw UnsupportedSchemaType(s"Unsupported data type $other")
    }

    if (t.isRepetition(Type.Repetition.OPTIONAL)) builder.optional().build() else builder.required().build()
  }

  def toParquetMessage(struct: Schema, name: String): MessageType = {
    require(name != null, "name cannot be null")
    val types = struct.fields.asScala.toSeq.map(field => toParquetType(field.schema(), field.name()))
    Types.buildMessage().addFields(types: _*).named(name)
  }

  def toParquetGroup(struct: Schema, name: String, repetition: Type.Repetition): GroupType = {
    val types = struct.fields.asScala.toSeq.map(field => toParquetType(field.schema(), field.name()))
    Types.buildGroup(repetition).addFields(types: _*).named(name)
  }

  def toParquetType(schema: Schema, name: String): Type = {
    val repetition = if (schema.isOptional) Type.Repetition.OPTIONAL else Type.Repetition.REQUIRED
    schema.`type`() match {
      case Schema.Type.STRING =>
        Types.primitive(PrimitiveTypeName.BINARY, repetition).as(LogicalTypeAnnotation.stringType()).named(name)
      case Schema.Type.BOOLEAN => Types.primitive(PrimitiveTypeName.BOOLEAN, repetition).named(name)
      case Schema.Type.FLOAT32 => Types.primitive(PrimitiveTypeName.FLOAT, repetition).named(name)
      case Schema.Type.FLOAT64 => Types.primitive(PrimitiveTypeName.DOUBLE, repetition).named(name)
      case Schema.Type.INT8 =>
        Types.primitive(PrimitiveTypeName.INT32, repetition).as(LogicalTypeAnnotation.intType(8, true)).named(name)
      case Schema.Type.INT16 =>
        Types.primitive(PrimitiveTypeName.INT32, repetition).as(LogicalTypeAnnotation.intType(16, true)).named(name)
      case Schema.Type.INT32 => Types.primitive(PrimitiveTypeName.INT32, repetition).named(name)
      case Schema.Type.INT64 => Types.primitive(PrimitiveTypeName.INT64, repetition).named(name)
      case Schema.Type.BYTES => Types.primitive(PrimitiveTypeName.BINARY, repetition).named(name)
      case Schema.Type.ARRAY =>
        Types.list(repetition).element(toParquetType(schema.valueSchema(), "element")).named(name)
      case Schema.Type.STRUCT => toParquetGroup(schema, name, repetition)
      case other              => throw UnsupportedSchemaType(s"Unsupported data type $other")
    }
  }
}
