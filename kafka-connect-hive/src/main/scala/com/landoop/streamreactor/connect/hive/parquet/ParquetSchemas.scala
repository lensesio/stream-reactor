package com.landoop.streamreactor.connect.hive.parquet

import com.landoop.streamreactor.connect.hive.UnsupportedSchemaType
import org.apache.kafka.connect.data.{Schema, SchemaBuilder}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema._


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
    val builder = SchemaBuilder.array(toKafka(arrayField))
    // the optionality is set on the outer group
    if (group.isRepetition(Type.Repetition.OPTIONAL)) builder.optional().build() else builder.required().build()
  }

  /**
    * Converts a Parquet [[GroupType]] into a kafka connect [[Schema]].
    * Each field of the parquet group is converted into a Schema Field.
    * Nullability is determined by the parquet optional flag.
    */
  def toKafka(group: GroupType): Schema = {
    group.getOriginalType match {
      case OriginalType.LIST => toKafkaArray(group)
      case _ => toKafkaStruct(group)
    }
  }

  def toKafka(`type`: Type): Schema = `type` match {
    case pt: PrimitiveType => toKafka(pt)
    case gt: GroupType => toKafka(gt)
  }

  def toKafka(t: PrimitiveType): Schema = {

    def int32(original: OriginalType): SchemaBuilder = original match {
      case OriginalType.INT_32 | OriginalType.UINT_32 => SchemaBuilder.`type`(Schema.Type.INT32)
      case OriginalType.UINT_16 | OriginalType.INT_16 => SchemaBuilder.`type`(Schema.Type.INT16)
      case OriginalType.UINT_8 | OriginalType.INT_8 => SchemaBuilder.`type`(Schema.Type.INT8)
      case OriginalType.DATE => SchemaBuilder.`type`(Schema.Type.INT32)
      case _ => SchemaBuilder.`type`(Schema.Type.INT32)
    }

    def int64(original: OriginalType): SchemaBuilder = original match {
      case OriginalType.UINT_64 | OriginalType.INT_64 => SchemaBuilder.`type`(Schema.Type.INT64)
      case OriginalType.TIMESTAMP_MILLIS => SchemaBuilder.`type`(Schema.Type.INT64)
      case _ => SchemaBuilder.`type`(Schema.Type.INT64)
    }

    def binary(original: OriginalType): SchemaBuilder = original match {
      case OriginalType.ENUM => SchemaBuilder.`type`(Schema.Type.STRING)
      case OriginalType.UTF8 => SchemaBuilder.`type`(Schema.Type.STRING)
      case _ => SchemaBuilder.`type`(Schema.Type.BYTES)
    }

    val builder: SchemaBuilder = t.getPrimitiveTypeName match {
      case PrimitiveTypeName.BINARY => binary(t.getOriginalType)
      case PrimitiveTypeName.BOOLEAN => SchemaBuilder.`type`(Schema.Type.BOOLEAN)
      case PrimitiveTypeName.DOUBLE => SchemaBuilder.`type`(Schema.Type.FLOAT64)
      case PrimitiveTypeName.FLOAT => SchemaBuilder.`type`(Schema.Type.FLOAT32)
      case PrimitiveTypeName.INT32 => int32(t.getOriginalType)
      case PrimitiveTypeName.INT64 => int64(t.getOriginalType)
      case PrimitiveTypeName.INT96 => SchemaBuilder.`type`(Schema.Type.STRING)
      case other => throw UnsupportedSchemaType(s"Unsupported data type $other")
    }

    if (t.isRepetition(Type.Repetition.OPTIONAL)) builder.optional().build() else builder.required().build()
  }

  def toParquetMessage(struct: Schema, name: String): MessageType = {
    require(name != null, "name cannot be null")
    val types = struct.fields.asScala.map(field => toParquetType(field.schema(), field.name()))
    Types.buildMessage().addFields(types: _*).named(name)
  }

  def toParquetGroup(struct: Schema, name: String, repetition: Type.Repetition): GroupType = {
    val types = struct.fields.asScala.map(field => toParquetType(field.schema(), field.name()))
    Types.buildGroup(repetition).addFields(types: _*).named(name)
  }

  def toParquetType(schema: Schema, name: String): Type = {
    val repetition = if (schema.isOptional) Type.Repetition.OPTIONAL else Type.Repetition.REQUIRED
    schema.`type`() match {
      case Schema.Type.STRING => Types.primitive(PrimitiveTypeName.BINARY, repetition).as(OriginalType.UTF8).named(name)
      case Schema.Type.BOOLEAN => Types.primitive(PrimitiveTypeName.BOOLEAN, repetition).named(name)
      case Schema.Type.FLOAT32 => Types.primitive(PrimitiveTypeName.FLOAT, repetition).named(name)
      case Schema.Type.FLOAT64 => Types.primitive(PrimitiveTypeName.DOUBLE, repetition).named(name)
      case Schema.Type.INT8 => Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.INT_8).named(name)
      case Schema.Type.INT16 => Types.primitive(PrimitiveTypeName.INT32, repetition).as(OriginalType.INT_16).named(name)
      case Schema.Type.INT32 => Types.primitive(PrimitiveTypeName.INT32, repetition).named(name)
      case Schema.Type.INT64 => Types.primitive(PrimitiveTypeName.INT64, repetition).named(name)
      case Schema.Type.BYTES => Types.primitive(PrimitiveTypeName.BINARY, repetition).named(name)
      case Schema.Type.ARRAY => Types.list(repetition).element(toParquetType(schema.valueSchema(), "element")).named(name)
      case Schema.Type.STRUCT => toParquetGroup(schema, name, repetition)
      case other => throw UnsupportedSchemaType(s"Unsupported data type $other")
    }
  }
}
