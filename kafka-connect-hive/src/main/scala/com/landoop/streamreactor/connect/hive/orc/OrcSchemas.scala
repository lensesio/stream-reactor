package com.landoop.streamreactor.connect.hive.orc

import com.landoop.streamreactor.connect.hive.UnsupportedSchemaType
import org.apache.kafka.connect.data.{Decimal, Schema, SchemaBuilder}
import org.apache.orc.TypeDescription
import org.apache.orc.TypeDescription.Category

import scala.jdk.CollectionConverters.ListHasAsScala


object OrcSchemas {

  def toKafka(schema: TypeDescription): Schema = schema.getCategory match {
    case Category.BOOLEAN => Schema.OPTIONAL_BOOLEAN_SCHEMA
    case Category.BYTE => Schema.OPTIONAL_INT8_SCHEMA
    case Category.DOUBLE => Schema.OPTIONAL_FLOAT64_SCHEMA
    case Category.INT => Schema.OPTIONAL_INT32_SCHEMA
    case Category.FLOAT => Schema.OPTIONAL_FLOAT32_SCHEMA
    case Category.LONG => Schema.OPTIONAL_INT64_SCHEMA
    case Category.SHORT => Schema.OPTIONAL_INT16_SCHEMA
    case Category.STRING => Schema.OPTIONAL_STRING_SCHEMA
    case Category.VARCHAR => Schema.OPTIONAL_STRING_SCHEMA
    case Category.CHAR => Schema.OPTIONAL_STRING_SCHEMA
    case Category.DATE => Schema.OPTIONAL_STRING_SCHEMA
    case Category.TIMESTAMP => Schema.OPTIONAL_STRING_SCHEMA
    case Category.STRUCT => toKafkaStruct(schema)
    case other => throw new IllegalStateException(s"No match for other $other in toKafka")
  }

  def toKafkaStruct(schema: TypeDescription): Schema = {
        val builder = SchemaBuilder.struct().name("from_orc")
    schema.getFieldNames.asScala.zipWithIndex.foreach { case (field, k) =>
      builder.field(field, toKafka(schema.getChildren.get(k)))
    }
    builder.build()
  }

  def toOrc(schema: Schema): TypeDescription = {
    schema.`type`() match {
      case Schema.Type.STRING if schema.name() == Decimal.LOGICAL_NAME => TypeDescription.createDecimal()
      case Schema.Type.STRING => TypeDescription.createString()
      case Schema.Type.BOOLEAN => TypeDescription.createBoolean()
      case Schema.Type.FLOAT32 => TypeDescription.createFloat()
      case Schema.Type.FLOAT64 => TypeDescription.createDouble()
      case Schema.Type.INT8 => TypeDescription.createByte()
      case Schema.Type.INT16 => TypeDescription.createShort()
      case Schema.Type.INT32 => TypeDescription.createInt()
      case Schema.Type.INT64 => TypeDescription.createLong()
      case Schema.Type.BYTES if schema.name() == Decimal.LOGICAL_NAME => TypeDescription.createDecimal()
      case Schema.Type.BYTES => TypeDescription.createBinary()
      case Schema.Type.ARRAY => TypeDescription.createList(toOrc(schema.valueSchema()))
      case Schema.Type.MAP => TypeDescription.createMap(toOrc(schema.keySchema()), toOrc(schema.valueSchema()))
      case Schema.Type.STRUCT =>
        schema.fields().asScala.foldLeft(TypeDescription.createStruct) { case (struct, field) =>
          struct.addField(field.name, toOrc(field.schema))
        }
      case unsupportedDataType => throw UnsupportedSchemaType(unsupportedDataType.toString)
    }
  }
}