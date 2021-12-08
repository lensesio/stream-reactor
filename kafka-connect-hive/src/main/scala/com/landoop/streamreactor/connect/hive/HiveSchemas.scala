package com.landoop.streamreactor.connect.hive

import org.apache.hadoop.hive.metastore.api.{FieldSchema, Table}
import org.apache.kafka.connect.data.{Field, Schema, SchemaBuilder}

import scala.jdk.CollectionConverters.ListHasAsScala


/**
  * Conversions to and from hive types into kafka connect types.
  *
  * hive types are taken from the language wiki here:
  * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types
  */
object HiveSchemas {

  private object HiveTypes {
    val string = "string"
    val array_r = "array<(.+)>".r
    val struct_r = "struct<(.+)>".r
    val struct_r_nested = "(.+):struct<(.+)>".r
    val struct_field_r = "(.+?)\\:(.+?)(,|$)".r
    val varchar_r = "varchar\\((.+?)\\)".r
    val char_r = "char\\((.+?)\\)".r
    val decimal_r = "decimal\\((.+?),(.+?)\\)".r
    val int = "int"
    val smallint = "smallint"
    val tinyint = "tinyint"
    val boolean = "boolean"
    val bigint = "bigint"
    val double = "double"
    val float = "float"
    val date = "date"
    val array = "array"
  }

  def toFieldSchemas(schema: Schema): Seq[FieldSchema] = {
    require(schema.`type`() == Schema.Type.STRUCT)
    schema.fields.asScala.map(toFieldSchema).toSeq
  }

  def toFieldSchema(field: Field): FieldSchema = new FieldSchema(field.name, toHiveType(field.schema), null)

  def toHiveType(schema: Schema): String = {
    schema.`type`() match {
      case Schema.Type.INT8 => HiveTypes.tinyint
      case Schema.Type.INT16 => HiveTypes.smallint
      case Schema.Type.INT32 => HiveTypes.int
      case Schema.Type.INT64 => HiveTypes.bigint
      case Schema.Type.BOOLEAN => HiveTypes.boolean
      case Schema.Type.FLOAT32 => HiveTypes.float
      case Schema.Type.FLOAT64 => HiveTypes.double
      case Schema.Type.STRING => HiveTypes.string
      case Schema.Type.ARRAY => s"${HiveTypes.array}<${toHiveType(schema.valueSchema)}>"
      case Schema.Type.STRUCT =>
        val fields_string = schema.fields.asScala.map { field => s"${field.name}:${toHiveType(field.schema())}" }.mkString(",")
        s"struct<$fields_string>"
      case _ => throw UnsupportedHiveTypeConversionException(s"Unknown data type ${schema.`type`}")
    }
  }

  def toKafka(table: Table): Schema = toKafka(table.getSd.getCols.asScala.toSeq, table.getPartitionKeys.asScala.toSeq, table.getTableName)

  def toKafka(cols: Seq[FieldSchema], partitionKeys: Seq[FieldSchema], name: String): Schema = {
    val builder = SchemaBuilder.struct.name(name).optional()

    // hive field columns are always nullable
    cols.foldLeft(builder) { (builder, col) =>
      builder.field(col.getName, toKafka(col.getType, col.getName, true))
    }
    // partition fields are always non-nullable
    partitionKeys.foldLeft(builder) { (builder, col) =>
      builder.field(col.getName, toKafka(col.getType, col.getName, false))
    }

    builder.build()
  }

  /**
   * Creates a kafka type from a hive field.
   */
  def toKafka(hiveType: String, fieldName: String, optional: Boolean): Schema = {
    val builder: SchemaBuilder = hiveType match {
      case HiveTypes.boolean => SchemaBuilder.bool()
      case HiveTypes.date => SchemaBuilder.int64()
      case HiveTypes.double => SchemaBuilder.float64()
      case HiveTypes.float => SchemaBuilder.float32()
      case HiveTypes.string => SchemaBuilder.string()
      case HiveTypes.bigint => SchemaBuilder.int64()
      case HiveTypes.tinyint => SchemaBuilder.int8()
      case HiveTypes.smallint => SchemaBuilder.int16()
      case HiveTypes.int => SchemaBuilder.int32()
      case HiveTypes.varchar_r(_) => SchemaBuilder.string()
      case HiveTypes.char_r(_) => SchemaBuilder.string()
      // todo encode decimals
      case HiveTypes.decimal_r(_, _) => SchemaBuilder.float64()
      case HiveTypes.array_r(element) => SchemaBuilder.array(toKafka(element.trim, fieldName, true))
      case HiveTypes.struct_r(columns) =>
        val builder = SchemaBuilder.struct//.name(fieldName)
        columns match {
          case HiveTypes.struct_r_nested(subkey, subcolumns) =>
            val subschemaBuilder = SchemaBuilder.struct//.name(subkey)
            for (m <- HiveTypes.struct_field_r.findAllMatchIn(subcolumns)) {
              subschemaBuilder.field(m.group(1).trim, toKafka(m.group(2).trim, m.group(1).trim, true))
            }
            val subschema = subschemaBuilder.optional().build()
            builder.field(subkey, subschema)

          case _ =>
            for (m <- HiveTypes.struct_field_r.findAllMatchIn(columns)) {
              builder.field(m.group(1).trim, toKafka(m.group(2).trim, m.group(1).trim, true))
            }
        }

        builder

      case _ =>
        throw UnsupportedHiveTypeConversionException(s"Unknown hive type $hiveType")
    }
    if (optional)
      builder.optional()
    builder.build
  }
}
