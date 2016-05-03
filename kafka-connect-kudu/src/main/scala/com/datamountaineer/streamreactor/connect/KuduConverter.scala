package com.datamountaineer.streamreactor.connect

import com.datamountaineer.streamreactor.connect.utils.ConverterUtil
import org.apache.kafka.connect.data.{Field, Struct}
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.sink.SinkRecord
import org.kududb.ColumnSchema.ColumnSchemaBuilder

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.kududb.client.{KuduTable, PartialRow}


trait KuduConverter extends ConverterUtil {

  /**
    * Convert SinkRecord type to Kudu and add the column to the Kudu row
    *
    * @param field field of sink record
    * @param value data object
    * @param row the Kudu row to add the field to
    * @param prefix outer schema prefix
    * @return the updated Kudu row
    **/
   private def addFieldToRow(field: Field, value: Object, row: PartialRow, prefix: String = ""): PartialRow = {
    val schema = field.schema
    val fieldSchemaType = schema.`type`
    val fieldSchemaName = prefix + (if (prefix.nonEmpty) "_" else "") + field.name

    fieldSchemaType match {
      case Type.STRUCT =>
        val schemas: mutable.Buffer[Field] = schema.fields().asScala
        val valueStruct: Struct = value.asInstanceOf[Struct]
        val values = schemas.map(f=>valueStruct.get(f.name()))
        (schemas zip values).map(f=>addFieldToRow(f._1, f._2, row, fieldSchemaName.toLowerCase))
      case Type.STRING => row.addString(fieldSchemaName.toLowerCase, value.toString)
      case Type.INT8 => row.addByte(fieldSchemaName.toLowerCase, value.asInstanceOf[Byte])
      case Type.INT16 => row.addShort(fieldSchemaName.toLowerCase, value.asInstanceOf[Short])
      case Type.INT32 => row.addInt(fieldSchemaName.toLowerCase, value.asInstanceOf[Int])
      case Type.INT64 => row.addLong(fieldSchemaName.toLowerCase, value.asInstanceOf[Long])
      case Type.BOOLEAN => row.addBoolean(fieldSchemaName.toLowerCase, value.asInstanceOf[Boolean])
      case Type.FLOAT32 | Type.FLOAT64 => row.addFloat(fieldSchemaName.toLowerCase, value.asInstanceOf[Float])
      case Type.BYTES => row.addBinary(fieldSchemaName.toLowerCase, value.asInstanceOf[Array[Byte]])
      case _ => throw new UnsupportedOperationException(s"Unknown type $fieldSchemaType")
    }
    row
  }

  /**
    * Convert a SinkRecord to a Kudu row insert for a Kudu Table
 *
    * @param record A SinkRecord to convert
    * @param table A Kudu table to create a row insert for
    * @return A Kudu insert operation
    * */
  def convert(record: SinkRecord, table: KuduTable) = {
    val insert = table.newInsert
    val valueSchema = record.valueSchema
    val fieldSchemas = valueSchema.fields.asScala
    val value = record.value.asInstanceOf[Struct]
    val fieldValues = fieldSchemas.map(f=>value.get(f.name()))
    val row = insert.getRow

    configureConverter(jsonConverter)
    val valueJson = convertValueToJson(record)
    (fieldSchemas zip fieldValues).map(f=>addFieldToRow(f._1, f._2, row))
    insert
  }

  /**
    * Convert Connect Schema to Kudu
    *
    * @param record A sinkRecord to get the value schema from
    * */
  def convertToKuduSchema(record: SinkRecord) = {
    val connectFields = record.valueSchema().fields().asScala
    val kuduFields = connectFields.flatMap(cf=>flattenIfRequired(cf)).map(cf=>convertConnectField(cf)).asJava
    val schema = new org.kududb.Schema(kuduFields)
    schema
  }

  def flattenIfRequired(field: Field): List[Field] = {
    if (field.schema.`type`.getName.equals(Type.STRUCT.getName)) {
      field.schema.fields.asScala.flatMap(f=>flattenIfRequired(f)).toList
    } else
      List(field)
  }

  /**
    * Convert a connect schema field to a Kudu field
    *
    * @param field The Connect field to convert
    * @return The equivalent Kudu type
    * */
  def convertConnectField(field: Field) = {
    val fieldType = field.schema().`type`()
    val fieldName = field.name()
    val kudu = fieldType match {
        case Type.STRING => new ColumnSchemaBuilder(fieldName, org.kududb.Type.STRING)
        case Type.INT8 => new ColumnSchemaBuilder(fieldName, org.kududb.Type.INT8)
        case Type.INT16 => new ColumnSchemaBuilder(fieldName, org.kududb.Type.INT16)
        case Type.INT32 => new ColumnSchemaBuilder(fieldName, org.kududb.Type.INT32)
        case Type.INT64 => new ColumnSchemaBuilder(fieldName, org.kududb.Type.INT64)
        case Type.BOOLEAN => new ColumnSchemaBuilder(fieldName, org.kududb.Type.BOOL)
        case Type.FLOAT32 | Type.FLOAT64 => new ColumnSchemaBuilder(fieldName, org.kududb.Type.FLOAT)
        case Type.BYTES => new ColumnSchemaBuilder(fieldName, org.kududb.Type.BINARY)
        case _ => throw new UnsupportedOperationException(s"Unknown type $fieldType")
      }
    kudu.build()
  }
}
