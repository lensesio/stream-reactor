package com.datamountaineer.streamreactor.connect

import com.datamountaineer.streamreactor.connect.utils.ConverterUtil
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.Field
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.sink.SinkRecord
import org.kududb.ColumnSchema.ColumnSchemaBuilder
import org.kududb.Schema

import scala.collection.JavaConverters._

//import org.kududb.Type
import org.kududb.client.{KuduTable, PartialRow}


trait KuduConverter extends ConverterUtil {

  /**
    * Convert SinkRecord type to Kudu and add the column to the Kudu row
    *
    * @param field SinkRecord Field
    * @param avroRecord Avro record
    * @param row The Kudu row to add the field to
    * @return the updated Kudu row
    **/
   private def addFieldToRow(field: Field, avroRecord: GenericRecord, row: PartialRow): PartialRow = {
    val fieldType = field.schema().`type`()
    val fieldName = field.name()
    val avro = avroRecord.get(fieldName)

    fieldType match {
      case Type.STRING => row.addString(fieldName, avro.toString)
      case Type.INT8 => row.addByte(fieldName, avro.asInstanceOf[Byte])
      case Type.INT16 => row.addShort(fieldName, avro.asInstanceOf[Short])
      case Type.INT32 => row.addInt(fieldName, avro.asInstanceOf[Int])
      case Type.INT64 => row.addLong(fieldName, avro.asInstanceOf[Long])
      case Type.BOOLEAN => row.addBoolean(fieldName, avro.asInstanceOf[Boolean])
      case Type.FLOAT32 | Type.FLOAT64 => row.addFloat(fieldName, avro.asInstanceOf[Float])
      case Type.BYTES => row.addBinary(fieldName, avro.asInstanceOf[Array[Byte]])
      case _ => throw new UnsupportedOperationException(s"Unknown type $fieldType")
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
    val avro = convertToGenericAvro(record)
    val fields = record.valueSchema().fields().asScala
    val insert = table.newInsert()
    val row = insert.getRow
    fields.map(f=>addFieldToRow(f, avro, row))
    insert
  }


  /**
    * Convert Connect Schema to Kudu
    *
    * @param record A sinkRecord to get the value schema from
    * */
  def convertToKuduSchema(record: SinkRecord) = {
    val connectFields = record.valueSchema().fields().asScala
    val kuduFields = connectFields.map(cf=>convertConnectField(cf)).asJava
    val schema = new Schema(kuduFields)
    schema
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
