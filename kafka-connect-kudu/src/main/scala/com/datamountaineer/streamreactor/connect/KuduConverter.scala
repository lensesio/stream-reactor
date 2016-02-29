package com.datamountaineer.streamreactor.connect

import com.datamountaineer.streamreactor.connect.utils.ConverterUtil
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.sink.SinkRecord
import org.kududb.client.PartialRow


trait KuduConverter extends ConverterUtil {
  /**
    * Convert SinkRecord type to Kudu and add the column to the Kudu row
    *
    * @param fieldType Type of SinkRecord field
    * @param fieldName Name of SinkRecord field
    * @param record    The SinkRecord
    * @param row       The Kudu row to add the field to
    * @return the updated Kudu row
    **/
   def convertTypeAndAdd(fieldType: Type, fieldName: String, record: SinkRecord, row: PartialRow): PartialRow = {
    val avro = convertToGenericAvro(record)
    fieldType match {
      case Type.STRING => row.addString(fieldName, avro.get(fieldName).toString)
      case Type.INT8 => row.addByte(fieldName, avro.get(fieldName).asInstanceOf[Byte])
      case Type.INT16 => row.addShort(fieldName, avro.get(fieldName).asInstanceOf[Short])
      case Type.INT32 => row.addInt(fieldName, avro.get(fieldName).asInstanceOf[Int])
      case Type.INT64 => row.addLong(fieldName, avro.get(fieldName).asInstanceOf[Long])
      case Type.BOOLEAN => row.addBoolean(fieldName, avro.get(fieldName).asInstanceOf[Boolean])
      case Type.FLOAT32 | Type.FLOAT64 => row.addFloat(fieldName, avro.get(fieldName).asInstanceOf[Float])
      case Type.BYTES => row.addBinary(fieldName, avro.get(fieldName).asInstanceOf[Array[Byte]])
      case _ => throw new UnsupportedOperationException(s"Unknown type $fieldType")
    }
    row
  }
}
