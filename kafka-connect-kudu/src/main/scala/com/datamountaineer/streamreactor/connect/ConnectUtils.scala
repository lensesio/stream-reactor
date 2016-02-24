package com.datamountaineer.streamreactor.connect

import com.fasterxml.jackson.databind.JsonNode
import io.confluent.connect.avro.{AvroConverter, AvroData}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.json.{JsonConverter, JsonDeserializer}
import org.apache.kafka.connect.sink.SinkRecord
import org.kududb.client.PartialRow

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

class ConnectUtils extends Logging {
  private val jsonConverter = new JsonConverter()
  //initialize the converter cache
  jsonConverter.configure(new HashMap[String, String].asJava, false)
  private val jsonDeserializer = new JsonDeserializer()

  private val avroConverter = new AvroConverter()
  avroConverter.configure(new HashMap[String, String].asJava, false)
  private val avroData = new AvroData(100)


  /**
    * Convert SinkRecord to GenericRecord
    *
    * @param record SinkRecord to convert
    * @return a GenericRecord
    **/
  def convertToGenericAvro(record: SinkRecord): GenericRecord = {
    val obj = avroConverter.fromConnectData(record.topic(), record.valueSchema(), record.value())
    val avroSchema = avroData.fromConnectSchema(record.keySchema())
    val reader = new GenericDatumReader[GenericRecord](avroSchema)
    val decoder = DecoderFactory.get().binaryDecoder(obj, null)
    reader.read(null, decoder)
  }

  /**
    * Convert a SinkRecords value to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  def convertValueToJson(record: SinkRecord) : JsonNode = {
    val converted: Array[Byte] = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value())
    val json = jsonDeserializer.deserialize(record.topic(), converted).get("payload")
    log.debug(s"Converted payload to $json.")
    json
  }

  /**
    * Convert a SinkRecords key to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  def convertKeyToJson(record: SinkRecord) : String = {
    val converted: Array[Byte] = jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key())
    val json = jsonDeserializer.deserialize(record.topic(), converted).get("key").toString
    log.debug(s"Converted key to $json.")
    json
  }

  /**
    * Convert SinkRecord type to Kudu and add the column to the Kudu row
    *
    * @param fieldType Type of SinkRecord field
    * @param fieldName Name of SinkRecord field
    * @param record    The SinkRecord
    * @param row       The Kudu row to add the field tp
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
