package com.datamountaineer.streamreactor.connect

import com.fasterxml.jackson.databind.JsonNode
import io.confluent.connect.avro.{AvroConverter, AvroData}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DecoderFactory, BinaryDecoder}
import org.apache.kafka.connect.json.{JsonConverter, JsonDeserializer}
import org.apache.kafka.connect.sink.SinkRecord

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
}
