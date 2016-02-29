package com.datamountaineer.streamreactor.connect.utils

import com.fasterxml.jackson.databind.JsonNode
import io.confluent.connect.avro.{AvroData, AvroConverter}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.connect.json.{JsonDeserializer, JsonConverter}
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.storage.Converter

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */

trait ConverterUtil extends Logging {
  lazy val jsonConverter = new JsonConverter()
  lazy val deserializer = new JsonDeserializer()
  lazy val avroConverter = new AvroConverter()
  lazy val avroData = new AvroData(100)

  /**
    * Convert a SinkRecords value to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  def convertValueToJson(record: SinkRecord) : JsonNode = {
    val converted: Array[Byte] = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value())
    val json = deserializer.deserialize(record.topic(), converted).get("payload")
    log.debug(s"Converted payload to $json.")
    json
  }

  /**
    * Convert a SinkRecords key to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  def convertKeyToJson(record: SinkRecord) : JsonNode = {
    val converted: Array[Byte] = jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key())
    val json = deserializer.deserialize(record.topic(), converted).get("key")
    log.debug(s"Converted key to $json.")
    json
  }

  /**
    * Configure the converter
    *
    * @param converter The Converter to configure
    * @param props The props to configure with
    *
    * */
  def configureConverter(converter: Converter, props: HashMap[String, String] = new HashMap[String, String] ) = {
    converter.configure(props.asJava, false)
  }

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

}
