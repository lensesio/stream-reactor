package com.datamountaineer.streamreactor.connect.utils

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.slf4j.StrictLogging
import io.confluent.connect.avro.AvroData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.json.{JsonConverter, JsonDeserializer}
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.storage.Converter

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

/**
  * Created by andrew@datamountaineer.com on 22/02/16. 
  * stream-reactor
  */

trait ConverterUtil extends StrictLogging {
  lazy val jsonConverter = new JsonConverter()
  lazy val deserializer = new JsonDeserializer()
  lazy val avroData = new AvroData(100)

  /**
    * Convert a SinkRecords value to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  def convertValueToJson(record: SinkRecord) : JsonNode = {
    val converted: Array[Byte] = jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value())
    deserializeToJson(record.topic(), payload = converted)
  }

  /**
    * Convert a SinkRecords key to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  def convertKeyToJson(record: SinkRecord) : JsonNode = {
    val converted = jsonConverter.fromConnectData(record.topic(), record.keySchema(), record.key())
    deserializeToJson(record.topic(), payload = converted)
  }

  /**
    * Deserialize Byte array for a topic to json
    *
    * @param topic Topic name for the byte array
    * @param payload Byte Array payload
    * @return A JsonNode representing the byte array
    *
    * */
  def deserializeToJson(topic: String, payload: Array[Byte]) : JsonNode = {
    val json = deserializer.deserialize(topic, payload).get("payload")
    logger.debug(s"Converted to $json.")
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
    val avro = avroData.fromConnectData(record.valueSchema(), record.value())
    avro.asInstanceOf[GenericRecord]
  }
}
