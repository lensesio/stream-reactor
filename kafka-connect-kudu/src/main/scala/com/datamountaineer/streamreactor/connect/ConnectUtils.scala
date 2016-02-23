package com.datamountaineer.streamreactor.connect

import com.fasterxml.jackson.databind.JsonNode
import org.apache.kafka.connect.json.{JsonConverter, JsonDeserializer}
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.JavaConverters._
import scala.collection.immutable.HashMap

class ConnectUtils extends Logging {
  private val converter = new JsonConverter()
  //initialize the converter cache
  converter.configure(new HashMap[String, String].asJava, false)
  private val deserializer = new JsonDeserializer()

  /**
    * Convert a SinkRecords value to a Json string using Kafka Connects deserializer
    *
    * @param record A SinkRecord to extract the payload value from
    * @return A json string for the payload of the record
    * */
  def convertValueToJson(record: SinkRecord) : JsonNode = {
    val converted: Array[Byte] = converter.fromConnectData(record.topic(), record.valueSchema(), record.value())
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
  def convertKeyToJson(record: SinkRecord) : String = {
    val converted: Array[Byte] = converter.fromConnectData(record.topic(), record.keySchema(), record.key())
    val json = deserializer.deserialize(record.topic(), converted).get("key").toString
    log.debug(s"Converted key to $json.")
    json
  }
}
