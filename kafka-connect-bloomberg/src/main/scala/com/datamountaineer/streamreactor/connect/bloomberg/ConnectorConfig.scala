package com.datamountaineer.streamreactor.connect.bloomberg

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}


class ConnectorConfig(map: java.util.Map[String, String]) extends AbstractConfig(ConnectorConfig.config, map)

object ConnectorConfig {
  val SERVER_HOST = "server.host"
  val SERVER_HOST_DOC = "The hostname running the bloomberg service"

  val SERVER_PORT = "server.port"
  val SERVER_PORT_DOC = "The port on which the bloomberg service runs (8124-is the default)"

  val SERVICE_URI = "service.uri"
  val SERVICE_URI_DOC = "The Bloomberg service type: Market Data(//blp/mkdata);Reference Data(//blp/refdata)"

  val AUTHENTICATION_MODE = "authentication.mode"
  val AUTHENTICATION_MODE_DOC = "Optional parameter setting how the authentication should be done. " +
    "It can be APPLICATION_ONLY or USER_AND_APPLICATION. Follow the Bloomberg API documentation for how to configure this"

  val SUBSCRIPTIONS = "bloomberg.subscriptions"
  val SUBSCRIPTION_DOC = "Provides the list of securities and the fields to subscribe to. " +
    "Example: \"AAPL US Equity:LAST_PRICE,BID,ASK;IBM US Equity:BID,ASK,HIGH,LOW,OPEN\""

  val KAFKA_TOPIC: String = "kafka.topic"
  val KAFKA_TOPIC_DOC: String = "The name of the kafka topic on which the data from Bloomberg will be sent."

  val BUFFER_SIZE = "buffer.size"
  val BUFFER_SIZE_DOC = "Specifies how big is the queue to hold the updates received from Bloomberg. If the buffer is full" +
    "it won't accept new items until it is drained."

  val PAYLOAD_TYPE = "payload.type"
  val PAYLOAD_TYPE_DOC = "Specifies the way the information is serialized and sent over kafka. There are two modes supported: json(default) and avro."

  val BloombergServicesUris = Set("//blp/mkdata", "//blp/refdata")
  val PayloadTypes = Set("json", "avro")

  lazy val config = new ConfigDef()
    .define(SERVER_HOST, Type.STRING, Importance.HIGH, SERVER_HOST_DOC)
    .define(SERVER_PORT, Type.INT, Importance.HIGH, SERVER_PORT_DOC)
    .define(SERVICE_URI, Type.STRING, Importance.HIGH, SERVICE_URI_DOC)
    .define(SUBSCRIPTIONS, Type.STRING, Importance.HIGH, SUBSCRIPTION_DOC)
    .define(AUTHENTICATION_MODE, Type.STRING, Importance.LOW, AUTHENTICATION_MODE_DOC)
    .define(KAFKA_TOPIC, Type.STRING, Importance.HIGH, KAFKA_TOPIC_DOC)
    .define(BUFFER_SIZE, Type.INT, Importance.MEDIUM, BUFFER_SIZE_DOC)
    .define(PAYLOAD_TYPE, Type.STRING, Importance.MEDIUM, PAYLOAD_TYPE_DOC)
}