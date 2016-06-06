/**
  * Copyright 2016 Datamountaineer.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  **/

package com.datamountaineer.streamreactor.connect.bloomberg

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}


class ConnectorConfig(map: java.util.Map[String, String]) extends AbstractConfig(ConnectorConfig.config, map)

object ConnectorConfig {
  val SERVER_HOST = "connect.bloomberg.server.host"
  val SERVER_HOST_DOC = "The hostname running the bloomberg service"

  val SERVER_PORT = "connect.bloomberg.server.port"
  val SERVER_PORT_DOC = "The port on which the bloomberg service runs (8124-is the default)"

  val SERVICE_URI = "connect.bloomberg.service.uri"
  val SERVICE_URI_DOC = "The Bloomberg service type: Market Data(//blp/mkdata);Reference Data(//blp/refdata)"

  val AUTHENTICATION_MODE = "connect.bloomberg.authentication.mode"
  val AUTHENTICATION_MODE_DOC = "Optional parameter setting how the authentication should be done. " +
    "It can be APPLICATION_ONLY or USER_AND_APPLICATION. Follow the Bloomberg API documentation for how to configure this"

  val SUBSCRIPTIONS = "connect.bloomberg.subscriptions"
  val SUBSCRIPTION_DOC = "Provides the list of securities and the fields to subscribe to. " +
    "Example: \"AAPL US Equity:LAST_PRICE,BID,ASK;IBM US Equity:BID,ASK,HIGH,LOW,OPEN\""

  val KAFKA_TOPIC: String = "connect.bloomberg.kafka.topic"
  val KAFKA_TOPIC_DOC: String = "The name of the kafka topic on which the data from Bloomberg will be sent."

  val BUFFER_SIZE = "connect.bloomberg.buffer.size"
  val BUFFER_SIZE_DOC = "Specifies how big is the queue to hold the updates received from Bloomberg. If the buffer is full" +
    "it won't accept new items until it is drained."

  val PAYLOAD_TYPE = "connect.bloomberg.payload.type"
  val PAYLOAD_TYPE_DOC = "Specifies the way the information is serialized and sent over kafka. There are two modes supported: json(default) and avro."

  val BloombergServicesUris = Set("//blp/mkdata", "//blp/refdata")
  val PayloadTypes = Set("json", "avro")


  val EXPORT_ROUTE_QUERY = "connect.hbase.export.route.query"
  val EXPORT_ROUTE_QUERY_DOC = ""

  val ERROR_POLICY = "connect.hbase.error.policy"
  val ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically";
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.hbase.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.hbase.max.retires"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

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
