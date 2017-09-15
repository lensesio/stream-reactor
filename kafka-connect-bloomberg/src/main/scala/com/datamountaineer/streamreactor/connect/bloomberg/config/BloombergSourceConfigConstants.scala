/*
 * Copyright 2017 Datamountaineer.
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
 */

package com.datamountaineer.streamreactor.connect.bloomberg.config

object BloombergSourceConfigConstants {

  val SERVER_HOST = "connect.bloomberg.server.host"
  val SERVER_HOST_DOC = "The hostname running the bloomberg service"

  val SERVER_PORT = "connect.bloomberg.server.port"
  val SERVER_PORT_DOC = "The port on which the bloomberg service runs (8124-is the default)"

  val SERVICE_URI = "connect.bloomberg.service.uri"
  val SERVICE_URI_DOC = "The Bloomberg service type: Market Data(//blp/mktdata);Reference Data(//blp/refdata)"

  val AUTHENTICATION_MODE = "connect.bloomberg.authentication.mode"
  val AUTHENTICATION_MODE_DOC: String = "Optional parameter setting how the authentication should be done. " +
    "It can be APPLICATION_ONLY or USER_AND_APPLICATION. Follow the Bloomberg API documentation for how to configure this"

  val SUBSCRIPTIONS = "connect.bloomberg.subscriptions"
  val SUBSCRIPTION_DOC: String = "Provides the list of securities and the fields to subscribe to. " +
    "Example: \"AAPL US Equity:LAST_PRICE,BID,ASK;IBM US Equity:BID,ASK,HIGH,LOW,OPEN\""

  val KAFKA_TOPIC: String = "connect.bloomberg.kafka.topic"
  val KAFKA_TOPIC_DOC: String = "The name of the kafka topic on which the data from Bloomberg will be sent."

  val BUFFER_SIZE = "connect.bloomberg.buffer.size"
  val BUFFER_SIZE_DOC: String = "Specifies how big is the queue to hold the updates received from Bloomberg. If the buffer is full" +
    "it won't accept new items until it is drained."

  val PAYLOAD_TYPE = "connect.bloomberg.payload.type"
  val PAYLOAD_TYPE_DOC: String = "Specifies the way the information is serialized and sent over kafka. " +
    "There are two modes supported: json(default) and avro."

  val BloombergServicesUris = Set("//blp/mktdata", "//blp/refdata")
  val PayloadTypes = Set("json", "avro")


  val EXPORT_ROUTE_QUERY = "connect.hbase.sink.kcql"
  val EXPORT_ROUTE_QUERY_DOC = ""

  val ERROR_POLICY = "connect.bloomberg.error.policy"
  val ERROR_POLICY_DOC: String = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically"
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.bloomberg.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.bloomberg.max.retries"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

}
