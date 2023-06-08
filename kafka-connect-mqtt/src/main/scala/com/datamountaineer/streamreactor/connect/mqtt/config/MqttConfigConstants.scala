/*
 * Copyright 2017-2023 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datamountaineer.streamreactor.connect.mqtt.config

import com.datamountaineer.streamreactor.common.config.base.const.TraitConfigConst._

/**
  * Created by tomasfartaria on 10/04/2017.
  */
object MqttConfigConstants {

  val CONNECTOR_PREFIX = "connect.mqtt"

  val KCQL_CONFIG = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC =
    "Contains the Kafka Connect Query Language describing the sourced MQTT source and the target Kafka topics"
  val KCQL_DISPLAY = "KCQL commands"

  val HOSTS_CONFIG  = s"$CONNECTOR_PREFIX.$CONNECTION_HOSTS_SUFFIX"
  val HOSTS_DOC     = "Contains the MQTT connection end points."
  val HOSTS_DISPLAY = "Mqtt connection endpoints"

  val QS_CONFIG = s"$CONNECTOR_PREFIX.service.quality"
  val QS_DOC    = "Specifies the Mqtt quality of service"
  val QS_DISPLAY =
    """
      |The Quality of Service (QoS) level is an agreement between sender and receiver of a message
      |regarding the guarantees of delivering a message. There are 3 QoS levels in MQTT: 0 = At most once;
      |1 = At least once; 2 = Exactly once
    """.stripMargin

  val RM_CONFIG  = s"${CONNECTOR_PREFIX}.retained.messages"
  val RM_DOC     = "Specifies the Mqtt retained flag."
  val RM_DEFAULT = false

  val USER_CONFIG  = s"$CONNECTOR_PREFIX.$USERNAME_SUFFIX"
  val USER_DOC     = "Contains the Mqtt connection user name"
  val USER_DISPLAY = "Username"

  val PASSWORD_CONFIG  = s"$CONNECTOR_PREFIX.$PASSWORD_SUFFIX"
  val PASSWORD_DOC     = "Contains the Mqtt connection password"
  val PASSWORD_DISPLAY = "Password"

  val CLIENT_ID_CONFIG  = s"$CONNECTOR_PREFIX.client.id"
  val CLIENT_ID_DOC     = "Contains the Mqtt session client id"
  val CLIENT_ID_DISPLAY = "Client id"

  val CONNECTION_TIMEOUT_CONFIG  = s"$CONNECTOR_PREFIX.timeout"
  val CONNECTION_TIMEOUT_DOC     = "Provides the time interval to establish the mqtt connection"
  val CONNECTION_TIMEOUT_DISPLAY = "Connection timeout"
  val CONNECTION_TIMEOUT_DEFAULT = 3000

  val POLLING_TIMEOUT_CONFIG  = s"$CONNECTOR_PREFIX.polling.timeout"
  val POLLING_TIMEOUT_DOC     = "Provides the timeout to poll incoming messages"
  val POLLING_TIMEOUT_DISPLAY = "Polling timeout"
  val POLLING_TIMEOUT_DEFAULT = 1000

  val CLEAN_SESSION_CONFIG     = s"$CONNECTOR_PREFIX.clean"
  val CLEAN_CONNECTION_DISPLAY = "Clean session"
  val CLEAN_CONNECTION_DEFAULT = true

  val KEEP_ALIVE_INTERVAL_CONFIG = s"$CONNECTOR_PREFIX.keep.alive"
  val KEEP_ALIVE_INTERVAL_DOC =
    """
      | The keep alive functionality assures that the connection is still open and both broker and client are connected to
      | the broker during the establishment of the connection. The interval is the longest possible period of time,
      | which broker and client can endure without sending a message.
    """.stripMargin
  val KEEP_ALIVE_INTERVAL_DISPLAY = "Keep alive interval"
  val KEEP_ALIVE_INTERVAL_DEFAULT = 5000

  val SSL_CA_CERT_CONFIG  = s"$CONNECTOR_PREFIX.ssl.ca.cert"
  val SSL_CA_CERT_DOC     = "Provides the path to the CA certificate file to use with the Mqtt connection"
  val SSL_CA_CERT_DISPLAY = "CA certificate file path"

  val SSL_CERT_CONFIG  = s"$CONNECTOR_PREFIX.ssl.cert"
  val SSL_CERT_DOC     = "Provides the path to the certificate file to use with the Mqtt connection"
  val SSL_CERT_DISPLAY = "Certificate key file path"

  val SSL_CERT_KEY_CONFIG  = s"$CONNECTOR_PREFIX.ssl.key"
  val SSL_CERT_KEY_DOC     = "Certificate private [config] key file path."
  val SSL_CERT_KEY_DISPLAY = "Certificate private [config] key file path"

  val THROW_ON_CONVERT_ERRORS_CONFIG = s"$CONNECTOR_PREFIX.converter.throw.on.error"
  val THROW_ON_CONVERT_ERRORS_DOC =
    """
      | If set to false the conversion exception will be swallowed and everything carries on BUT the message is lost!!;
      | true will throw the exception.Default is false.
    """.stripMargin

  val THROW_ON_CONVERT_ERRORS_DISPLAY = "Throw error on conversion"
  val THROW_ON_CONVERT_ERRORS_DEFAULT = false

  val AVRO_CONVERTERS_SCHEMA_FILES = "connect.converter.avro.schemas"
  val AVRO_CONVERTERS_SCHEMA_FILES_DOC =
    "If the AvroConverter is used you need to provide an avro Schema to be able to read and translate the raw bytes to an avro record. The format is $MQTT_TOPIC=$PATH_TO_AVRO_SCHEMA_FILE in case of source converter, or $KAFKA_TOPIC=PATH_TO_AVRO_SCHEMA in case of sink converter"
  val AVRO_CONVERTERS_SCHEMA_FILES_DEFAULT = ""

  val REPLICATE_SHARED_SUBSCIRPTIONS_CONFIG = s"$CONNECTOR_PREFIX.share.replicate"
  val REPLICATE_SHARED_SUBSCIRPTIONS_DOC =
    "Replicate the shared subscriptions to all tasks instead of distributing them"
  val REPLICATE_SHARED_SUBSCIRPTIONS_DEFAULT = false
  val REPLICATE_SHARED_SUBSCIRPTIONS_DISPLAY = "Activate shared subscriptions replication"

  val PROGRESS_COUNTER_ENABLED         = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC     = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val ERROR_POLICY = s"$CONNECTOR_PREFIX.$ERROR_POLICY_PROP_SUFFIX"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data.
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL         = s"$CONNECTOR_PREFIX.$RETRY_INTERVAL_PROP_SUFFIX"
  val ERROR_RETRY_INTERVAL_DOC     = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES               = s"$CONNECTOR_PREFIX.max.retries"
  val NBR_OF_RETRIES_DOC           = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT       = 20

  val LOG_MESSAGE_ARRIVED_KEY     = s"$CONNECTOR_PREFIX.log.message"
  val LOG_MESSAGE_ARRIVED_DISPLAY = "Logs received MQTT messages"
  val LOG_MESSAGE_ARRIVED_DEFAULT = false

  val PROCESS_DUPES_CONFIG = s"$CONNECTOR_PREFIX.process.duplicates"
  val PROCESS_DUPES_DOC =
    "Process MQTT messages that are marked as duplicates"
  val PROCESS_DUPES_DISPLAY = "Process Duplicates"

}
