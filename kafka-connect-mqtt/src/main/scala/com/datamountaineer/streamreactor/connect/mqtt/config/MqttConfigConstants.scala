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

package com.datamountaineer.streamreactor.connect.mqtt.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst._

/**
  * Created by tomasfartaria on 10/04/2017.
  */
object MqttConfigConstants {

  val CONNECTOR_PREFIX = "connect.mqtt"
  
  val KCQL_CONFIG = s"${CONNECTOR_PREFIX}.${KCQL_PROP_SUFFIX}"
  val KCQL_DOC = "Contains the Kafka Connect Query Language describing the sourced MQTT source and the target Kafka topics"
  val KCQL_DISPLAY = "KCQL commands"

  val HOSTS_CONFIG = s"${CONNECTOR_PREFIX}.${CONNECTION_HOSTS_SUFFIX}"
  val HOSTS_DOC = "Contains the MQTT connection end points."
  val HOSTS_DISPLAY = "Mqtt connection endpoints"

  val QS_CONFIG = s"${CONNECTOR_PREFIX}.service.quality"
  val QS_DOC = "Specifies the Mqtt quality of service"
  val QS_DISPLAY =
    """
      |The Quality of Service (QoS) level is an agreement between sender and receiver of a message
      |regarding the guarantees of delivering a message. There are 3 QoS levels in MQTT: 0 = At most once;
      |1 = At least once; 2 = Exactly once
    """.stripMargin

  val USER_CONFIG = s"${CONNECTOR_PREFIX}.${USERNAME_SUFFIX}"
  val USER_DOC = "Contains the Mqtt connection user name"
  val USER_DISPLAY = "Username"

  val PASSWORD_CONFIG = s"${CONNECTOR_PREFIX}.${PASSWORD_SUFFIX}"
  val PASSWORD_DOC = "Contains the Mqtt connection password"
  val PASSWORD_DISPLAY = "Password"

  val CLIENT_ID_CONFIG = s"${CONNECTOR_PREFIX}.client.id"
  val CLIENT_ID_DOC = "Contains the Mqtt session client id"
  val CLIENT_ID_DISPLAY = "Client id"

  val CONNECTION_TIMEOUT_CONFIG = s"${CONNECTOR_PREFIX}.connection.timeout"
  val CONNECTION_TIMEOUT_DOC = "Provides the time interval to establish the mqtt connection"
  val CONNECTION_TIMEOUT_DISPLAY = "Connection timeout"
  val CONNECTION_TIMEOUT_DEFAULT = 3000

  val CLEAN_SESSION_CONFIG = s"${CONNECTOR_PREFIX}.connection.clean"
  val CLEAN_CONNECTION_DISPLAY = "Clean session"
  val CLEAN_CONNECTION_DEFAULT = true

  val KEEP_ALIVE_INTERVAL_CONFIG = s"${CONNECTOR_PREFIX}.connection.keep.alive"
  val KEEP_ALIVE_INTERVAL_DOC =
    """
      | The keep alive functionality assures that the connection is still open and both broker and client are connected to
      | the broker during the establishment of the connection. The interval is the longest possible period of time,
      | which broker and client can endure without sending a message.
    """.stripMargin
  val KEEP_ALIVE_INTERVAL_DISPLAY = "Keep alive interval"
  val KEEP_ALIVE_INTERVAL_DEFAULT = 5000

  val SSL_CA_CERT_CONFIG = s"${CONNECTOR_PREFIX}.connection.ssl.ca.cert"
  val SSL_CA_CERT_DOC = "Provides the path to the CA certificate file to use with the Mqtt connection"
  val SSL_CA_CERT_DISPLAY = "CA certificate file path"

  val SSL_CERT_CONFIG = s"${CONNECTOR_PREFIX}.connection.ssl.cert"
  val SSL_CERT_DOC = "Provides the path to the certificate file to use with the Mqtt connection"
  val SSL_CERT_DISPLAY = "Certificate key file path"

  val SSL_CERT_KEY_CONFIG = s"${CONNECTOR_PREFIX}.connection.ssl.key"
  val SSL_CERT_KEY_DOC = "Certificate private [config] key file path."
  val SSL_CERT_KEY_DISPLAY = "Certificate private [config] key file path"

  val CONVERTER_CONFIG = s"${CONNECTOR_PREFIX}.converters"
  val CONVERTER_DOC =
    """Contains a tuple (Mqtt source topic and the canonical class name for the converter of a raw Mqtt message bytes to a SourceRecord).
      |If the source topic is not matched it will default to the BytesConverter
      |i.e. $mqtt_source1=com.datamountaineer.streamreactor.connect.source.converters.AvroConverter;$mqtt_source2=com.datamountaineer.streamreactor.connect.source.converters.JsonConverter""".stripMargin
  val CONVERTER_DISPLAY = "Converter class"

  val THROW_ON_CONVERT_ERRORS_CONFIG = s"${CONNECTOR_PREFIX}.converter.throw.on.error"
  val THROW_ON_CONVERT_ERRORS_DOC =
    """
      | If set to false the conversion exception will be swallowed and everything carries on BUT the message is lost!!;
      | true will throw the exception.Default is false.
    """.stripMargin

  val THROW_ON_CONVERT_ERRORS_DISPLAY = "Throw error on conversion"
  val THROW_ON_CONVERT_ERRORS_DEFAULT = false

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"
}
