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

/**
  * Created by tomasfartaria on 10/04/2017.
  */
object MqttSourceConfigConstants {
  val KCQL_CONFIG = "connect.mqtt.source.kcql"
  val HOSTS_CONFIG = "connect.mqtt.hosts"
  val QS_CONFIG = "connect.mqtt.service.quality"
  val USER_CONFIG = "connect.mqtt.user"
  val PASSWORD_CONFIG = "connect.mqtt.password"
  val CLIENT_ID_CONFIG = "connect.mqtt.client.id"
  val CONNECTION_TIMEOUT_CONFIG = "connect.mqtt.connection.timeout"
  val CLEAN_SESSION_CONFIG = "connect.mqtt.connection.clean"
  val KEEP_ALIVE_INTERVAL_CONFIG = "connect.mqtt.connection.keep.alive"
  val SSL_CA_CERT_CONFIG = "connect.mqtt.connection.ssl.ca.cert"
  val SSL_CERT_CONFIG = "connect.mqtt.connection.ssl.cert"
  val SSL_CERT_KEY_CONFIG = "connect.mqtt.connection.ssl.key"
  val CONVERTER_CONFIG = "connect.mqtt.source.converters"
  val THROW_ON_CONVERT_ERRORS_CONFIG = "connect.mqtt.converter.throw.on.error"

  private[config] val KCQL_DOC = "Contains the Kafka Connect Query Language describing the sourced MQTT source and the target Kafka topics"
  private[config] val KCQL_DISPLAY = "KCQL commands"
  private[config] val HOSTS_DOC = "Contains the MQTT connection end points."
  private[config] val HOSTS_DISPLAY = "Mqtt connection endpoints"
  private[config] val QS_DOC = "Specifies the Mqtt quality of service"
  private[config] val QS_DISPLAY = "he Quality of Service (QoS) level is an agreement between sender and receiver of a message regarding the guarantees of delivering a message. There are 3 QoS levels in MQTT: 0 = At most once; 1 = At least once; 2 = Exactly once"
  private[config] val USER_DOC = "Contains the Mqtt connection user name"
  private[config] val USER_DISPLAY = "Username"
  private[config] val PASSWORD_DOC = "Contains the Mqtt connection password"
  private[config] val PASSWORD_DISPLAY = "Password"
  private[config] val CLIENT_ID_DOC = "Contains the Mqtt session client id"
  private[config] val CLIENT_ID_DISPLAY = "Client id"
  private[config] val CONNECTION_TIMEOUT_DOC = "Provides the time interval to establish the mqtt connection"
  private[config] val CONNECTION_TIMEOUT_DISPLAY = "Connection timeout"
  private[config] val CONNECTION_TIMEOUT_DEFAULT = 3000
  private[config] val CLEAN_CONNECTION_DISPLAY = "Clean session"
  private[config] val CLEAN_CONNECTION_DEFAULT = true
  private[config] val KEEP_ALIVE_INTERVAL_DOC = "The keep alive functionality assures that the connection is still open and both broker and client are connected to one another. Therefore the client specifies a time interval in seconds and communicates it to the broker during the establishment of the connection. The interval is the longest possible period of time, which broker and client can endure without sending a message."
  private[config] val KEEP_ALIVE_INTERVAL_DISPLAY = "Keep alive interval"
  private[config] val KEEP_ALIVE_INTERVAL_DEFAULT = 5000
  private[config] val SSL_CA_CERT_DOC = "Provides the path to the CA certificate file to use with the Mqtt connection"
  private[config] val SSL_CA_CERT_DISPLAY = "CA certificate file path"
  private[config] val SSL_CERT_DOC = "Provides the path to the certificate file to use with the Mqtt connection"
  private[config] val SSL_CERT_DISPLAY = "Certificate key file path"
  private[config] val SSL_CERT_KEY_DOC = "Certificate private [config] key file path."
  private[config] val SSL_CERT_KEY_DISPLAY = "Certificate private [config] key file path"
  private[config] val CONVERTER_DOC =
    """Contains a tuple (Mqtt source topic and the canonical class name for the converter of a raw Mqtt message bytes to a SourceRecord).
      |If the source topic is not matched it will default to the BytesConverter
      |i.e. $mqtt_source1=com.datamountaineer.streamreactor.connect.source.converters.AvroConverter;$mqtt_source2=com.datamountaineer.streamreactor.connect.source.converters.JsonConverter""".stripMargin
  private[config] val CONVERTER_DISPLAY = "Converter class"
  private[config] val THROW_ON_CONVERT_ERRORS_DOC = "If set to false the conversion exception will be swallowed and everything carries on BUT the message is lost!!; true will throw the exception.Default is false."
  private[config] val THROW_ON_CONVERT_ERRORS_DISPLAY = "Throw error on conversion"
  private[config] val THROW_ON_CONVERT_ERRORS_DEFAULT = false

  val PROGRESS_COUNTER_ENABLED = "connect.progress.enabled"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"
}
