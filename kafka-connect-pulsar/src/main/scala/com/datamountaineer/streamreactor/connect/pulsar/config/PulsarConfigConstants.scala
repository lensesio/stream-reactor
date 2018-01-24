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

package com.datamountaineer.streamreactor.connect.pulsar.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst._

/**
  * Created by tomasfartaria on 10/04/2017.
  */
object PulsarConfigConstants {

  val CONNECTOR_PREFIX = "connect.pulsar"
  
  val KCQL_CONFIG = s"$CONNECTOR_PREFIX.$KCQL_PROP_SUFFIX"
  val KCQL_DOC = "Contains the Kafka Connect Query Language describing the flow from Apache Pulsar to Apache Kafka topics"
  val KCQL_DISPLAY = "KCQL commands"

  val HOSTS_CONFIG = s"$CONNECTOR_PREFIX.$CONNECTION_HOSTS_SUFFIX"
  val HOSTS_DOC = "Contains the Pulsar connection end points."
  val HOSTS_DISPLAY = "Pulsar connection endpoints"

  val THROW_ON_CONVERT_ERRORS_CONFIG = s"$CONNECTOR_PREFIX.converter.throw.on.error"
  val THROW_ON_CONVERT_ERRORS_DOC =
    """
      | If set to false the conversion exception will be swallowed and everything carries on BUT the message is lost!!;
      | true will throw the exception.Default is false.
    """.stripMargin
  val THROW_ON_CONVERT_ERRORS_DISPLAY = "Throw error on conversion"
  val THROW_ON_CONVERT_ERRORS_DEFAULT = false

  val AVRO_CONVERTERS_SCHEMA_FILES = "connect.converter.avro.schemas"
  val AVRO_CONVERTERS_SCHEMA_FILES_DOC = "If the AvroConverter is used you need to provide an avro Schema to be able to read and translate the raw bytes to an avro record. The format is $MQTT_TOPIC=$PATH_TO_AVRO_SCHEMA_FILE"
  val AVRO_CONVERTERS_SCHEMA_FILES_DEFAULT = ""

  val POLLING_TIMEOUT_CONFIG = s"$CONNECTOR_PREFIX.polling.timeout"
  val POLLING_TIMEOUT_DOC = s"Provides the timeout to poll incoming messages. Connect will write to Kafka is this reached of the $CONNECTOR_PREFIX.$BATCH_SIZE_PROP_SUFFIX. Which ever is first."
  val POLLING_TIMEOUT_DISPLAY = "Polling timeout"
  val POLLING_TIMEOUT_DEFAULT = 1000

  val PROGRESS_COUNTER_ENABLED = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
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

  val ERROR_RETRY_INTERVAL = s"$CONNECTOR_PREFIX.$RETRY_INTERVAL_PROP_SUFFIX"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = s"$CONNECTOR_PREFIX.max.retries"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val INTERNAL_BATCH_SIZE = s"$CONNECTOR_PREFIX.$BATCH_SIZE_PROP_SUFFIX"
  val INTERNAL_BATCH_SIZE_DOC = s"The number of records Connect will wait before writing to Kafka. The connector will return if $CONNECTOR_PREFIX.polling.timeout is reached first."
  val INTERNAL_BATCH_SIZE_DEFAULT = 100

  val SSL_CA_CERT_CONFIG = s"${CONNECTOR_PREFIX}.tls.ca.cert"
  val SSL_CA_CERT_DOC = "Provides the path to the CA certificate file to use with the Pulsar connection"
  val SSL_CA_CERT_DISPLAY = "CA certificate file path"

  val SSL_CERT_CONFIG = s"${CONNECTOR_PREFIX}.tls.cert"
  val SSL_CERT_DOC = "Provides the path to the certificate file to use with the Pulsar connection"
  val SSL_CERT_DISPLAY = "Certificate key file path"

  val SSL_CERT_KEY_CONFIG = s"${CONNECTOR_PREFIX}.tls.key"
  val SSL_CERT_KEY_DOC = "Certificate private [config] key file path."
  val SSL_CERT_KEY_DISPLAY = "Certificate private [config] key file path"
}
