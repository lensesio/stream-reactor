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

package com.datamountaineer.streamreactor.connect.jms.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst._

object JMSConfigConstants {
  
  val CONNECTOR_PREFIX = "connect.jms"

  val JMS_URL = s"${CONNECTOR_PREFIX}.${URL_SUFFIX}"
  private[config] val JMS_URL_DOC = "Provides the JMS broker url"

  val JMS_USER = s"${CONNECTOR_PREFIX}.${USERNAME_SUFFIX}"
  private[config] val JMS_USER_DOC = "Provides the user for the JMS connection"

  val JMS_PASSWORD = s"${CONNECTOR_PREFIX}.${PASSWORD_SUFFIX}"
  private[config] val JMS_PASSWORD_DOC = "Provides the password for the JMS connection"

  val INITIAL_CONTEXT_FACTORY = s"${CONNECTOR_PREFIX}.initial.context.factory"
  private[config] val INITIAL_CONTEXT_FACTORY_DOC = "Initial Context Factory, e.g: org.apache.activemq.jndi.ActiveMQInitialContextFactory"

  val CONNECTION_FACTORY = s"${CONNECTOR_PREFIX}.connection.factory"
  private[config] val CONNECTION_FACTORY_DOC = "Provides the full class name for the ConnectionFactory implementation to use, e.g" +
    "org.apache.activemq.ActiveMQConnectionFactory"
  val CONNECTION_FACTORY_DEFAULT = "ConnectionFactory"


  val KCQL = s"${CONNECTOR_PREFIX}.${KCQL_PROP_SUFFIX}"
  val KCQL_DOC =  "KCQL expression describing field selection and routes."

  val ERROR_POLICY = s"${CONNECTOR_PREFIX}.${ERROR_POLICY_PROP_SUFFIX}"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data.
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = s"${CONNECTOR_PREFIX}.${RETRY_INTERVAL_PROP_SUFFIX}"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  val NBR_OF_RETRIES = s"${CONNECTOR_PREFIX}.${MAX_RETRIES_PROP_SUFFIX}"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val AVRO_CONVERTERS_SCHEMA_FILES = "connect.converter.avro.schemas"
  val AVRO_CONVERTERS_SCHEMA_FILES_DOC = "If the AvroConverter is used you need to provide an avro Schema to be able to read and translate the raw bytes to an avro record. The format is $MQTT_TOPIC=$PATH_TO_AVRO_SCHEMA_FILE"
  val AVRO_CONVERTERS_SCHEMA_FILES_DEFAULT = ""

  val DEFAULT_CONVERTER_CONFIG = s"${CONNECTOR_PREFIX}.source.default.converter"
  private[config] val DEFAULT_CONVERTER_DOC =
    """
      |Contains a canonical class name for the default converter of a raw JMS message bytes to a SourceRecord.
      |Overrides to the default can be done by using connect.jms.source.converters still.
      |i.e. com.datamountaineer.streamreactor.connect.source.converters.AvroConverter""".stripMargin
  private[config] val DEFAULT_CONVERTER_DISPLAY = "Default Converter class"


  val THROW_ON_CONVERT_ERRORS_CONFIG = s"${CONNECTOR_PREFIX}.converter.throw.on.error"
  private[config] val THROW_ON_CONVERT_ERRORS_DOC = "If set to false the conversion exception will be swallowed and everything carries on BUT the message is lost!!; true will throw the exception.Default is false."
  private[config] val THROW_ON_CONVERT_ERRORS_DISPLAY = "Throw error on conversion"
  private[config] val THROW_ON_CONVERT_ERRORS_DEFAULT = false

  val DESTINATION_SELECTOR = s"${CONNECTOR_PREFIX}.destination.selector"
  val DESTINATION_SELECTOR_DOC = "Selector to use for destination lookup. Either CDI or JNDI."
  val DESTINATION_SELECTOR_DEFAULT = "CDI"

  val TOPIC_SUBSCRIPTION_NAME = s"${CONNECTOR_PREFIX}.subscription.name"
  val TOPIC_SUBSCRIPTION_NAME_DOC = "subscription name to use when subscribing to a topic, specifying this makes a durable subscription for topics"

  val EXTRA_PROPS = s"${CONNECTOR_PREFIX}.initial.context.extra.params"
  private[config] val EXTRA_PROPS_DOC = "List (comma separated) of extra properties as key/value pairs with a colon delimiter to " +
    "supply to the initial context e.g. SOLACE_JMS_VPN:my_solace_vp"
  private[config] val EXTRA_PROPS_DEFAULT = new java.util.ArrayList[String]

  val BATCH_SIZE = s"${CONNECTOR_PREFIX}.${BATCH_SIZE_PROP_SUFFIX}"
  private[config] val BATCH_SIZE_DOC = "The number of records to poll for on the target JMS destination in each Connect poll."
  private[config] val BATCH_SIZE_DEFAULT = 100

  val PROGRESS_COUNTER_ENABLED = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val POLLING_TIMEOUT_CONFIG = s"$CONNECTOR_PREFIX.polling.timeout"
  val POLLING_TIMEOUT_DOC = "Provides the timeout to poll incoming messages"
  val POLLING_TIMEOUT_DISPLAY = "Polling timeout"
  val POLLING_TIMEOUT_DEFAULT = 1000L
}
