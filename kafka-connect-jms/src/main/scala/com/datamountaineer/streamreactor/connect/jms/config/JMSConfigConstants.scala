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

object JMSConfigConstants {
  val JMS_URL = "connect.jms.url"
  private[config] val JMS_URL_DOC = "Provides the JMS broker url"

  val JMS_USER = "connect.jms.user"
  private[config] val JMS_USER_DOC = "Provides the user for the JMS connection"

  val JMS_PASSWORD = "connect.jms.password"
  private[config] val JMS_PASSWORD_DOC = "Provides the password for the JMS connection"

  val INITIAL_CONTEXT_FACTORY = "connect.jms.initial.context.factory"
  private[config] val INITIAL_CONTEXT_FACTORY_DOC = "Initial Context Factory, e.g: org.apache.activemq.jndi.ActiveMQInitialContextFactory"

  val CONNECTION_FACTORY = "connect.jms.connection.factory"
  private[config] val CONNECTION_FACTORY_DOC = "Provides the full class name for the ConnectionFactory implementation to use, e.g" +
    "org.apache.activemq.ActiveMQConnectionFactory"

  val KCQL = "connect.jms.kcql"
  val KCQL_DOC =  "KCQL expression describing field selection and routes."

  val ERROR_POLICY = "connect.jms.error.policy"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data.
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.jms.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.jms.max.retries"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val CONVERTER_CONFIG = "connect.jms.source.converters"
  private[config] val CONVERTER_DOC =
    """
      |Contains a tuple (JMS source topic and the canonical class name for the converter of a raw JMS message bytes to a SourceRecord).
      |If the source topic is not matched it will default to the BytesConverter
      |i.e. $jms_source1=com.datamountaineer.streamreactor.connect.source.converters.AvroConverter;$jms_source2=com.datamountaineer.streamreactor.connect.source.converters.JsonConverter""".stripMargin
  private[config] val CONVERTER_DISPLAY = "Converter class"

  val THROW_ON_CONVERT_ERRORS_CONFIG = "connect.jms.converter.throw.on.error"
  private[config] val THROW_ON_CONVERT_ERRORS_DOC = "If set to false the conversion exception will be swallowed and everything carries on BUT the message is lost!!; true will throw the exception.Default is false."
  private[config] val THROW_ON_CONVERT_ERRORS_DISPLAY = "Throw error on conversion"
  private[config] val THROW_ON_CONVERT_ERRORS_DEFAULT = false

  val DESTINATION_SELECTOR = "connect.jms.destination.selector"
  val DESTINATION_SELECTOR_DOC = "Selector to use for destination lookup. Either CDI or JNDI."
  val DESTINATION_SELECTOR_DEFAULT = "CDI"

  val TOPIC_LIST = "connect.jms.topics"
  val TOPIC_LIST_DOC = "A comma separated list of JMS topics, must match the KCQL source or target JMS topics."

  val QUEUE_LIST = "connect.jms.queues"
  val QUEUE_LIST_DOC = "A comma separated list of JMS topics, must match the KCQL source or target JMS queues."

  val EXTRA_PROPS = "connect.jms.initial.context.extra.params"
  private[config] val EXTRA_PROPS_DOC = "List (comma separated) of extra properties as key/value pairs with a colon delimiter to " +
    "supply to the initial context e.g. SOLACE_JMS_VPN:my_solace_vp"
  private[config] val EXTRA_PROPS_DEFAULT = new java.util.ArrayList[String]

  val BATCH_SIZE = "connect.jms.batch.size"
  private[config] val BATCH_SIZE_DOC = "The number of records to poll for on the target JMS destination in each Connect poll."
  private[config] val BATCH_SIZE_DEFAULT = 100
}
