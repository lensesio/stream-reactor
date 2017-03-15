/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.jms.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object JMSConfig {

  val JMS_URL = "connect.jms.url"
  private val JMS_URL_DOC = "Provides the JMS broker url"

  val JMS_USER = "connect.jms.user"
  private val JMS_USER_DOC = "Provides the user for the JMS connection"

  val JMS_PASSWORD = "connect.jms.password"
  private val JMS_PASSWORD_DOC = "Provides the password for the JMS connection"

  val CONNECTION_FACTORY = "connect.jms.connection.factory"
  private val CONNECTION_FACTORY_DOC = "Provides the full class name for the ConnectionFactory implementation to use."

  val KCQL = "connect.jms.kcql"
  val KCQL_DOC =  "KCQL expression describing field selection and routes."

  val MESSAGE_TYPE = "connect.jms.message.type"

  val ERROR_POLICY = "connect.jms.error.policy"
  val ERROR_POLICY_DOC: String = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
    "There are two available options: \n" + "NOOP - the error is swallowed \n" +
    "THROW - the error is allowed to propagate. \n" +
    "RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on \n" +
    "The error will be logged automatically"
  val ERROR_POLICY_DEFAULT = "THROW"

  val ERROR_RETRY_INTERVAL = "connect.jms.retry.interval"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"
  val NBR_OF_RETRIES = "connect.jms.max.retires"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20


  val CONVERTER_CONFIG = "connect.jms.source.converters"
  private val CONVERTER_DOC =
    """
      |Contains a tuple (Mqtt source topic and the canonical class name for the converter of a raw JMS message bytes to a SourceRecord).
      |If the source topic is not matched it will default to the BytesConverter
      |i.e. $jms_source1=com.datamountaineer.streamreactor.connect.source.converters.AvroConverter;$jms_source2=com.datamountaineer.streamreactor.connect.source.converters.JsonConverter""".stripMargin
  private val CONVERTER_DISPLAY = "Converter class"

  val THROW_ON_CONVERT_ERRORS_CONFIG = "connect.jms.converter.throw.on.error"
  private val THROW_ON_CONVERT_ERRORS_DOC = "If set to false the conversion exception will be swallowed and everything carries on BUT the message is lost!!; true will throw the exception.Default is false."
  private val THROW_ON_CONVERT_ERRORS_DISPLAY = "Throw error on conversion"
  private val THROW_ON_CONVERT_ERRORS_DEFAULT = false

  val DESTINATION_SELECTOR = "connect.jms.destination.selector"
  val DESTINATION_SELECTOR_DOC = "Selector to use for destination lookup. Either CDI or JNDI."
  val DESTINATION_SELECTOR_DEFAULT = "CDI"

  val TOPIC_LIST = "connect.jms.topics"
  val TOPIC_LIST_DOC = "A comma separated list of JMS topics, must match the KCQL source or target JMS topics."

  val QUEUE_LIST = "connect.jms.queues"
  val QUEUE_LIST_DOC = "A comma separated list of JMS topics, must match the KCQL source or target JMS queues."

  val config: ConfigDef = new ConfigDef()
    .define(JMS_URL, Type.STRING, Importance.HIGH, JMS_URL_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, JMS_URL)
    .define(CONNECTION_FACTORY, Type.STRING, Importance.HIGH, CONNECTION_FACTORY_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, CONNECTION_FACTORY)
    .define(KCQL, Type.STRING, Importance.HIGH, KCQL,
      "Connection", 3, ConfigDef.Width.MEDIUM, KCQL)
    .define(QUEUE_LIST, Type.LIST, new util.ArrayList[String], Importance.HIGH, QUEUE_LIST_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, QUEUE_LIST)
    .define(TOPIC_LIST, Type.LIST, new util.ArrayList[String], Importance.HIGH, TOPIC_LIST_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, TOPIC_LIST)
    .define(JMS_PASSWORD, Type.PASSWORD, null, Importance.HIGH, JMS_PASSWORD_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, JMS_PASSWORD)
    .define(JMS_USER, Type.STRING, null, Importance.HIGH, JMS_USER_DOC,
      "Connection", 8, ConfigDef.Width.MEDIUM, JMS_USER)
    .define(ERROR_POLICY, Type.STRING, ERROR_POLICY_DEFAULT, Importance.HIGH, ERROR_POLICY_DOC,
      "Connection", 9, ConfigDef.Width.MEDIUM, ERROR_POLICY)
    .define(ERROR_RETRY_INTERVAL, Type.INT, ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, ERROR_RETRY_INTERVAL_DOC,
      "Connection", 10, ConfigDef.Width.MEDIUM, ERROR_RETRY_INTERVAL)
    .define(NBR_OF_RETRIES, Type.INT, NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, NBR_OF_RETRIES_DOC,
      "Connection", 11, ConfigDef.Width.MEDIUM, NBR_OF_RETRIES)
    .define(DESTINATION_SELECTOR, Type.STRING, DESTINATION_SELECTOR_DEFAULT, Importance.MEDIUM, DESTINATION_SELECTOR_DOC,
      "Connection", 11, ConfigDef.Width.MEDIUM, DESTINATION_SELECTOR)

    //converters
    //converter
    .define(CONVERTER_CONFIG, Type.STRING, null, Importance.HIGH, CONVERTER_DOC, "Converter", 1, ConfigDef.Width.MEDIUM, CONVERTER_DISPLAY)
    .define(THROW_ON_CONVERT_ERRORS_CONFIG, Type.BOOLEAN, THROW_ON_CONVERT_ERRORS_DEFAULT, Importance.HIGH, THROW_ON_CONVERT_ERRORS_DOC, "Converter", 2, ConfigDef.Width.MEDIUM, THROW_ON_CONVERT_ERRORS_DISPLAY)

}

/**
  * <h1>JMSSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class JMSConfig(props: util.Map[String, String]) extends AbstractConfig(JMSConfig.config, props)
