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

package com.datamountaineer.streamreactor.connect.jms.sink.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object JMSSinkConfig {

  val JMS_URL = "connect.jms.sink.url"
  private val JMS_URL_DOC = "Provides the JMS broker url"

  val JMS_USER = "connect.jms.sink.user"
  private val JMS_USER_DOC = "Provides the user for the JMS connection"

  val JMS_PASSWORD = "connect.jms.sink.password"
  private val JMS_PASSWORD_DOC = "Provides the password for the JMS connection"

  val CONNECTION_FACTORY = "connect.jms.sink.connection.factory"
  private val CONNECTION_FACTORY_DOC = "Provides the full class name for the ConnectionFactory implementation to use."

  val EXPORT_ROUTE_QUERY = "connect.jms.sink.export.route.query"
  val EXPORT_ROUTE_QUERY_DOC =  "KCQL expression describing field selection and routes."

  val TOPICS_LIST = "connect.jms.sink.export.route.topics"
  private val TOPICS_LIST_DOC = "Lists all the jms target topics"

  val QUEUES_LIST = "connect.jms.sink.export.route.queue"
  private val QUEUE_LIST_DOC = "Lists all the jms target queues"

  val MESSAGE_TYPE = "connect.jms.sink.message.type"
  private val MESSAGE_TYPE_DOC =
    """
      |Specifies the JMS payload. If JSON is chosen it will send a TextMessage;
      |if AVRO is chosen it will send a BytesMessage;
      |if MAP is chosen it will send a MapMessage
      |if OBJECT is chosen it will send an ObjectMessage""".stripMargin

  val ERROR_POLICY = "connect.jms.error.policy"
  val ERROR_POLICY_DOC = "Specifies the action to be taken if an error occurs while inserting the data.\n" +
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

  val config: ConfigDef = new ConfigDef()
    .define(JMS_URL, Type.STRING, Importance.HIGH, JMS_URL_DOC)
    .define(CONNECTION_FACTORY, Type.STRING, Importance.HIGH, CONNECTION_FACTORY_DOC)
    .define(EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH, EXPORT_ROUTE_QUERY)
    .define(ERROR_POLICY, Type.STRING, ERROR_POLICY_DEFAULT, Importance.HIGH, ERROR_POLICY_DOC)
    .define(ERROR_RETRY_INTERVAL, Type.INT, ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, ERROR_RETRY_INTERVAL_DOC)
    .define(NBR_OF_RETRIES, Type.INT, NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, NBR_OF_RETRIES_DOC)
    .define(TOPICS_LIST, Type.LIST, new util.ArrayList[String], Importance.HIGH, TOPICS_LIST_DOC)
    .define(QUEUES_LIST, Type.LIST, new util.ArrayList[String], Importance.HIGH, QUEUE_LIST_DOC)
    .define(JMS_PASSWORD, Type.STRING, null, Importance.HIGH, JMS_PASSWORD_DOC)
    .define(JMS_USER, Type.STRING, null, Importance.HIGH, JMS_USER_DOC)
    .define(MESSAGE_TYPE, Type.STRING, MessageType.AVRO.toString, Importance.HIGH, MESSAGE_TYPE_DOC)
}

/**
  * <h1>JMSSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class JMSSinkConfig(props: util.Map[String, String]) extends AbstractConfig(JMSSinkConfig.config, props)
