/*
 *  Copyright 2016 Datamountaineer.
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

package com.datamountaineer.streamreactor.connect.jms.sink.config

import javax.jms.{ConnectionFactory, QueueConnectionFactory, TopicConnectionFactory}

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import io.confluent.common.config.ConfigException

import scala.collection.JavaConversions._
import scala.util.Try



case class JMSSettings(connectionURL: String,
                       connectionFactoryClass: Class[_ <: ConnectionFactory with QueueConnectionFactory with TopicConnectionFactory],
                       routes: List[JMSConfig],
                       user: Option[String],
                       password: Option[String],
                       errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                       retries: Int) {
  require(connectionURL != null && connectionURL.trim.length > 0, "Invalid connection URL")
  require(connectionFactoryClass != null, "Invalid class for connection factory")
}


object JMSSettings {

  /**
    * Creates an instance of JMSSettings from a JMSSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of JmsSettings
    */
  def apply(config: JMSSinkConfig): JMSSettings = {

    val raw = config.getString(JMSSinkConfig.EXPORT_ROUTE_QUERY)
    require(raw != null && !raw.isEmpty, s"No ${JMSSinkConfig.EXPORT_ROUTE_QUERY} provided!")

    val topics = config.getList(JMSSinkConfig.TOPICS_LIST).toSet
    val queues = config.getList(JMSSinkConfig.QUEUES_LIST).toSet
    val routes: Array[JMSConfig] = raw.split(";").map(r => JMSConfig(Config.parse(r), topics, queues))

    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(JMSSinkConfig.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val nbrOfRetries = config.getInt(JMSSinkConfig.NBR_OF_RETRIES)

    val clazz = config.getString(JMSSinkConfig.CONNECTION_FACTORY)

    val connectionFactoryClass = Try(Class.forName(clazz)).getOrElse(throw new ConfigException("$clazz can not be loaded"))

    if (!connectionFactoryClass.isInstanceOf[Class[_ <: ConnectionFactory with QueueConnectionFactory with TopicConnectionFactory]]) {
      throw new ConfigException("$clazz is not derived from ConnectionFactory")
    }

    val url = config.getString(JMSSinkConfig.JMS_URL)
    if (url == null || url.trim.length == 0) {
      throw new ConfigException(s"${JMSSinkConfig.JMS_URL} has not been set")
    }

    val passwordRaw = config.getPassword(JMSSinkConfig.JMS_PASSWORD)

    val password = passwordRaw match {
      case null => null
      case _ => passwordRaw.value()
    }


    new JMSSettings(
      url,
      connectionFactoryClass.asInstanceOf[Class[_ <: ConnectionFactory with QueueConnectionFactory with TopicConnectionFactory]],
      routes.toList,
      Option(config.getString(JMSSinkConfig.JMS_USER)),
      Option(password),
      errorPolicy,
      nbrOfRetries)
  }
}
