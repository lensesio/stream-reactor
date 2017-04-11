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

import com.datamountaineer.connector.config.{Config, FormatType}
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.jms.config.DestinationSelector.DestinationSelector
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

case class JMSSettings(connectionURL: String,
                       initialContextClass: String,
                       connectionFactoryClass: String,
                       destinationSelector: DestinationSelector,
                       extraProps : List[Map[String, String]],
                       settings: List[JMSSetting],
                       user: Option[String],
                       password: Option[Password],
                       batchSize: Int,
                       errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                       retries: Int) {
  require(connectionURL != null && connectionURL.trim.length > 0, "Invalid connection URL")
  require(connectionFactoryClass != null, "Invalid class for connection factory")
}


object JMSSettings extends StrictLogging {

  /**
    * Creates an instance of JMSSettings from a JMSSinkConfig
    *
    * @param config : The map of all provided configurations
    * @return An instance of JmsSettings
    */
  def apply(config: JMSConfig, sink: Boolean) : JMSSettings = {
    val raw = config.getString(JMSConfigConstants.KCQL)
    require(raw != null && !raw.isEmpty, s"No ${JMSConfigConstants.KCQL} provided!")

    val kcql = raw.split(";").map(r => Config.parse(r))
    val errorPolicyE = ErrorPolicyEnum.withName(config.getString(JMSConfigConstants.ERROR_POLICY).toUpperCase)
    val errorPolicy = ErrorPolicy(errorPolicyE)
    val nbrOfRetries = config.getInt(JMSConfigConstants.NBR_OF_RETRIES)
    val initialContextFactoryClass = config.getString(JMSConfigConstants.INITIAL_CONTEXT_FACTORY)
    val clazz = config.getString(JMSConfigConstants.CONNECTION_FACTORY)
    val destinationSelector = DestinationSelector.withName(config.getString(JMSConfigConstants.DESTINATION_SELECTOR).toUpperCase)
    val extraProps = config.getList(JMSConfigConstants.EXTRA_PROPS)
      .map(p => p.split("=").grouped(2).map { case Array(k: String, v: String) => (k.trim -> v.trim) }.toMap).toList

    val url = config.getString(JMSConfigConstants.JMS_URL)
    if (url == null || url.trim.length == 0) {
      throw new ConfigException(s"${JMSConfigConstants.JMS_URL} has not been set")
    }

    val user = config.getString(JMSConfigConstants.JMS_USER)
    val passwordRaw = config.getPassword(JMSConfigConstants.JMS_PASSWORD)
    val sources = kcql.map(_.getSource).toSet
    val batchSize = config.getInt(JMSConfigConstants.BATCH_SIZE)

    val fields = kcql.map(rm => (rm.getSource,
      rm.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap)
    ).toMap

    val ignoreFields = kcql.map(rm => (rm.getSource, rm.getIgnoredField.toSet)).toMap

    val sourcesToConverterMap = Option(config.getString(JMSConfigConstants.CONVERTER_CONFIG))
      .map { c =>
        c.split(';')
          .map(_.trim)
          .filter(_.nonEmpty)
          .map { e =>
            e.split('=') match {
              case Array(source: String, clazz: String) =>

                if (!sources.contains(source)) {
                  throw new ConfigException(s"Invalid ${JMSConfigConstants.CONVERTER_CONFIG}. Source '$source' is not found in ${JMSConfigConstants.KCQL}. Defined sources:${sources.mkString(",")}")
                }
                Try(getClass.getClassLoader.loadClass(clazz)) match {
                  case Failure(_) => throw new ConfigException(s"Invalid ${JMSConfigConstants.CONVERTER_CONFIG}.$clazz can't be found")
                  case Success(clz) =>
                    if (!classOf[Converter].isAssignableFrom(clz)) {
                      throw new ConfigException(s"Invalid ${JMSConfigConstants.CONVERTER_CONFIG}. $clazz is not inheriting Converter")
                    }
                }

                source -> clazz
              case _ => throw new ConfigException(s"Invalid ${JMSConfigConstants.CONVERTER_CONFIG}. '$e' is not correct. Expecting source = className")
            }
          }.toMap
      }.getOrElse(Map.empty[String, String])

    val convertersMap = sourcesToConverterMap.map { s =>
      val clazz = s._2
      logger.info(s"Creating converter instance for $clazz")
      val converter = Try(this.getClass.getClassLoader.loadClass(clazz).newInstance()) match {
        case Success(value) => value.asInstanceOf[Converter]
        case Failure(_) => throw new ConfigException(s"Invalid ${JMSConfigConstants.CONVERTER_CONFIG} is invalid. $clazz should have an empty ctor!")
      }
      converter.initialize(config.props.toMap)
      s._1 -> converter
    }

    val jmsTopics = config.getList(JMSConfigConstants.TOPIC_LIST).toSet
    val jmsQueues = config.getList(JMSConfigConstants.QUEUE_LIST).toSet

    val settings = kcql.map(r => {
      val jmsName = if (sink) r.getTarget else r.getSource
      JMSSetting(r.getSource, r.getTarget, fields(r.getSource), ignoreFields(r.getSource), getDestinationType(jmsName, jmsQueues, jmsTopics), getFormatType(r), convertersMap.get(jmsName))
    }).toList

    new JMSSettings(
      url,
      initialContextFactoryClass,
      clazz,
      destinationSelector,
      extraProps,
      settings,
      Option(user),
      Option(passwordRaw),
      batchSize,
      errorPolicy,
      nbrOfRetries)
  }

  def getFormatType(config: Config) : FormatType = Option(config.getFormatType).getOrElse(FormatType.JSON)

  def getDestinationType(target: String, queues: Set[String], topics: Set[String]): DestinationType = {
    if (topics.contains(target)) {
      TopicDestination
    } else if (queues.contains(target)) {
      QueueDestination
    } else {
      throw new ConfigException(s"$target has not been configured as topic or queue.")
    }
  }
}
