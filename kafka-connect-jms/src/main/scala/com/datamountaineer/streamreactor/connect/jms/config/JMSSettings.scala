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

import com.datamountaineer.kcql.{FormatType, Kcql}
import com.datamountaineer.streamreactor.connect.converters.source.Converter
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.jms.config.DestinationSelector.DestinationSelector
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

case class JMSSetting(source: String,
                      target: String,
                      fields: Map[String, String],
                      ignoreField: Set[String],
                      destinationType: DestinationType,
                      format: FormatType = FormatType.JSON,
                      sourceConverters: Option[Converter],
                      messageSelector: Option[String])

case class JMSSettings(connectionURL: String,
                       initialContextClass: String,
                       connectionFactoryClass: String,
                       destinationSelector: DestinationSelector,
                       extraProps : List[Map[String, String]],
                       settings: List[JMSSetting],
                       subscriptionName: Option[String],
                       user: Option[String],
                       password: Option[Password],
                       batchSize: Int,
                       errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                       retries: Int,
                       pollingTimeout: Long) {
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

    val kcql = config.getKCQL
    val errorPolicy = config.getErrorPolicy
    val nbrOfRetries = config.getNumberRetries
    val url = config.getUrl
    val user = config.getUsername
    val password = config.getSecret
    val batchSize = config.getInt(JMSConfigConstants.BATCH_SIZE)
    val fields = config.getFieldsMap()
    val ignoreFields = config.getIgnoreFieldsMap()
    val pollingTimeout = config.getLong(JMSConfigConstants.POLLING_TIMEOUT_CONFIG)

    val initialContextFactoryClass = config.getString(JMSConfigConstants.INITIAL_CONTEXT_FACTORY)
    val clazz = config.getString(JMSConfigConstants.CONNECTION_FACTORY)
    val destinationSelector = DestinationSelector.withName(config.getString(JMSConfigConstants.DESTINATION_SELECTOR).toUpperCase)

    val extraProps = config.getList(JMSConfigConstants.EXTRA_PROPS)
                            .map(p => p.split("=").grouped(2)
                            .map { case Array(k: String, v: String) => k.trim -> v.trim }.toMap)
                            .toList
    //get default converter
    val defaultConverterClassName = config.getString(JMSConfigConstants.DEFAULT_CONVERTER_CONFIG)

    val defaultConverter = Option(defaultConverterClassName)
      .filterNot(c => c.isEmpty).map { c =>
      Try(Class.forName(c)) match {
        case Failure(_) => throw new ConfigException(s"Invalid ${JMSConfigConstants.DEFAULT_CONVERTER_CONFIG}.$c can't be found")
        case Success(clz) =>
          if (!classOf[Converter].isAssignableFrom(clz)) {
            throw new ConfigException(s"Invalid ${JMSConfigConstants.DEFAULT_CONVERTER_CONFIG}. $c is not inheriting Converter")
          }
          logger.info(s"Creating converter instance for $c")
          val converter = Try(Class.forName(c).newInstance()) match {
            case Success(value) => value.asInstanceOf[Converter]
            case Failure(_) => throw new ConfigException(s"${JMSConfigConstants.DEFAULT_CONVERTER_CONFIG} is invalid. $c should have an empty ctor!")
          }
          converter.initialize(config.props.toMap)
          converter
      }
    }

    //get converters, filtering out those with it not set in kcql
    val converters = kcql
                      .filterNot(k => k.getWithConverter == null)
                      .map(k => (k.getSource, k.getWithConverter))
                      .toMap

    //check converters
    val convertersMap = converters.map( {
      case (jms_source, clazz) => {
        logger.info(s"Creating converter instance for $clazz")
        val converter = Try(Class.forName(clazz).newInstance()) match {
          case Success(value) => value.asInstanceOf[Converter]
          case Failure(_) => throw new ConfigException(s"Invalid ${JMSConfigConstants.KCQL} is invalid for $jms_source. $clazz should have an empty ctor!")
        }
        converter.initialize(config.props.toMap)
        (jms_source,converter)
      }
    })

    //Check withtype is set
    kcql.foreach(k => {
      if (k.getWithType == null) {
        throw new ConfigException(s"WITHTYPE not set for kcql $k so can't determine JMS destination type. Provide WITHTYPE=TOPIC or WITHTYPE=QUEUE")
      }
    })

    val jmsTopics = kcql.filter(k => k.getWithType.toUpperCase.equals("TOPIC")).map(k => if (sink) k.getTarget else k.getSource)
    val jmsQueues = kcql.filter(k => k.getWithType.toUpperCase.equals("QUEUE")).map(k => if (sink) k.getTarget else k.getSource)
    val jmsSubscriptionName = config.getString(JMSConfigConstants.TOPIC_SUBSCRIPTION_NAME)

    val settings = kcql.map(r => {
      val jmsName = if (sink) r.getTarget else r.getSource
      var converter = convertersMap.get(jmsName)
      if (converter.isEmpty) {
        converter = defaultConverter
      }

      JMSSetting(r.getSource,
                r.getTarget,
                fields(r.getSource),
                ignoreFields(r.getSource),
                getDestinationType(jmsName, jmsQueues, jmsTopics),
                getFormatType(r),
                converter,
                Option(r.getWithJmsSelector()))
    }).toList

    new JMSSettings(
      url,
      initialContextFactoryClass,
      clazz,
      destinationSelector,
      extraProps,
      settings,
      Option(jmsSubscriptionName),
      Option(user),
      Option(password),
      batchSize,
      errorPolicy,
      nbrOfRetries,
      pollingTimeout)
  }

  def getFormatType(kcql: Kcql) : FormatType = Option(kcql.getFormatType).getOrElse(FormatType.JSON)

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
