/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.jms.config

import io.lenses.kcql.FormatType
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.common.errors.ThrowErrorPolicy
import io.lenses.streamreactor.connect.converters.source.Converter
import io.lenses.streamreactor.connect.jms.config.DestinationSelector.DestinationSelector
import io.lenses.streamreactor.connect.jms.source.converters.CommonJMSMessageConverter
import io.lenses.streamreactor.connect.jms.source.converters.{ JMSSourceMessageConverter => JMSMessageSourceConverter }
import com.google.common.base.Splitter
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.config.types.Password

import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

case class JMSSetting(
  source:           String,
  target:           String,
  fields:           Map[String, String],
  ignoreField:      Set[String],
  destinationType:  DestinationType,
  format:           FormatType = FormatType.JSON,
  storageOptions:   StorageOptions,
  converter:        ConverterConfigWrapper,
  messageSelector:  Option[String],
  subscriptionName: Option[String],
  headers:          Map[String, String],
)

case class JMSSettings(
  connectionURL:          String,
  initialContextClass:    String,
  connectionFactoryClass: String,
  destinationSelector:    DestinationSelector,
  extraProps:             List[Map[String, String]],
  settings:               List[JMSSetting],
  subscriptionName:       Option[String],
  user:                   Option[String],
  password:               Option[Password],
  batchSize:              Int,
  errorPolicy:            ErrorPolicy = new ThrowErrorPolicy,
  retries:                Int,
  pollingTimeout:         Long,
  evictInterval:          Int,
  evictThreshold:         Int,
) {
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
  def apply(config: JMSConfig, sink: Boolean): JMSSettings = {

    val kcql           = config.getKCQL
    val errorPolicy    = config.getErrorPolicy
    val nbrOfRetries   = config.getNumberRetries
    val url            = config.getUrl
    val user           = config.getUsername
    val password       = config.getSecret
    val batchSize      = config.getInt(JMSConfigConstants.BATCH_SIZE)
    val fields         = config.getFieldsMap()
    val ignoreFields   = config.getIgnoreFieldsMap()
    val pollingTimeout = config.getLong(JMSConfigConstants.POLLING_TIMEOUT_CONFIG)

    val initialContextFactoryClass = config.getString(JMSConfigConstants.INITIAL_CONTEXT_FACTORY)
    val clazz                      = config.getString(JMSConfigConstants.CONNECTION_FACTORY)
    val destinationSelector =
      DestinationSelector.withName(config.getString(JMSConfigConstants.DESTINATION_SELECTOR).toUpperCase)

    val extraProps = config.getList(JMSConfigConstants.EXTRA_PROPS).asScala
      .map(p =>
        p.split("=").grouped(2)
          .map { case Array(k: String, v: String) => k.trim -> v.trim }.toMap,
      )
      .toList

    //Check withtype is set
    kcql.foreach { k =>
      if (k.getWithType == null) {
        throw new ConfigException(
          s"WITHTYPE not set for kcql $k so can't determine JMS destination type. Provide WITHTYPE=TOPIC or WITHTYPE=QUEUE",
        )
      }
    }

    val jmsTopics =
      kcql.filter(k => k.getWithType.toUpperCase.equals("TOPIC")).map(k => if (sink) k.getTarget else k.getSource)
    val jmsQueues =
      kcql.filter(k => k.getWithType.toUpperCase.equals("QUEUE")).map(k => if (sink) k.getTarget else k.getSource)
    val jmsSubscriptionName = config.getString(JMSConfigConstants.TOPIC_SUBSCRIPTION_NAME)

    val headers = parseAdditionalHeaders(config.getString(s"${JMSConfigConstants.HEADERS_CONFIG}"))

    val settings = kcql.map { r =>
      val jmsName = if (sink) r.getTarget else r.getSource

      val converters = JMSConnectorConverters(sink)(r, config.props.asScala.toMap) match {
        case None                    => throw new ConfigException("Converters should not be empty")
        case Some(Left(exception))   => throw exception
        case Some(Right(converters)) => converters
      }

      val headersForJmsDest: Map[String, String] = Splitter.on(',').omitEmptyStrings()
        .split(headers.getOrElse(jmsName, "")).asScala
        .map { header =>
          val keyValue = header.split(":", 2)
          (keyValue(0), keyValue(1))
        }.toMap

      JMSSetting(
        r.getSource,
        r.getTarget,
        fields(r.getSource),
        ignoreFields(r.getSource),
        getDestinationType(jmsName, jmsQueues, jmsTopics),
        getFormatType(r),
        StorageOptions(r),
        converters,
        Option(r.getWithJmsSelector),
        Option(if (r.getWithSubscription == null) jmsSubscriptionName else r.getWithSubscription),
        headersForJmsDest,
      )
    }.toList

    val evictInterval  = config.getInt(JMSConfigConstants.EVICT_UNCOMMITTED_MINUTES)
    val evictThreshold = config.getInt(JMSConfigConstants.EVICT_THRESHOLD_MINUTES)

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
      pollingTimeout,
      evictInterval,
      evictThreshold,
    )
  }

  def parseAdditionalHeaders(cfgLine: String): Map[String, String] =
    Splitter.on(';').omitEmptyStrings()
      .split(cfgLine).asScala
      .map { header =>
        val keyValue = header.split("=", 2)
        (keyValue(0), keyValue(1))
      }
      .toMap

  def getFormatType(kcql: Kcql): FormatType = Option(kcql.getFormatType).getOrElse(FormatType.JSON)

  def getDestinationType(target: String, queues: Set[String], topics: Set[String]): DestinationType =
    if (topics.contains(target)) {
      TopicDestination
    } else if (queues.contains(target)) {
      QueueDestination
    } else {
      throw new ConfigException(s"$target has not been configured as topic or queue.")
    }

  def toSourceJMSMessageConverter(value: Any): JMSMessageSourceConverter =
    value match {
      case converter1: Converter =>
        new CommonJMSMessageConverter(converter1)
      case converter: JMSMessageSourceConverter =>
        converter
      case _ =>
        throw new ConfigException(s"${value.getClass.toString} is neither JMSMessageConverter nor Converter.")
    }

}
