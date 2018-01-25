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

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.converters.source.{BytesConverter, Converter}
import com.datamountaineer.streamreactor.connect.pulsar.ConsumerConfigFactory
import org.apache.kafka.common.config.ConfigException
import org.apache.pulsar.client.api.SubscriptionType

import scala.util.{Failure, Success, Try}

case class PulsarSourceSettings(connection: String,
                                sourcesToConverters: Map[String, String],
                                throwOnConversion: Boolean,
                                kcql: Set[Kcql],
                                pollingTimeout: Int,
                                batchSize: Int,
                                sslCACertFile: Option[String],
                                sslCertFile: Option[String],
                                sslCertKeyFile: Option[String],
                                enableProgress: Boolean = PulsarConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT) {

  def asMap(): java.util.Map[String, String] = {
    val map = new java.util.HashMap[String, String]()
    map.put(PulsarConfigConstants.HOSTS_CONFIG, connection)
    map.put(PulsarConfigConstants.POLLING_TIMEOUT_CONFIG, pollingTimeout.toString)
    map.put(PulsarConfigConstants.KCQL_CONFIG, kcql.mkString(";"))
    map
  }
}


object PulsarSourceSettings {
  def apply(config: PulsarSourceConfig, maxTasks: Int): PulsarSourceSettings = {

    def getFile(configKey: String) = Option(config.getString(configKey))

    val kcql = config.getKCQL
    val connection = config.getHosts
    val progressEnabled = config.getBoolean(PulsarConfigConstants.PROGRESS_COUNTER_ENABLED)

    val converters = kcql.map(k => {
      (k.getSource, if (k.getWithConverter == null) classOf[BytesConverter].getCanonicalName else k.getWithConverter)
    }).toMap

    converters.foreach { case (pulsar_source, clazz) =>
      Try(Class.forName(clazz)) match {
        case Failure(_) => throw new ConfigException(s"Invalid ${PulsarConfigConstants.KCQL_CONFIG}. $clazz can't be found for $pulsar_source")
        case Success(clz) =>
          if (!classOf[Converter].isAssignableFrom(clz)) {
            throw new ConfigException(s"Invalid ${PulsarConfigConstants.KCQL_CONFIG}. $clazz is not inheriting Converter for $pulsar_source")
          }
      }
    }

    val batchSize = config.getInt(PulsarConfigConstants.INTERNAL_BATCH_SIZE)
    val sslCACertFile = getFile(PulsarConfigConstants.SSL_CA_CERT_CONFIG)
    val sslCertFile = getFile(PulsarConfigConstants.SSL_CERT_CONFIG)
    val sslCertKeyFile = getFile(PulsarConfigConstants.SSL_CERT_KEY_CONFIG)
    val throwOnError = config.getBoolean(PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG)
    val pollingTimeout = config.getInt(PulsarConfigConstants.POLLING_TIMEOUT_CONFIG)

    kcql.map( k => {
      val subscriptionType = ConsumerConfigFactory.getSubscriptionType(k)
      if (maxTasks > 1 && subscriptionType == SubscriptionType.Exclusive) {
        throw new ConfigException("Subscription mode set to Exclusive and max tasks greater than 1. Each task gets all KCQLs. " +
          "Multiple subscriptions to the same topic are not allowed in Exclusive mode.")
      }
    })

    PulsarSourceSettings(
      connection,
      converters,
      throwOnError,
      kcql,
      pollingTimeout,
      batchSize,
      sslCACertFile,
      sslCertFile,
      sslCertKeyFile,
      progressEnabled
    )
  }
}
