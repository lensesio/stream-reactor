/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.mqtt.config

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.converters.sink.Converter
import io.lenses.streamreactor.common.errors.ErrorPolicy
import io.lenses.streamreactor.common.errors.ThrowErrorPolicy
import org.apache.kafka.common.config.ConfigException
import org.eclipse.paho.client.mqttv3.MqttClient

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * Created by andrew@datamountaineer.com on 27/08/2017.
  * stream-reactor
  */
case class MqttSinkSettings(
  connection:           String,
  user:                 Option[String],
  password:             Option[String],
  clientId:             String,
  sinksToConverters:    Map[String, String],
  kcql:                 Set[Kcql],
  mqttQualityOfService: Int,
  mqttRetainedMessage:  Boolean,
  connectionTimeout:    Int,
  cleanSession:         Boolean,
  keepAliveInterval:    Int,
  sslCACertFile:        Option[String],
  sslCertFile:          Option[String],
  sslCertKeyFile:       Option[String],
  enableProgress:       Boolean     = MqttConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
  errorPolicy:          ErrorPolicy = new ThrowErrorPolicy,
  maxRetries:           Int         = MqttConfigConstants.NBR_OF_RETIRES_DEFAULT,
)

object MqttSinkSettings {
  def apply(config: MqttSinkConfig): MqttSinkSettings = {
    def getFile(configKey: String) = Option(config.getString(configKey))

    val kcql       = config.getKCQL
    val user       = Some(config.getUsername)
    val password   = Option(config.getSecret).map(_.value())
    val connection = config.getHosts
    val clientId =
      Option(config.getString(MqttConfigConstants.CLIENT_ID_CONFIG)).getOrElse(MqttClient.generateClientId())

    val sslCACertFile  = getFile(MqttConfigConstants.SSL_CA_CERT_CONFIG)
    val sslCertFile    = getFile(MqttConfigConstants.SSL_CERT_CONFIG)
    val sslCertKeyFile = getFile(MqttConfigConstants.SSL_CERT_KEY_CONFIG)

    (sslCACertFile, sslCertFile, sslCertKeyFile) match {
      case (Some(_), Some(_), Some(_)) =>
      case (None, None, None)          =>
      case _ =>
        throw new ConfigException(
          s"You can't define one of the ${MqttConfigConstants.SSL_CA_CERT_CONFIG},${MqttConfigConstants.SSL_CERT_CONFIG}, ${MqttConfigConstants.SSL_CERT_KEY_CONFIG} without the other",
        )
    }

    val progressEnabled = config.getBoolean(MqttConfigConstants.PROGRESS_COUNTER_ENABLED)

    val errorPolicy = config.getErrorPolicy
    val maxRetries  = config.getNumberRetries

    val qs = config.getInt(MqttConfigConstants.QS_CONFIG)
    if (qs < 0 || qs > 2) {
      throw new ConfigException(s"${MqttConfigConstants.QS_CONFIG} is not valid. Can be 0,1 or 2")
    }

    val rm = config.getBoolean(MqttConfigConstants.RM_CONFIG)

    val converters = kcql.map { k =>
      (k.getSource, k.getWithConverter)
    }.toMap

    converters.foreach {
      case (kafka_topic, clazz) =>
        if (clazz != null) {
          Try(Class.forName(clazz)) match {
            case Failure(_) =>
              throw new ConfigException(
                s"Invalid ${MqttConfigConstants.KCQL_CONFIG}. $clazz can't be found for $kafka_topic",
              )
            case Success(clz) =>
              if (!classOf[Converter].isAssignableFrom(clz)) {
                throw new ConfigException(
                  s"Invalid ${MqttConfigConstants.KCQL_CONFIG}. $clazz is not inheriting Converter for $kafka_topic",
                )
              }
          }
        }
    }

    new MqttSinkSettings(
      connection,
      user,
      password,
      clientId,
      converters,
      kcql,
      qs,
      rm,
      config.getInt(MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG),
      config.getBoolean(MqttConfigConstants.CLEAN_SESSION_CONFIG),
      config.getInt(MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG),
      sslCACertFile,
      sslCertFile,
      sslCertKeyFile,
      progressEnabled,
      errorPolicy,
      maxRetries,
    )
  }

  //new MqttSinkSettings(connection, user, password, clientId, kcql, mqttQualityOfService, connectionTimeout, cleanSession, keepAliveInterval, sslCACertFile, sslCertFile, sslCertKeyFile, enableProgress)
}
