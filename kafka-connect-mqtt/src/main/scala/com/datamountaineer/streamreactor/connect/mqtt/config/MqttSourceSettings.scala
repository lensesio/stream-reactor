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

package com.datamountaineer.streamreactor.connect.mqtt.config

import com.datamountaineer.streamreactor.connect.converters.source.{BytesConverter, Converter}
import org.apache.kafka.common.config.ConfigException
import org.eclipse.paho.client.mqttv3.MqttClient

import scala.util.{Failure, Success, Try}

case class MqttSourceSettings(connection: String,
                              user: Option[String],
                              password: Option[String],
                              clientId: String,
                              sourcesToConverters: Map[String, String],
                              throwOnConversion: Boolean,
                              kcql: Array[String],
                              mqttQualityOfService: Int,
                              connectionTimeout: Int,
                              pollingTimeout: Int,
                              cleanSession: Boolean,
                              keepAliveInterval: Int,
                              sslCACertFile: Option[String],
                              sslCertFile: Option[String],
                              sslCertKeyFile: Option[String],
                              enableProgress : Boolean = MqttConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT) {

  def asMap(): java.util.Map[String, String] = {
    val map = new java.util.HashMap[String, String]()
    map.put(MqttConfigConstants.HOSTS_CONFIG, connection)
    user.foreach(u => map.put(MqttConfigConstants.USER_CONFIG, u))
    password.foreach(p => map.put(MqttConfigConstants.PASSWORD_CONFIG, p))
    map.put(MqttConfigConstants.CLIENT_ID_CONFIG, clientId)
    map.put(MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG, connectionTimeout.toString)
    map.put(MqttConfigConstants.POLLING_TIMEOUT_CONFIG, pollingTimeout.toString)
    map.put(MqttConfigConstants.CLEAN_SESSION_CONFIG, cleanSession.toString)
    map.put(MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG, keepAliveInterval.toString)
    sslCACertFile.foreach(s => map.put(MqttConfigConstants.SSL_CA_CERT_CONFIG, s))
    sslCertFile.foreach(s => map.put(MqttConfigConstants.SSL_CERT_CONFIG, s))
    sslCertKeyFile.foreach(s => map.put(MqttConfigConstants.SSL_CERT_KEY_CONFIG, s))
    map.put(MqttConfigConstants.QS_CONFIG, mqttQualityOfService.toString)
    map.put(MqttConfigConstants.KCQL_CONFIG, kcql.mkString(";"))
    map
  }
}


object MqttSourceSettings {
  def apply(config: MqttSourceConfig): MqttSourceSettings = {
    def getFile(configKey: String) = Option(config.getString(configKey))

    val kcql = config.getKCQL
    val kcqlStr = config.getKCQLRaw
    val user = Some(config.getUsername)
    val password =  Option(config.getSecret).map(_.value())
    val connection = config.getHosts
    val clientId = Option(config.getString(MqttConfigConstants.CLIENT_ID_CONFIG)).getOrElse(MqttClient.generateClientId())

    val sslCACertFile = getFile(MqttConfigConstants.SSL_CA_CERT_CONFIG)
    val sslCertFile = getFile(MqttConfigConstants.SSL_CERT_CONFIG)
    val sslCertKeyFile = getFile(MqttConfigConstants.SSL_CERT_KEY_CONFIG)

    (sslCACertFile, sslCertFile, sslCertKeyFile) match {
      case (Some(_), Some(_), Some(_)) =>
      case (None, None, None) =>
      case _ => throw new ConfigException(s"You can't define one of the ${MqttConfigConstants.SSL_CA_CERT_CONFIG},${MqttConfigConstants.SSL_CERT_CONFIG}, ${MqttConfigConstants.SSL_CERT_KEY_CONFIG} without the other")
    }

    val progressEnabled = config.getBoolean(MqttConfigConstants.PROGRESS_COUNTER_ENABLED)

    val converters = kcql.map(k => {
      (k.getSource, if (k.getWithConverter == null) classOf[BytesConverter].getCanonicalName else k.getWithConverter)
    }).toMap

    converters.map( {
        case (mqtt_source, clazz) => {
          Try(Class.forName(clazz)) match {
            case Failure(_) => throw new ConfigException(s"Invalid ${MqttConfigConstants.KCQL_CONFIG}. $clazz can't be found for $mqtt_source")
            case Success(clz) =>
              if (!classOf[Converter].isAssignableFrom(clz)) {
                throw new ConfigException(s"Invalid ${MqttConfigConstants.KCQL_CONFIG}. $clazz is not inheriting Converter for $mqtt_source")
              }
          }
        }
      }
    )

    val qs = config.getInt(MqttConfigConstants.QS_CONFIG)
    if (qs < 0 || qs > 2) {
      throw new ConfigException(s"${MqttConfigConstants.QS_CONFIG} is not valid. Can be 0,1 or 2")
    }

    MqttSourceSettings(
      connection,
      user,
      password,
      clientId,
      converters,
      config.getBoolean(MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG),
      kcqlStr,
      qs,
      config.getInt(MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG),
      config.getInt(MqttConfigConstants.POLLING_TIMEOUT_CONFIG),
      config.getBoolean(MqttConfigConstants.CLEAN_SESSION_CONFIG),
      config.getInt(MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG),
      sslCACertFile,
      sslCertFile,
      sslCertKeyFile,
      progressEnabled
    )
  }
}
