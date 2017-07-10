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

import com.datamountaineer.connector.config.Config
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
                              cleanSession: Boolean,
                              keepAliveInterval: Int,
                              sslCACertFile: Option[String],
                              sslCertFile: Option[String],
                              sslCertKeyFile: Option[String],
                              enableProgress : Boolean = MqttSourceConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT) {

  def asMap(): java.util.Map[String, String] = {
    val map = new java.util.HashMap[String, String]()
    map.put(MqttSourceConfigConstants.HOSTS_CONFIG, connection)
    user.foreach(u => map.put(MqttSourceConfigConstants.USER_CONFIG, u))
    password.foreach(p => map.put(MqttSourceConfigConstants.PASSWORD_CONFIG, p))
    map.put(MqttSourceConfigConstants.CLIENT_ID_CONFIG, clientId)
    map.put(MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG, connectionTimeout.toString)
    map.put(MqttSourceConfigConstants.CLEAN_SESSION_CONFIG, cleanSession.toString)
    map.put(MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG, keepAliveInterval.toString)
    sslCACertFile.foreach(s => map.put(MqttSourceConfigConstants.SSL_CA_CERT_CONFIG, s))
    sslCertFile.foreach(s => map.put(MqttSourceConfigConstants.SSL_CERT_CONFIG, s))
    sslCertKeyFile.foreach(s => map.put(MqttSourceConfigConstants.SSL_CERT_KEY_CONFIG, s))

    map.put(MqttSourceConfigConstants.QS_CONFIG, mqttQualityOfService.toString)
    map.put(MqttSourceConfigConstants.KCQL_CONFIG, kcql.mkString(";"))


    map.put(MqttSourceConfigConstants.CONVERTER_CONFIG, kcql.map(Config.parse).map(_.getSource).map(s => s"$s=${sourcesToConverters(s)}").mkString(";"))
    map
  }
}


object MqttSourceSettings {
  def apply(config: MqttSourceConfig): MqttSourceSettings = {
    val kcqlStr = config.getString(MqttSourceConfigConstants.KCQL_CONFIG).split(';')
      .filter(_.trim.nonEmpty)

    val kcql = kcqlStr.map(Config.parse)
    require(kcql.nonEmpty, s"${MqttSourceConfigConstants.KCQL_CONFIG} provided!")

    val sources = kcql.map(_.getSource).toSet

    val connection = config.getString(MqttSourceConfigConstants.HOSTS_CONFIG)
    if (connection == null || connection.trim.isEmpty) {
      throw new ConfigException(s"${MqttSourceConfigConstants.HOSTS_CONFIG} is not provided!")
    }

    val user = Option(config.getString(MqttSourceConfigConstants.USER_CONFIG))
    val password = Option(config.getPassword(MqttSourceConfigConstants.PASSWORD_CONFIG)).map(_.value())

    val clientId = Option(config.getString(MqttSourceConfigConstants.CLIENT_ID_CONFIG)).getOrElse(MqttClient.generateClientId())

    def getFile(configKey: String) = Option(config.getString(configKey))

    val sslCACertFile = getFile(MqttSourceConfigConstants.SSL_CA_CERT_CONFIG)
    val sslCertFile = getFile(MqttSourceConfigConstants.SSL_CERT_CONFIG)
    val sslCertKeyFile = getFile(MqttSourceConfigConstants.SSL_CERT_KEY_CONFIG)

    (sslCACertFile, sslCertFile, sslCertKeyFile) match {
      case (Some(_), Some(_), Some(_)) =>
      case (None, None, None) =>
      case _ => throw new ConfigException(s"You can't define one of the ${MqttSourceConfigConstants.SSL_CA_CERT_CONFIG},${MqttSourceConfigConstants.SSL_CERT_CONFIG}, ${MqttSourceConfigConstants.SSL_CERT_KEY_CONFIG} without the other")
    }

    val progressEnabled = config.getBoolean(MqttSourceConfigConstants.PROGRESS_COUNTER_ENABLED)

    val sourcesToConverterMap = Option(config.getString(MqttSourceConfigConstants.CONVERTER_CONFIG))
      .map { c =>
        c.split(';')
          .map(_.trim)
          .filter(_.nonEmpty)
          .map { e =>
            e.split('=') match {
              case Array(source: String, clazz: String) =>

                if (!sources.contains(source)) {
                  throw new ConfigException(s"Invalid ${MqttSourceConfigConstants.CONVERTER_CONFIG}. Source '$source' is not found in ${MqttSourceConfigConstants.KCQL_CONFIG}. Defined sources:${sources.mkString(",")}")
                }
                Try(getClass.getClassLoader.loadClass(clazz)) match {
                  case Failure(_) => throw new ConfigException(s"Invalid ${MqttSourceConfigConstants.CONVERTER_CONFIG}.$clazz can't be found")
                  case Success(clz) =>
                    if (!classOf[Converter].isAssignableFrom(clz)) {
                      throw new ConfigException(s"Invalid ${MqttSourceConfigConstants.CONVERTER_CONFIG}. $clazz is not inheriting MqttConverter")
                    }
                }

                source -> clazz
              case _ => throw new ConfigException(s"Invalid ${MqttSourceConfigConstants.CONVERTER_CONFIG}. '$e' is not correct. Expecting source = className")
            }
          }.toMap
      }.getOrElse(Map.empty[String, String])

    val sourcesToConverterMap1 = sources.filterNot(sourcesToConverterMap.contains)
      .map { s => s -> classOf[BytesConverter].getCanonicalName }
      .foldLeft(sourcesToConverterMap)(_ + _)

    val qs = config.getInt(MqttSourceConfigConstants.QS_CONFIG)
    if (qs < 0 || qs > 2) {
      throw new ConfigException(s"${MqttSourceConfigConstants.QS_CONFIG} is not valid. Can be 0,1 or 2")
    }
    MqttSourceSettings(
      connection,
      user,
      password,
      clientId,
      sourcesToConverterMap1,
      config.getBoolean(MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG),
      kcqlStr,
      qs,
      config.getInt(MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG),
      config.getBoolean(MqttSourceConfigConstants.CLEAN_SESSION_CONFIG),
      config.getInt(MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG),
      sslCACertFile,
      sslCertFile,
      sslCertKeyFile,
      progressEnabled
    )
  }
}
