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
                              sslCertKeyFile: Option[String]) {

  def asMap(): java.util.Map[String, String] = {
    val map = new java.util.HashMap[String, String]()
    map.put(MqttSourceConfig.HOSTS_CONFIG, connection)
    user.foreach(u => map.put(MqttSourceConfig.USER_CONFIG, u))
    password.foreach(p => map.put(MqttSourceConfig.PASSWORD_CONFIG, p))
    map.put(MqttSourceConfig.CLIENT_ID_CONFIG, clientId)
    map.put(MqttSourceConfig.CONNECTION_TIMEOUT_CONFIG, connectionTimeout.toString)
    map.put(MqttSourceConfig.CLEAN_SESSION_CONFIG, cleanSession.toString)
    map.put(MqttSourceConfig.KEEP_ALIVE_INTERVAL_CONFIG, keepAliveInterval.toString)
    sslCACertFile.foreach(s => map.put(MqttSourceConfig.SSL_CA_CERT_CONFIG, s))
    sslCertFile.foreach(s => map.put(MqttSourceConfig.SSL_CERT_CONFIG, s))
    sslCertKeyFile.foreach(s => map.put(MqttSourceConfig.SSL_CERT_KEY_CONFIG, s))

    map.put(MqttSourceConfig.QS_CONFIG, mqttQualityOfService.toString)
    map.put(MqttSourceConfig.KCQL_CONFIG, kcql.mkString(";"))


    map.put(MqttSourceConfig.CONVERTER_CONFIG, kcql.map(Config.parse).map(_.getSource).map(s => s"$s=${sourcesToConverters(s)}").mkString(";"))
    map
  }
}


object MqttSourceSettings {
  def apply(config: MqttSourceConfig): MqttSourceSettings = {
    val kcqlStr = config.getString(MqttSourceConfig.KCQL_CONFIG).split(';')
      .filter(_.trim.nonEmpty)

    val kcql = kcqlStr.map(Config.parse)
    require(kcql.nonEmpty, s"${MqttSourceConfig.KCQL_CONFIG} provided!")

    val sources = kcql.map(_.getSource).toSet

    val connection = config.getString(MqttSourceConfig.HOSTS_CONFIG)
    if (connection == null || connection.trim.isEmpty) {
      throw new ConfigException(s"${MqttSourceConfig.HOSTS_CONFIG} is not provided!")
    }

    val user = Option(config.getString(MqttSourceConfig.USER_CONFIG))
    val password = Option(config.getPassword(MqttSourceConfig.PASSWORD_CONFIG)).map(_.value())

    val clientId = Option(config.getString(MqttSourceConfig.CLIENT_ID_CONFIG)).getOrElse(MqttClient.generateClientId())

    def getFile(configKey: String) = Option(config.getString(configKey))

    val sslCACertFile = getFile(MqttSourceConfig.SSL_CA_CERT_CONFIG)
    val sslCertFile = getFile(MqttSourceConfig.SSL_CERT_CONFIG)
    val sslCertKeyFile = getFile(MqttSourceConfig.SSL_CERT_KEY_CONFIG)

    (sslCACertFile, sslCertFile, sslCertKeyFile) match {
      case (Some(_), Some(_), Some(_)) =>
      case (None, None, None) =>
      case _ => throw new ConfigException(s"You can't define one of the ${MqttSourceConfig.SSL_CA_CERT_CONFIG},${MqttSourceConfig.SSL_CERT_CONFIG}, ${MqttSourceConfig.SSL_CERT_KEY_CONFIG} without the other")
    }

    val sourcesToConverterMap = Option(config.getString(MqttSourceConfig.CONVERTER_CONFIG))
      .map { c =>
        c.split(';')
          .map(_.trim)
          .filter(_.nonEmpty)
          .map { e =>
            e.split('=') match {
              case Array(source: String, clazz: String) =>

                if (!sources.contains(source)) {
                  throw new ConfigException(s"Invalid ${MqttSourceConfig.CONVERTER_CONFIG}. Source '$source' is not found in ${MqttSourceConfig.KCQL_CONFIG}. Defined sources:${sources.mkString(",")}")
                }
                Try(getClass.getClassLoader.loadClass(clazz)) match {
                  case Failure(_) => throw new ConfigException(s"Invalid ${MqttSourceConfig.CONVERTER_CONFIG}.$clazz can't be found")
                  case Success(clz) =>
                    if (!classOf[Converter].isAssignableFrom(clz)) {
                      throw new ConfigException(s"Invalid ${MqttSourceConfig.CONVERTER_CONFIG}. $clazz is not inheriting MqttConverter")
                    }
                }

                source -> clazz
              case _ => throw new ConfigException(s"Invalid ${MqttSourceConfig.CONVERTER_CONFIG}. '$e' is not correct. Expecting source = className")
            }
          }.toMap
      }.getOrElse(Map.empty[String, String])

    val sourcesToConverterMap1 = sources.filterNot(sourcesToConverterMap.contains)
      .map { s => s -> classOf[BytesConverter].getCanonicalName }
      .foldLeft(sourcesToConverterMap)(_ + _)

    val qs = config.getInt(MqttSourceConfig.QS_CONFIG)
    if (qs < 0 || qs > 2) {
      throw new ConfigException(s"${MqttSourceConfig.QS_CONFIG} is not valid. Can be 0,1 or 2")
    }
    MqttSourceSettings(
      connection,
      user,
      password,
      clientId,
      sourcesToConverterMap1,
      config.getBoolean(MqttSourceConfig.THROW_ON_CONVERT_ERRORS_CONFIG),
      kcqlStr,
      qs,
      config.getInt(MqttSourceConfig.CONNECTION_TIMEOUT_CONFIG),
      config.getBoolean(MqttSourceConfig.CLEAN_SESSION_CONFIG),
      config.getInt(MqttSourceConfig.KEEP_ALIVE_INTERVAL_CONFIG),
      sslCACertFile,
      sslCertFile,
      sslCertKeyFile
    )
  }
}
