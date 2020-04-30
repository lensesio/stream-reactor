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

package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.rowkeys._
import org.apache.kafka.common.config.{ConfigException, SslConfigs}

import scala.collection.JavaConverters._

// Redis connection details: host, port, password
case class RedisConnectionInfo(host: String,
                               port: Int,
                               password: Option[String],
                               isSslConnection: Boolean = false,
                               keyPassword: Option[String] = None,
                               keyStoreType: Option[String] = None,
                               keyStorePassword: Option[String] = None,
                               keyStoreFilepath: Option[String] = None,
                               trustStoreType: Option[String] = None,
                               trustStorePassword: Option[String] = None,
                               trustStoreFilepath: Option[String] = None
                              )

// Sink settings of each Redis KCQL statement
case class RedisKCQLSetting(topic: String,
                            kcqlConfig: Kcql,
                            builder: StringKeyBuilder,
                            fieldsAndAliases: Map[String, String],
                            ignoredFields: Set[String])

// All the settings of the running connector
case class RedisSinkSettings(connectionInfo: RedisConnectionInfo,
                             pkDelimiter: String,
                             kcqlSettings: Set[RedisKCQLSetting],
                             errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                             taskRetries: Int = RedisConfigConstants.NBR_OF_RETIRES_DEFAULT)

object RedisSinkSettings {

  def apply(config: RedisConfig): RedisSinkSettings = {

    // Ensure KCQL command/s are provided
    val kcqlCommands = config.getKCQL
    // Get per KCQL : kcqlConfig, key-builder, aliases, ignored-fields
    val kcqlConfigs = kcqlCommands.toList.distinct
    // Get the error-policy, num-of-retries, redis-connection-info
    val errorPolicy = config.getErrorPolicy
    val nbrOfRetries = config.getNumberRetries

    // Get the aliases
    val aliases = config.getFieldsAliases()
    // Get the ignored fields
    val ignoredFields = kcqlConfigs.map(r => r.getIgnoredFields.asScala.map(f => f.getName).toSet)
    // Get connection info
    val connectionInfo = RedisConnectionInfo(config)

    val pkDelimiter = config.getString(RedisConfigConstants.REDIS_PK_DELIMITER)

    // Get the builders
    val builders = config.getRowKeyBuilders()

    val size = kcqlConfigs.length

    val allRedisKCQLSettings = (0 until size).map { i =>
      RedisKCQLSetting(
        kcqlConfigs(i).getSource,
        kcqlConfigs(i),
        builders(i),
        aliases(i),
        ignoredFields(i)
      )
    }.toSet

    RedisSinkSettings(connectionInfo, pkDelimiter, allRedisKCQLSettings, errorPolicy, nbrOfRetries)
  }

}

object RedisConnectionInfo {
  def apply(config: RedisConfig): RedisConnectionInfo = {
    val host = config.getString(RedisConfigConstants.REDIS_HOST)
    if (host.isEmpty) new ConfigException(s"${RedisConfigConstants.REDIS_HOST} is not set correctly")

    val password = Option(config.getPassword(RedisConfigConstants.REDIS_PASSWORD)).map(_.value())

    val isSslConnection = config.getBoolean(RedisConfigConstants.REDIS_SSL_ENABLED)

    val trustStoreType = Option(config.getString(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG))
    val trustStorePath = Option(config.getString(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG))
    val trustStorePassword = Option(config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG)) match {
      case Some(p) => Some(p.value())
      case None => None
    }

    val keyStoreType = Option(config.getString(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG))
    val keyStorePath = Option(config.getString(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG))
    val keyStorePassword = Option(config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG)) match {
      case Some(p) => Some(p.value())
      case None => None
    }

    new RedisConnectionInfo(
      host = host,
      port = config.getInt(RedisConfigConstants.REDIS_PORT),
      password = password,
      isSslConnection = isSslConnection,
      keyStoreType = keyStoreType,
      keyStorePassword = keyStorePassword,
      keyStoreFilepath = keyStorePath,
      trustStoreType = trustStoreType,
      trustStorePassword = trustStorePassword,
      trustStoreFilepath = trustStorePath)
  }
}
