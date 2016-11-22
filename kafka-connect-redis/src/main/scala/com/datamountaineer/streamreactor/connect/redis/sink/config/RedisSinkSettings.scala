/**
  * Copyright 2016 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.redis.sink.config

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.{ErrorPolicy, ErrorPolicyEnum, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import com.datamountaineer.streamreactor.connect.rowkeys._
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Holds the Redis Sink settings
  */
case class RedisKCQLSetting(topic: String,
                            kcqlConfig: Config,
                            builder: StringKeyBuilder,
                            aliases: Map[String, String],
                            ignoredFields: Set[String])

case class RedisSinkSettings(connectionInfo: RedisConnectionInfo,
                             allKCQLSettings: Set[RedisKCQLSetting],
                             errorPolicy: ErrorPolicy = new ThrowErrorPolicy,
                             taskRetries: Int = RedisSinkConfig.NBR_OF_RETIRES_DEFAULT)

object RedisSinkSettings {
  def apply(config: RedisSinkConfig): RedisSinkSettings = {

    // Get the error-policy, num-of-retries, redis-connection-info
    val errorPolicy = ErrorPolicy(ErrorPolicyEnum.withName(config.getString(RedisSinkConfig.ERROR_POLICY).toUpperCase))
    val nbrOfRetries = config.getInt(RedisSinkConfig.NBR_OF_RETRIES)
    val connectionInfo = RedisConnectionInfo(config)

    // Ensure KCQL command/s are provided
    val kcqlCommands = config.getString(RedisSinkConfig.KCQL_CONFIG)
    require(kcqlCommands != null && kcqlCommands.nonEmpty, s"No ${RedisSinkConfig.KCQL_CONFIG} provided!")

    // Get per KCQL : kcqlConfig, key-builder, aliases, ignored-fields
    val kcqlConfigs = kcqlCommands.split(';').map(r => Config.parse(r)).toList.distinct

    // Get the builders
    val builders = kcqlConfigs.map { k =>
      val keys = k.getPrimaryKeys.asScala.toList
      // No PK => 'topic|par|offset' builder else generic-builder
      if (keys.nonEmpty) StringStructFieldsStringKeyBuilder(keys) else new StringGenericRowKeyBuilder()
    }

    // Get the aliases
    val aliases = kcqlConfigs.map { k => k.getFieldAlias.map(fa => (fa.getField, fa.getAlias)).toMap }

    // Get the ignored fields
    val ignoredFields = kcqlConfigs.map(r => r.getIgnoredField.asScala.toSet)

    val size = kcqlConfigs.length
    val allRedisKCQLSettings = (0 until size).map { i =>
      RedisKCQLSetting(kcqlConfigs.get(i).getSource, kcqlConfigs(i), builders(i), aliases(i), ignoredFields(i))
    }.toSet

    RedisSinkSettings(connectionInfo, allRedisKCQLSettings, errorPolicy, nbrOfRetries)
  }
}

object RedisConnectionInfo {
  def apply(config: RedisSinkConfig): RedisConnectionInfo = {
    val host = config.getString(REDIS_HOST)
    if (host.isEmpty) new ConfigException(s"$REDIS_HOST is not set correctly")

    val password = Option(config.getPassword(REDIS_PASSWORD)).map(_.value())

    new RedisConnectionInfo(
      host,
      config.getInt(REDIS_PORT),
      password)
  }
}

/**
  * Holds the Redis connection details.
  */
case class RedisConnectionInfo(host: String, port: Int, password: Option[String])
