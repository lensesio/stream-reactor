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

import com.datamountaineer.streamreactor.common.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

import java.util

object RedisConfig {

  val config: ConfigDef = new ConfigDef()
    .define(RedisConfigConstants.REDIS_HOST, Type.STRING, Importance.HIGH, RedisConfigConstants.REDIS_HOST_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, RedisConfigConstants.REDIS_HOST)
    .define(RedisConfigConstants.REDIS_PORT, Type.INT, Importance.HIGH, RedisConfigConstants.REDIS_PORT_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, RedisConfigConstants.REDIS_PORT)
    .define(RedisConfigConstants.REDIS_PASSWORD, Type.PASSWORD, null, Importance.LOW, RedisConfigConstants.REDIS_PASSWORD_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, RedisConfigConstants.REDIS_PASSWORD)
    .define(RedisConfigConstants.REDIS_SSL_ENABLED, Type.BOOLEAN, false, Importance.LOW, RedisConfigConstants.REDIS_SSL_ENABLED_DOC,
        "Connection", 5, ConfigDef.Width.MEDIUM, RedisConfigConstants.REDIS_SSL_ENABLED)
    .define(RedisConfigConstants.KCQL_CONFIG, Type.STRING, Importance.HIGH, RedisConfigConstants.KCQL_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, RedisConfigConstants.KCQL_CONFIG)
    .define(RedisConfigConstants.ERROR_POLICY, Type.STRING, RedisConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH, RedisConfigConstants.ERROR_POLICY_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, RedisConfigConstants.ERROR_POLICY)
    .define(RedisConfigConstants.ERROR_RETRY_INTERVAL, Type.INT, RedisConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM, RedisConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, RedisConfigConstants.ERROR_RETRY_INTERVAL)
    .define(RedisConfigConstants.NBR_OF_RETRIES, Type.INT, RedisConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM, RedisConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, RedisConfigConstants.NBR_OF_RETRIES)
    .define(RedisConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, RedisConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, RedisConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, RedisConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
    .define(RedisConfigConstants.REDIS_PK_DELIMITER, Type.STRING,
      RedisConfigConstants.REDIS_PK_DELIMITER_DEFAULT_VALUE,
      Importance.LOW, RedisConfigConstants.REDIS_PK_DELIMITER_DOC)
    .withClientSslSupport()
}

/**
  * <h1>RedisSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class RedisConfig(props: util.Map[String, String])
  extends BaseConfig(RedisConfigConstants.CONNECTOR_PREFIX, RedisConfig.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
