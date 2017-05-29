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

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object RedisSinkConfig {

  val config: ConfigDef = new ConfigDef()
    .define(RedisSinkConfigConstants.REDIS_HOST, Type.STRING, Importance.HIGH, RedisSinkConfigConstants.REDIS_HOST_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, RedisSinkConfigConstants.REDIS_HOST)
    .define(RedisSinkConfigConstants.REDIS_PORT, Type.INT, Importance.HIGH, RedisSinkConfigConstants.REDIS_PORT_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, RedisSinkConfigConstants.REDIS_PORT)
    .define(RedisSinkConfigConstants.REDIS_PASSWORD, Type.PASSWORD, null, Importance.LOW, RedisSinkConfigConstants.REDIS_PASSWORD_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, RedisSinkConfigConstants.REDIS_PASSWORD)
    .define(RedisSinkConfigConstants.KCQL_CONFIG, Type.STRING, Importance.HIGH, RedisSinkConfigConstants.KCQL_CONFIG,
      "Connection", 1, ConfigDef.Width.MEDIUM, RedisSinkConfigConstants.KCQL_CONFIG)
    .define(RedisSinkConfigConstants.ERROR_POLICY, Type.STRING, RedisSinkConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH, RedisSinkConfigConstants.ERROR_POLICY_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, RedisSinkConfigConstants.ERROR_POLICY)
    .define(RedisSinkConfigConstants.ERROR_RETRY_INTERVAL, Type.INT, RedisSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM, RedisSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, RedisSinkConfigConstants.ERROR_RETRY_INTERVAL)
    .define(RedisSinkConfigConstants.NBR_OF_RETRIES, Type.INT, RedisSinkConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM, RedisSinkConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, RedisSinkConfigConstants.NBR_OF_RETRIES)
    .define(RedisSinkConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, RedisSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
        Importance.MEDIUM, RedisSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
        "Metrics", 1, ConfigDef.Width.MEDIUM, RedisSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

/**
  * <h1>RedisSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class RedisSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(RedisSinkConfig.config, props)
