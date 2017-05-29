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

package com.datamountaineer.streamreactor.connect.voltdb.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object VoltSinkConfig {
  val config: ConfigDef = new ConfigDef()
    .define(VoltSinkConfigConstants.SERVERS_CONFIG, Type.STRING, Importance.HIGH, VoltSinkConfigConstants.SERVERS_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, VoltSinkConfigConstants.SERVERS_CONFIG)
    .define(VoltSinkConfigConstants.USER_CONFIG, Type.STRING, Importance.HIGH, VoltSinkConfigConstants.USER_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, VoltSinkConfigConstants.USER_CONFIG)
    .define(VoltSinkConfigConstants.PASSWORD_CONFIG, Type.PASSWORD, Importance.HIGH, VoltSinkConfigConstants.PASSWORD_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, VoltSinkConfigConstants.PASSWORD_CONFIG)
    .define(VoltSinkConfigConstants.EXPORT_ROUTE_QUERY_CONFIG, Type.STRING, Importance.HIGH, VoltSinkConfigConstants.EXPORT_ROUTE_QUERY_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, VoltSinkConfigConstants.EXPORT_ROUTE_QUERY_CONFIG)
    .define(VoltSinkConfigConstants.ERROR_POLICY_CONFIG, Type.STRING, VoltSinkConfigConstants.ERROR_POLICY_DEFAULT, Importance.HIGH, VoltSinkConfigConstants.ERROR_POLICY_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, VoltSinkConfigConstants.ERROR_POLICY_CONFIG)
    .define(VoltSinkConfigConstants.ERROR_RETRY_INTERVAL_CONFIG, Type.INT, VoltSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM,
      VoltSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC, "Connection", 1, ConfigDef.Width.MEDIUM, VoltSinkConfigConstants.ERROR_RETRY_INTERVAL_CONFIG)
    .define(VoltSinkConfigConstants.NBR_OF_RETRIES_CONFIG, Type.INT, VoltSinkConfigConstants.NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, VoltSinkConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 6, ConfigDef.Width.MEDIUM, VoltSinkConfigConstants.NBR_OF_RETRIES_CONFIG)
    .define(VoltSinkConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, VoltSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
        Importance.MEDIUM, VoltSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
        "Metrics", 1, ConfigDef.Width.MEDIUM, VoltSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

/**
  * <h1>VoltSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class VoltSinkConfig(props: util.Map[String, String]) extends AbstractConfig(VoltSinkConfig.config, props)
