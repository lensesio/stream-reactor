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

package com.datamountaineer.streamreactor.connect.influx.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

object InfluxSinkConfig {

  val config: ConfigDef = new ConfigDef()
    .define(InfluxSinkConfigConstants.INFLUX_URL_CONFIG, Type.STRING, Importance.HIGH, InfluxSinkConfigConstants.INFLUX_URL_DOC, "Connection", 1, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.INFLUX_URL_CONFIG)
    .define(InfluxSinkConfigConstants.INFLUX_DATABASE_CONFIG, Type.STRING, Importance.HIGH, InfluxSinkConfigConstants.INFLUX_DATABASE_DOC, "Connection", 2, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.INFLUX_DATABASE_CONFIG)
    .define(InfluxSinkConfigConstants.INFLUX_CONNECTION_USER_CONFIG, Type.STRING, Importance.HIGH, InfluxSinkConfigConstants.INFLUX_CONNECTION_USER_DOC, "Connection", 3, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.INFLUX_CONNECTION_USER_CONFIG)
    .define(InfluxSinkConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG, Type.PASSWORD, "", Importance.HIGH, InfluxSinkConfigConstants.INFLUX_CONNECTION_PASSWORD_DOC, "Connection", 4, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG)
    .define(InfluxSinkConfigConstants.KCQL_CONFIG, Type.STRING, Importance.HIGH, InfluxSinkConfigConstants.KCQL_DOC, "Connection", 5, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.KCQL_DISPLAY)
    .define(InfluxSinkConfigConstants.ERROR_POLICY_CONFIG, Type.STRING, InfluxSinkConfigConstants.ERROR_POLICY_DEFAULT, Importance.HIGH, InfluxSinkConfigConstants.ERROR_POLICY_DOC, "Miscellaneous", 1, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.ERROR_POLICY_CONFIG)
    .define(InfluxSinkConfigConstants.ERROR_RETRY_INTERVAL_CONFIG, Type.INT, InfluxSinkConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM, InfluxSinkConfigConstants.ERROR_RETRY_INTERVAL_DOC, "Miscellaneous", 2, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.ERROR_RETRY_INTERVAL_CONFIG)
    .define(InfluxSinkConfigConstants.NBR_OF_RETRIES_CONFIG, Type.INT, InfluxSinkConfigConstants.NBR_OF_RETIRES_DEFAULT, Importance.MEDIUM, InfluxSinkConfigConstants.NBR_OF_RETRIES_DOC, "Miscellaneous", 3, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.NBR_OF_RETRIES_CONFIG)
    .define(InfluxSinkConfigConstants.RETENTION_POLICY_CONFIG, Type.STRING, InfluxSinkConfigConstants.RETENTION_POLICY_DEFAULT, Importance.HIGH, InfluxSinkConfigConstants.RETENTION_POLICY_DOC, "Writes", 1, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.RETENTION_POLICY_DOC)
    .define(InfluxSinkConfigConstants.CONSISTENCY_CONFIG, Type.STRING, InfluxSinkConfigConstants.CONSISTENCY_DEFAULT, Importance.MEDIUM, InfluxSinkConfigConstants.CONSISTENCY_DOC, "Writes", 2, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.CONSISTENCY_DISPLAY)
    .define(InfluxSinkConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, InfluxSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT, Importance.MEDIUM, InfluxSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DOC, "Metrics", 1, ConfigDef.Width.MEDIUM, InfluxSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

/**
  * <h1>InfluxSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class InfluxSinkConfig(props: util.Map[String, String]) extends AbstractConfig(InfluxSinkConfig.config, props)
