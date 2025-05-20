/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.influx.config

import io.lenses.streamreactor.common.config.base.traits.BaseConfig
import io.lenses.streamreactor.common.config.base.traits.ConsistencyLevelSettings
import io.lenses.streamreactor.common.config.base.traits.DatabaseSettings
import io.lenses.streamreactor.common.config.base.traits.ErrorPolicySettings
import io.lenses.streamreactor.common.config.base.traits.KcqlSettings
import io.lenses.streamreactor.common.config.base.traits.NumberRetriesSettings
import io.lenses.streamreactor.common.config.base.traits.UserSettings

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
import com.influxdb.client.domain.WriteConsistency

object InfluxConfig {

  val config: ConfigDef = new ConfigDef()
    .define(
      InfluxConfigConstants.INFLUX_URL_CONFIG,
      Type.STRING,
      Importance.HIGH,
      InfluxConfigConstants.INFLUX_URL_DOC,
      "Connection",
      1,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.INFLUX_URL_CONFIG,
    )
    .define(
      InfluxConfigConstants.INFLUX_DATABASE_CONFIG,
      Type.STRING,
      Importance.HIGH,
      InfluxConfigConstants.INFLUX_DATABASE_DOC,
      "Connection",
      2,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.INFLUX_DATABASE_CONFIG,
    )
    .define(
      InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG,
      Type.STRING,
      Importance.HIGH,
      InfluxConfigConstants.INFLUX_CONNECTION_USER_DOC,
      "Connection",
      3,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.INFLUX_CONNECTION_USER_CONFIG,
    )
    .define(
      InfluxConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG,
      Type.PASSWORD,
      "",
      Importance.HIGH,
      InfluxConfigConstants.INFLUX_CONNECTION_PASSWORD_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.INFLUX_CONNECTION_PASSWORD_CONFIG,
    )
    .define(
      InfluxConfigConstants.KCQL_CONFIG,
      Type.STRING,
      Importance.HIGH,
      InfluxConfigConstants.KCQL_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.KCQL_DISPLAY,
    )
    .define(
      InfluxConfigConstants.ERROR_POLICY_CONFIG,
      Type.STRING,
      InfluxConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      InfluxConfigConstants.ERROR_POLICY_DOC,
      "Miscellaneous",
      1,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.ERROR_POLICY_CONFIG,
    )
    .define(
      InfluxConfigConstants.ERROR_RETRY_INTERVAL_CONFIG,
      Type.INT,
      InfluxConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      InfluxConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Miscellaneous",
      2,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.ERROR_RETRY_INTERVAL_CONFIG,
    )
    .define(
      InfluxConfigConstants.NBR_OF_RETRIES_CONFIG,
      Type.INT,
      InfluxConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      InfluxConfigConstants.NBR_OF_RETRIES_DOC,
      "Miscellaneous",
      3,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.NBR_OF_RETRIES_CONFIG,
    )
    .define(
      InfluxConfigConstants.RETENTION_POLICY_CONFIG,
      Type.STRING,
      InfluxConfigConstants.RETENTION_POLICY_DEFAULT,
      Importance.HIGH,
      InfluxConfigConstants.RETENTION_POLICY_DOC,
      "Writes",
      1,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.RETENTION_POLICY_DOC,
    )
    .define(
      InfluxConfigConstants.CONSISTENCY_CONFIG,
      Type.STRING,
      InfluxConfigConstants.CONSISTENCY_DEFAULT,
      Importance.MEDIUM,
      InfluxConfigConstants.CONSISTENCY_DOC,
      "Writes",
      2,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.CONSISTENCY_DISPLAY,
    )
    .define(
      InfluxConfigConstants.PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      InfluxConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      InfluxConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      InfluxConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY,
    )
}

/**
  * <h1>InfluxSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  */
case class InfluxConfig(props: Map[String, String])
    extends BaseConfig(InfluxConfigConstants.CONNECTOR_PREFIX, InfluxConfig.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with DatabaseSettings
    with ConsistencyLevelSettings[WriteConsistency]
    with UserSettings
