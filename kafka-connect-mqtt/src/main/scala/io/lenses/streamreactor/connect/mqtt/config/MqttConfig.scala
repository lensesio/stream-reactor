/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.mqtt.config

import io.lenses.streamreactor.common.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

/**
  * Created by andrew@datamountaineer.com on 27/08/2017.
  * stream-reactor
  */
object MqttConfig {
  val config: ConfigDef = new ConfigDef()
    .define(
      MqttConfigConstants.HOSTS_CONFIG,
      Type.STRING,
      Importance.HIGH,
      MqttConfigConstants.HOSTS_DOC,
      "Connection",
      1,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.HOSTS_DISPLAY,
    )
    .define(
      MqttConfigConstants.USER_CONFIG,
      Type.STRING,
      null,
      Importance.HIGH,
      MqttConfigConstants.USER_DOC,
      "Connection",
      2,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.USER_DISPLAY,
    )
    .define(
      MqttConfigConstants.PASSWORD_CONFIG,
      Type.PASSWORD,
      null,
      Importance.HIGH,
      MqttConfigConstants.PASSWORD_DOC,
      "Connection",
      3,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.PASSWORD_DISPLAY,
    )
    .define(
      MqttConfigConstants.QS_CONFIG,
      Type.INT,
      Importance.MEDIUM,
      MqttConfigConstants.QS_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.QS_DISPLAY,
    )
    .define(
      MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG,
      Type.INT,
      MqttConfigConstants.CONNECTION_TIMEOUT_DEFAULT,
      Importance.LOW,
      MqttConfigConstants.CONNECTION_TIMEOUT_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.CONNECTION_TIMEOUT_DISPLAY,
    )
    .define(
      MqttConfigConstants.CLEAN_SESSION_CONFIG,
      Type.BOOLEAN,
      MqttConfigConstants.CLEAN_CONNECTION_DEFAULT,
      Importance.LOW,
      MqttConfigConstants.CLEAN_SESSION_CONFIG,
      "Connection",
      6,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.CLEAN_CONNECTION_DISPLAY,
    )
    .define(
      MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG,
      Type.INT,
      MqttConfigConstants.KEEP_ALIVE_INTERVAL_DEFAULT,
      Importance.LOW,
      MqttConfigConstants.KEEP_ALIVE_INTERVAL_DOC,
      "Connection",
      7,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.KEEP_ALIVE_INTERVAL_DISPLAY,
    )
    .define(
      MqttConfigConstants.CLIENT_ID_CONFIG,
      Type.STRING,
      null,
      Importance.LOW,
      MqttConfigConstants.CLIENT_ID_DOC,
      "Connection",
      8,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.CLIENT_ID_DISPLAY,
    )
    //ssl
    .define(
      MqttConfigConstants.SSL_CA_CERT_CONFIG,
      Type.STRING,
      null,
      Importance.MEDIUM,
      MqttConfigConstants.SSL_CA_CERT_DOC,
      "SSL",
      1,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.SSL_CA_CERT_DISPLAY,
    )
    .define(
      MqttConfigConstants.SSL_CERT_CONFIG,
      Type.STRING,
      null,
      Importance.MEDIUM,
      MqttConfigConstants.SSL_CERT_DOC,
      "SSL",
      2,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.SSL_CERT_DISPLAY,
    )
    .define(
      MqttConfigConstants.SSL_CERT_KEY_CONFIG,
      Type.STRING,
      null,
      Importance.MEDIUM,
      MqttConfigConstants.SSL_CERT_KEY_DOC,
      "SSL",
      3,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.SSL_CERT_KEY_DISPLAY,
    )
    //kcql
    .define(
      MqttConfigConstants.KCQL_CONFIG,
      Type.STRING,
      Importance.HIGH,
      MqttConfigConstants.KCQL_DOC,
      "KCQL",
      1,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.KCQL_DISPLAY,
    )
    .define(
      MqttConfigConstants.PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      MqttConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      MqttConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY,
    )
    //converter
    .define(
      MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG,
      Type.BOOLEAN,
      MqttConfigConstants.THROW_ON_CONVERT_ERRORS_DEFAULT,
      Importance.HIGH,
      MqttConfigConstants.THROW_ON_CONVERT_ERRORS_DOC,
      "Converter",
      1,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.THROW_ON_CONVERT_ERRORS_DISPLAY,
    )
    .define(
      MqttConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES,
      Type.STRING,
      MqttConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES_DEFAULT,
      Importance.HIGH,
      MqttConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES_DOC,
      "Converter",
      3,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES,
    )
}

object MqttSourceConfig {
  val config = MqttConfig.config
    //manager
    .define(
      MqttConfigConstants.POLLING_TIMEOUT_CONFIG,
      Type.INT,
      MqttConfigConstants.POLLING_TIMEOUT_DEFAULT,
      Importance.LOW,
      MqttConfigConstants.POLLING_TIMEOUT_DOC,
      "Manager",
      1,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.POLLING_TIMEOUT_DISPLAY,
    )
    .define(
      MqttConfigConstants.REPLICATE_SHARED_SUBSCIRPTIONS_CONFIG,
      Type.BOOLEAN,
      MqttConfigConstants.REPLICATE_SHARED_SUBSCIRPTIONS_DEFAULT,
      Importance.LOW,
      MqttConfigConstants.REPLICATE_SHARED_SUBSCIRPTIONS_DOC,
      "Manager",
      2,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.REPLICATE_SHARED_SUBSCIRPTIONS_DISPLAY,
    )
    .define(
      MqttConfigConstants.LOG_MESSAGE_ARRIVED_KEY,
      Type.BOOLEAN,
      MqttConfigConstants.LOG_MESSAGE_ARRIVED_DEFAULT,
      Importance.MEDIUM,
      MqttConfigConstants.LOG_MESSAGE_ARRIVED_DISPLAY,
      "Debug",
      1,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.LOG_MESSAGE_ARRIVED_DISPLAY,
    )
    .define(
      MqttConfigConstants.PROCESS_DUPES_CONFIG,
      Type.BOOLEAN,
      false,
      Importance.LOW,
      MqttConfigConstants.PROCESS_DUPES_DOC,
      "Connection",
      1,
      ConfigDef.Width.SHORT,
      MqttConfigConstants.PROCESS_DUPES_DISPLAY,
    )
}

case class MqttSourceConfig(props: Map[String, String])
    extends BaseConfig(MqttConfigConstants.CONNECTOR_PREFIX, MqttSourceConfig.config, props)
    with MqttConfigBase

object MqttSinkConfig {
  val config: ConfigDef = MqttConfig.config
    .define(
      MqttConfigConstants.ERROR_POLICY,
      Type.STRING,
      MqttConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      MqttConfigConstants.ERROR_POLICY_DOC,
      "Connection",
      9,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.ERROR_POLICY,
    )
    .define(
      MqttConfigConstants.ERROR_RETRY_INTERVAL,
      Type.INT,
      MqttConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      MqttConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection",
      10,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.ERROR_RETRY_INTERVAL,
    )
    .define(
      MqttConfigConstants.NBR_OF_RETRIES,
      Type.INT,
      MqttConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      MqttConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection",
      11,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.NBR_OF_RETRIES,
    )
    .define(
      MqttConfigConstants.RM_CONFIG,
      Type.BOOLEAN,
      MqttConfigConstants.RM_DEFAULT,
      Importance.MEDIUM,
      MqttConfigConstants.RM_DOC,
      "Connection",
      12,
      ConfigDef.Width.MEDIUM,
      MqttConfigConstants.RM_CONFIG,
    )
}

case class MqttSinkConfig(props: Map[String, String])
    extends BaseConfig(MqttConfigConstants.CONNECTOR_PREFIX, MqttSinkConfig.config, props)
    with MqttConfigBase

trait MqttConfigBase
    extends KcqlSettings
    with NumberRetriesSettings
    with ErrorPolicySettings
    with SSLSettings
    with HostSettings
    with UserSettings
