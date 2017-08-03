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

import java.util

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.ConfigDef

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */
object MqttSourceConfig {
  val config: ConfigDef = new ConfigDef()
    .define(MqttConfigConstants.HOSTS_CONFIG, Type.STRING, Importance.HIGH, MqttConfigConstants.HOSTS_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, MqttConfigConstants.HOSTS_DISPLAY)
    .define(MqttConfigConstants.USER_CONFIG, Type.STRING, null, Importance.HIGH, MqttConfigConstants.USER_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, MqttConfigConstants.USER_DISPLAY)
    .define(MqttConfigConstants.PASSWORD_CONFIG, Type.PASSWORD, null, Importance.HIGH, MqttConfigConstants.PASSWORD_DOC,
      "Connection", 3, ConfigDef.Width.MEDIUM, MqttConfigConstants.PASSWORD_DISPLAY)
    .define(MqttConfigConstants.QS_CONFIG, Type.INT, Importance.MEDIUM, MqttConfigConstants.QS_DOC,
      "Connection", 4, ConfigDef.Width.MEDIUM, MqttConfigConstants.QS_DISPLAY)
    .define(MqttConfigConstants.CONNECTION_TIMEOUT_CONFIG, Type.INT, MqttConfigConstants.CONNECTION_TIMEOUT_DEFAULT,
      Importance.LOW, MqttConfigConstants.CONNECTION_TIMEOUT_DOC,
      "Connection", 5, ConfigDef.Width.MEDIUM, MqttConfigConstants.CONNECTION_TIMEOUT_DISPLAY)
    .define(MqttConfigConstants.CLEAN_SESSION_CONFIG, Type.BOOLEAN, MqttConfigConstants.CLEAN_CONNECTION_DEFAULT,
      Importance.LOW, MqttConfigConstants.CLEAN_SESSION_CONFIG,
      "Connection", 6, ConfigDef.Width.MEDIUM, MqttConfigConstants.CLEAN_CONNECTION_DISPLAY)
    .define(MqttConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG, Type.INT, MqttConfigConstants.KEEP_ALIVE_INTERVAL_DEFAULT,
      Importance.LOW, MqttConfigConstants.KEEP_ALIVE_INTERVAL_DOC,
      "Connection", 7, ConfigDef.Width.MEDIUM, MqttConfigConstants.KEEP_ALIVE_INTERVAL_DISPLAY)
    .define(MqttConfigConstants.CLIENT_ID_CONFIG, Type.STRING, null, Importance.LOW, MqttConfigConstants.CLIENT_ID_DOC,
      "Connection", 8, ConfigDef.Width.MEDIUM, MqttConfigConstants.CLIENT_ID_DISPLAY)


    //ssl
    .define(MqttConfigConstants.SSL_CA_CERT_CONFIG, Type.STRING, null, Importance.MEDIUM, MqttConfigConstants.SSL_CA_CERT_DOC,
      "SSL", 1, ConfigDef.Width.MEDIUM, MqttConfigConstants.SSL_CA_CERT_DISPLAY)
    .define(MqttConfigConstants.SSL_CERT_CONFIG, Type.STRING, null, Importance.MEDIUM, MqttConfigConstants.SSL_CERT_DOC,
      "SSL", 2, ConfigDef.Width.MEDIUM, MqttConfigConstants.SSL_CERT_DISPLAY)
    .define(MqttConfigConstants.SSL_CERT_KEY_CONFIG, Type.STRING, null, Importance.MEDIUM, MqttConfigConstants.SSL_CERT_KEY_DOC,
      "SSL", 3, ConfigDef.Width.MEDIUM, MqttConfigConstants.SSL_CERT_KEY_DISPLAY)


    //kcql
    .define(MqttConfigConstants.KCQL_CONFIG, Type.STRING, Importance.HIGH, MqttConfigConstants.KCQL_DOC,
      "KCQL", 1, ConfigDef.Width.MEDIUM, MqttConfigConstants.KCQL_DISPLAY)

    //converter
    .define(MqttConfigConstants.CONVERTER_CONFIG, Type.STRING, null, Importance.HIGH, MqttConfigConstants.CONVERTER_DOC,
      "Converter", 1, ConfigDef.Width.MEDIUM, MqttConfigConstants.CONVERTER_DISPLAY)
    .define(MqttConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG, Type.BOOLEAN, MqttConfigConstants.THROW_ON_CONVERT_ERRORS_DEFAULT,
      Importance.HIGH, MqttConfigConstants.THROW_ON_CONVERT_ERRORS_DOC,
      "Converter", 2, ConfigDef.Width.MEDIUM, MqttConfigConstants.THROW_ON_CONVERT_ERRORS_DISPLAY)

    .define(MqttConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, MqttConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, MqttConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, MqttConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

case class MqttSourceConfig(props: util.Map[String, String])
  extends BaseConfig(MqttConfigConstants.CONNECTOR_PREFIX, MqttSourceConfig.config, props)
    with KcqlSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with ConnectionSettings

