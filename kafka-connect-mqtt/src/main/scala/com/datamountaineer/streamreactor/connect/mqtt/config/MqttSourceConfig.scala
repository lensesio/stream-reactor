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

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */
object MqttSourceConfig {
  val config: ConfigDef = new ConfigDef()
    .define(MqttSourceConfigConstants.HOSTS_CONFIG, Type.STRING, Importance.HIGH, MqttSourceConfigConstants.HOSTS_DOC, "Connection", 1, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.HOSTS_DISPLAY)
    .define(MqttSourceConfigConstants.USER_CONFIG, Type.STRING, null, Importance.HIGH, MqttSourceConfigConstants.USER_DOC, "Connection", 2, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.USER_DISPLAY)
    .define(MqttSourceConfigConstants.PASSWORD_CONFIG, Type.PASSWORD, null, Importance.HIGH, MqttSourceConfigConstants.PASSWORD_DOC, "Connection", 3, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.PASSWORD_DISPLAY)
    .define(MqttSourceConfigConstants.QS_CONFIG, Type.INT, Importance.MEDIUM, MqttSourceConfigConstants.QS_DOC, "Connection", 4, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.QS_DISPLAY)
    .define(MqttSourceConfigConstants.CONNECTION_TIMEOUT_CONFIG, Type.INT, MqttSourceConfigConstants.CONNECTION_TIMEOUT_DEFAULT, Importance.LOW, MqttSourceConfigConstants.CONNECTION_TIMEOUT_DOC, "Connection", 5, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.CONNECTION_TIMEOUT_DISPLAY)
    .define(MqttSourceConfigConstants.CLEAN_SESSION_CONFIG, Type.BOOLEAN, MqttSourceConfigConstants.CLEAN_CONNECTION_DEFAULT, Importance.LOW, MqttSourceConfigConstants.CLEAN_SESSION_CONFIG, "Connection", 6, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.CLEAN_CONNECTION_DISPLAY)
    .define(MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_CONFIG, Type.INT, MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_DEFAULT, Importance.LOW, MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_DOC, "Connection", 7, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.KEEP_ALIVE_INTERVAL_DISPLAY)
    .define(MqttSourceConfigConstants.CLIENT_ID_CONFIG, Type.STRING, null, Importance.LOW, MqttSourceConfigConstants.CLIENT_ID_DOC, "Connection", 8, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.CLIENT_ID_DISPLAY)


    //ssl
    .define(MqttSourceConfigConstants.SSL_CA_CERT_CONFIG, Type.STRING, null, Importance.MEDIUM, MqttSourceConfigConstants.SSL_CA_CERT_DOC, "SSL", 1, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.SSL_CA_CERT_DISPLAY)
    .define(MqttSourceConfigConstants.SSL_CERT_CONFIG, Type.STRING, null, Importance.MEDIUM, MqttSourceConfigConstants.SSL_CERT_DOC, "SSL", 2, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.SSL_CERT_DISPLAY)
    .define(MqttSourceConfigConstants.SSL_CERT_KEY_CONFIG, Type.STRING, null, Importance.MEDIUM, MqttSourceConfigConstants.SSL_CERT_KEY_DOC, "SSL", 3, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.SSL_CERT_KEY_DISPLAY)


    //kcql
    .define(MqttSourceConfigConstants.KCQL_CONFIG, Type.STRING, Importance.HIGH, MqttSourceConfigConstants.KCQL_DOC, "KCQL", 1, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.KCQL_DISPLAY)

    //converter
    .define(MqttSourceConfigConstants.CONVERTER_CONFIG, Type.STRING, null, Importance.HIGH, MqttSourceConfigConstants.CONVERTER_DOC, "Converter", 1, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.CONVERTER_DISPLAY)
    .define(MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG, Type.BOOLEAN, MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_DEFAULT, Importance.HIGH, MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_DOC, "Converter", 2, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.THROW_ON_CONVERT_ERRORS_DISPLAY)

    .define(MqttSourceConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, MqttSourceConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, MqttSourceConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, MqttSourceConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

case class MqttSourceConfig(props: util.Map[String, String]) extends AbstractConfig(MqttSourceConfig.config, props)

