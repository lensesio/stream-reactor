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

package com.datamountaineer.streamreactor.connect.pulsar.config

import java.util

import com.datamountaineer.streamreactor.connect.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}

object PulsarConfig {
  val config: ConfigDef = new ConfigDef()
    .define(PulsarConfigConstants.HOSTS_CONFIG, Type.STRING, Importance.HIGH, PulsarConfigConstants.HOSTS_DOC,
      "Connection", 1, ConfigDef.Width.MEDIUM, PulsarConfigConstants.HOSTS_DISPLAY)

    //ssl
    .define(PulsarConfigConstants.SSL_CA_CERT_CONFIG, Type.STRING, null, Importance.MEDIUM, PulsarConfigConstants.SSL_CA_CERT_DOC,
      "TLS", 1, ConfigDef.Width.MEDIUM, PulsarConfigConstants.SSL_CA_CERT_DISPLAY)
    .define(PulsarConfigConstants.SSL_CERT_CONFIG, Type.STRING, null, Importance.MEDIUM, PulsarConfigConstants.SSL_CERT_DOC,
      "TLS", 2, ConfigDef.Width.MEDIUM, PulsarConfigConstants.SSL_CERT_DISPLAY)
    .define(PulsarConfigConstants.SSL_CERT_KEY_CONFIG, Type.STRING, null, Importance.MEDIUM, PulsarConfigConstants.SSL_CERT_KEY_DOC,
      "TLS", 3, ConfigDef.Width.MEDIUM, PulsarConfigConstants.SSL_CERT_KEY_DISPLAY)

    //kcql
    .define(PulsarConfigConstants.KCQL_CONFIG, Type.STRING, Importance.HIGH, PulsarConfigConstants.KCQL_DOC,
      "KCQL", 1, ConfigDef.Width.MEDIUM, PulsarConfigConstants.KCQL_DISPLAY)

    .define(PulsarConfigConstants.PROGRESS_COUNTER_ENABLED, Type.BOOLEAN, PulsarConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM, PulsarConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics", 1, ConfigDef.Width.MEDIUM, PulsarConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

object PulsarSourceConfig {
  val config = PulsarConfig.config
    //converter
    .define(PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG, Type.BOOLEAN, PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_DEFAULT,
      Importance.HIGH, PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_DOC,
      "Converter", 1, ConfigDef.Width.MEDIUM, PulsarConfigConstants.THROW_ON_CONVERT_ERRORS_DISPLAY)
    .define(PulsarConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES, Type.STRING, PulsarConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES_DEFAULT,
      Importance.HIGH, PulsarConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES_DOC, "Converter", 3, ConfigDef.Width.MEDIUM,
      PulsarConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES)
    //manager
    .define(PulsarConfigConstants.POLLING_TIMEOUT_CONFIG, Type.INT, PulsarConfigConstants.POLLING_TIMEOUT_DEFAULT,
      Importance.LOW, PulsarConfigConstants.POLLING_TIMEOUT_DOC,
      "Manager", 1, ConfigDef.Width.MEDIUM, PulsarConfigConstants.POLLING_TIMEOUT_DISPLAY)
    .define(PulsarConfigConstants.INTERNAL_BATCH_SIZE, Type.INT, PulsarConfigConstants.INTERNAL_BATCH_SIZE_DEFAULT,
      Importance.LOW, PulsarConfigConstants.INTERNAL_BATCH_SIZE_DOC,
      "Manager", 2, ConfigDef.Width.MEDIUM, PulsarConfigConstants.INTERNAL_BATCH_SIZE)

}

case class PulsarSourceConfig(props: util.Map[String, String])
  extends BaseConfig(PulsarConfigConstants.CONNECTOR_PREFIX, PulsarSourceConfig.config, props) with PulsarConfigBase

object PulsarSinkConfig {
  val config = PulsarConfig.config

    .define(PulsarConfigConstants.ERROR_POLICY, Type.STRING, PulsarConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH, PulsarConfigConstants.ERROR_POLICY_DOC,
      "Connection", 9, ConfigDef.Width.MEDIUM, PulsarConfigConstants.ERROR_POLICY)

    .define(PulsarConfigConstants.ERROR_RETRY_INTERVAL, Type.INT, PulsarConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM, PulsarConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection", 10, ConfigDef.Width.MEDIUM, PulsarConfigConstants.ERROR_RETRY_INTERVAL)

    .define(PulsarConfigConstants.NBR_OF_RETRIES, Type.INT, PulsarConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM, PulsarConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection", 11, ConfigDef.Width.MEDIUM, PulsarConfigConstants.NBR_OF_RETRIES)
}

case class PulsarSinkConfig(props: util.Map[String, String])
  extends BaseConfig(PulsarConfigConstants.CONNECTOR_PREFIX, PulsarSinkConfig.config, props) with PulsarConfigBase

case class PulsarConfig(props: util.Map[String, String])
  extends BaseConfig(PulsarConfigConstants.CONNECTOR_PREFIX, PulsarConfig.config, props) with PulsarConfigBase

trait PulsarConfigBase extends KcqlSettings
  with NumberRetriesSettings
  with ErrorPolicySettings
  with SSLSettings
  with ConnectionSettings
  with UserSettings