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
package io.lenses.streamreactor.connect.jms.config

import io.lenses.streamreactor.common.config.KcqlWithFieldsSettings
import io.lenses.streamreactor.common.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

object JMSConfig {

  val config: ConfigDef = new ConfigDef()
    .define(
      JMSConfigConstants.JMS_URL,
      Type.STRING,
      Importance.HIGH,
      JMSConfigConstants.JMS_URL_DOC,
      "Connection",
      1,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.JMS_URL,
    )
    .define(
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY,
      Type.STRING,
      Importance.HIGH,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY_DOC,
      "Connection",
      2,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.INITIAL_CONTEXT_FACTORY,
    )
    .define(
      JMSConfigConstants.CONNECTION_FACTORY,
      Type.STRING,
      JMSConfigConstants.CONNECTION_FACTORY_DEFAULT,
      Importance.HIGH,
      JMSConfigConstants.CONNECTION_FACTORY_DOC,
      "Connection",
      3,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.CONNECTION_FACTORY,
    )
    .define(
      JMSConfigConstants.KCQL,
      Type.STRING,
      Importance.HIGH,
      JMSConfigConstants.KCQL,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.KCQL,
    )
    .define(
      JMSConfigConstants.TOPIC_SUBSCRIPTION_NAME,
      Type.STRING,
      null,
      Importance.HIGH,
      JMSConfigConstants.TOPIC_SUBSCRIPTION_NAME_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.TOPIC_SUBSCRIPTION_NAME,
    )
    .define(
      JMSConfigConstants.JMS_PASSWORD,
      Type.PASSWORD,
      null,
      Importance.HIGH,
      JMSConfigConstants.JMS_PASSWORD_DOC,
      "Connection",
      6,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.JMS_PASSWORD,
    )
    .define(
      JMSConfigConstants.JMS_USER,
      Type.STRING,
      null,
      Importance.HIGH,
      JMSConfigConstants.JMS_USER_DOC,
      "Connection",
      7,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.JMS_USER,
    )
    .define(
      JMSConfigConstants.ERROR_POLICY,
      Type.STRING,
      JMSConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      JMSConfigConstants.ERROR_POLICY_DOC,
      "Connection",
      8,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.ERROR_POLICY,
    )
    .define(
      JMSConfigConstants.ERROR_RETRY_INTERVAL,
      Type.INT,
      JMSConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Connection",
      9,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.ERROR_RETRY_INTERVAL,
    )
    .define(
      JMSConfigConstants.NBR_OF_RETRIES,
      Type.INT,
      JMSConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.NBR_OF_RETRIES_DOC,
      "Connection",
      10,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.NBR_OF_RETRIES,
    )
    .define(
      JMSConfigConstants.DESTINATION_SELECTOR,
      Type.STRING,
      JMSConfigConstants.DESTINATION_SELECTOR_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.DESTINATION_SELECTOR_DOC,
      "Connection",
      11,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.DESTINATION_SELECTOR,
    )
    .define(
      JMSConfigConstants.EXTRA_PROPS,
      Type.LIST,
      JMSConfigConstants.EXTRA_PROPS_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.EXTRA_PROPS_DOC,
      "Connection",
      12,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.EXTRA_PROPS,
    )
    .define(
      JMSConfigConstants.BATCH_SIZE,
      Type.INT,
      JMSConfigConstants.BATCH_SIZE_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.BATCH_SIZE_DOC,
      "Connection",
      13,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.BATCH_SIZE,
    )
    .define(
      JMSConfigConstants.POLLING_TIMEOUT_CONFIG,
      Type.LONG,
      JMSConfigConstants.POLLING_TIMEOUT_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.POLLING_TIMEOUT_DOC,
      "Connection",
      14,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.POLLING_TIMEOUT_CONFIG,
    )
    //converters
    .define(
      JMSConfigConstants.DEFAULT_SOURCE_CONVERTER_CONFIG,
      Type.STRING,
      "",
      Importance.HIGH,
      JMSConfigConstants.DEFAULT_SOURCE_CONVERTER_DOC,
      "Converter",
      1,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.DEFAULT_SOURCE_CONVERTER_DISPLAY,
    )
    .define(
      JMSConfigConstants.DEFAULT_SINK_CONVERTER_CONFIG,
      Type.STRING,
      "",
      Importance.HIGH,
      JMSConfigConstants.DEFAULT_SINK_CONVERTER_DOC,
      "Converter",
      1,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.DEFAULT_SINK_CONVERTER_DISPLAY,
    )
    .define(
      JMSConfigConstants.THROW_ON_CONVERT_ERRORS_CONFIG,
      Type.BOOLEAN,
      JMSConfigConstants.THROW_ON_CONVERT_ERRORS_DEFAULT,
      Importance.HIGH,
      JMSConfigConstants.THROW_ON_CONVERT_ERRORS_DOC,
      "Converter",
      2,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.THROW_ON_CONVERT_ERRORS_DISPLAY,
    )
    .define(
      JMSConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES,
      Type.STRING,
      JMSConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES_DEFAULT,
      Importance.HIGH,
      JMSConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES_DOC,
      "Converter",
      3,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.AVRO_CONVERTERS_SCHEMA_FILES,
    )
    .define(
      JMSConfigConstants.HEADERS_CONFIG,
      Type.STRING,
      "",
      Importance.LOW,
      JMSConfigConstants.HEADERS_CONFIG_DOC,
      "Converter",
      4,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.HEADERS_CONFIG_DISPLAY,
    )
    .define(
      JMSConfigConstants.PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      JMSConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY,
    )
    .define(
      JMSConfigConstants.EVICT_UNCOMMITTED_MINUTES,
      Type.INT,
      JMSConfigConstants.EVICT_UNCOMMITTED_MINUTES_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.EVICT_UNCOMMITTED_MINUTES_DOC,
      "Settings",
      1,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.EVICT_UNCOMMITTED_MINUTES_DOC,
    )
    .define(
      JMSConfigConstants.EVICT_THRESHOLD_MINUTES,
      Type.INT,
      JMSConfigConstants.EVICT_THRESHOLD_MINUTES_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.EVICT_THRESHOLD_MINUTES_DOC,
      "Settings",
      2,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.EVICT_THRESHOLD_MINUTES_DOC,
    )
    .define(
      JMSConfigConstants.TASK_PARALLELIZATION_TYPE,
      Type.STRING,
      JMSConfigConstants.TASK_PARALLELIZATION_TYPE_DEFAULT,
      Importance.MEDIUM,
      JMSConfigConstants.TASK_PARALLELIZATION_TYPE_DOC,
      "Settings",
      4,
      ConfigDef.Width.MEDIUM,
      JMSConfigConstants.TASK_PARALLELIZATION_TYPE_DOC,
    )
}

/**
  * <h1>JMSSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  */
case class JMSConfig(props: Map[String, String])
    extends BaseConfig(JMSConfigConstants.CONNECTOR_PREFIX, JMSConfig.config, props)
    with KcqlWithFieldsSettings
    with ErrorPolicySettings
    with NumberRetriesSettings
    with UserSettings
    with UrlSettings
