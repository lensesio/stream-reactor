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

package com.datamountaineer.streamreactor.connect.elastic.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}


object ElasticSinkConfig {

  val config: ConfigDef = new ConfigDef()
    .define(
      ElasticSinkConfigConstants.URL,
      Type.STRING,
      ElasticSinkConfigConstants.URL_DEFAULT,
      Importance.HIGH,
      ElasticSinkConfigConstants.URL_DOC,
      "Connection",
      1,
      ConfigDef.Width.MEDIUM,
      ElasticSinkConfigConstants.URL)
    .define(
      ElasticSinkConfigConstants.ES_CLUSTER_NAME,
      Type.STRING,
      ElasticSinkConfigConstants.ES_CLUSTER_NAME_DEFAULT,
      Importance.HIGH,
      ElasticSinkConfigConstants.ES_CLUSTER_NAME_DOC,
      "Connection",
      2,
      ConfigDef.Width.MEDIUM,
      ElasticSinkConfigConstants.ES_CLUSTER_NAME)
    .define(
      ElasticSinkConfigConstants.URL_PREFIX,
      Type.STRING,
      ElasticSinkConfigConstants.URL_PREFIX_DEFAULT,
      Importance.LOW,
      ElasticSinkConfigConstants.URL_PREFIX_DOC,
      "Connection",
      3,
      ConfigDef.Width.MEDIUM,
      ElasticSinkConfigConstants.URL_PREFIX)
    .define(
      ElasticSinkConfigConstants.WRITE_TIMEOUT_CONFIG,
      Type.INT,
      ElasticSinkConfigConstants.WRITE_TIMEOUT_DEFAULT,
      Importance.MEDIUM,
      ElasticSinkConfigConstants.WRITE_TIMEOUT_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      ElasticSinkConfigConstants.WRITE_TIMEOUT_DISPLAY)
    .define(
      ElasticSinkConfigConstants.ERROR_POLICY_CONFIG,
      Type.STRING,
      ElasticSinkConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      ElasticSinkConfigConstants.ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.MEDIUM,
      ElasticSinkConfigConstants.ERROR_POLICY_CONFIG)
    .define(
      ElasticSinkConfigConstants.NBR_OF_RETRIES_CONFIG,
      Type.INT,
      ElasticSinkConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      ElasticSinkConfigConstants.NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.SHORT,
      ElasticSinkConfigConstants.NBR_OF_RETRIES_CONFIG
    )
    .define(
      ElasticSinkConfigConstants.KCQL,
      Type.STRING,
      Importance.HIGH,
      ElasticSinkConfigConstants.KCQL_DOC,
      "KCQL",
      1,
      ConfigDef.Width.LONG,
      ElasticSinkConfigConstants.KCQL)
    .define(
      ElasticSinkConfigConstants.ES_CLUSTER_XPACK_SETTINGS,
      Type.STRING,
      ElasticSinkConfigConstants.ES_CLUSTER_XPACK_SETTINGS_DEFAULT,
      Importance.MEDIUM,
      ElasticSinkConfigConstants.ES_CLUSTER_XPACK_SETTINGS_DOC,
      "XPack",
      1,
      ConfigDef.Width.LONG,
      ElasticSinkConfigConstants.ES_CLUSTER_XPACK_SETTINGS_DOC)

    .define(
      ElasticSinkConfigConstants.ES_CLUSTER_XPACK_PLUGINS,
      Type.STRING,
      ElasticSinkConfigConstants.ES_CLUSTER_XPACK_PLUGINS_DEFAULT,
      Importance.MEDIUM,
      ElasticSinkConfigConstants.ES_CLUSTER_XPACK_PLUGINS_DOC,
      "XPack",
      2,
      ConfigDef.Width.LONG,
      ElasticSinkConfigConstants.ES_CLUSTER_XPACK_PLUGINS_DOC)
    .define(ElasticSinkConfigConstants.PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      ElasticSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      ElasticSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      ElasticSinkConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)
}

/**
  * <h1>ElasticSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class ElasticSinkConfig(props: util.Map[String, String]) extends AbstractConfig(ElasticSinkConfig.config, props)
