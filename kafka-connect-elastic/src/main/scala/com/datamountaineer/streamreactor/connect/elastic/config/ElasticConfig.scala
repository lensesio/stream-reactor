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

import com.datamountaineer.streamreactor.connect.config.base.traits.{BaseConfig, KcqlSettings, WriteTimeoutSettings}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}


object ElasticConfig {

  val config: ConfigDef = new ConfigDef()
    .define(
      ElasticConfigConstants.URL,
      Type.STRING,
      ElasticConfigConstants.URL_DEFAULT,
      Importance.HIGH,
      ElasticConfigConstants.URL_DOC,
      "Connection",
      1,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.URL)
    .define(
      ElasticConfigConstants.ES_CLUSTER_NAME,
      Type.STRING,
      ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT,
      Importance.HIGH,
      ElasticConfigConstants.ES_CLUSTER_NAME_DOC,
      "Connection",
      2,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.ES_CLUSTER_NAME)
    .define(
      ElasticConfigConstants.URL_PREFIX,
      Type.STRING,
      ElasticConfigConstants.URL_PREFIX_DEFAULT,
      Importance.LOW,
      ElasticConfigConstants.URL_PREFIX_DOC,
      "Connection",
      3,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.URL_PREFIX)
    .define(
      ElasticConfigConstants.WRITE_TIMEOUT_CONFIG,
      Type.INT,
      ElasticConfigConstants.WRITE_TIMEOUT_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.WRITE_TIMEOUT_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.WRITE_TIMEOUT_DISPLAY)
    .define(
      ElasticConfigConstants.THROW_ON_ERROR_CONFIG,
      Type.BOOLEAN,
      ElasticConfigConstants.THROW_ON_ERROR_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.THROW_ON_ERROR_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.THROW_ON_ERROR_DISPLAY)
    .define(
      ElasticConfigConstants.BATCH_SIZE_CONFIG,
      Type.INT,
      ElasticConfigConstants.BATCH_SIZE_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.BATCH_SIZE_DOC,
      "Connection",
      6,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.BATCH_SIZE_DISPLAY)
    .define(
      ElasticConfigConstants.KCQL_QUERY,
      Type.STRING,
      Importance.HIGH,
      ElasticConfigConstants.KCQL_DOC,
      "Target",
      1,
      ConfigDef.Width.LONG,
      ElasticConfigConstants.KCQL_QUERY)
    .define(ElasticConfigConstants.PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      ElasticConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY)

}

/**
  * <h1>ElasticSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  **/
case class ElasticConfig(props: util.Map[String, String])
  extends BaseConfig(ElasticConfigConstants.CONNECTOR_PREFIX, ElasticConfig.config, props)
    with KcqlSettings
    with WriteTimeoutSettings