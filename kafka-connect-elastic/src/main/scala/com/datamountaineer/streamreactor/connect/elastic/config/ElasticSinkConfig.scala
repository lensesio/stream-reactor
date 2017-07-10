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
      ElasticSinkConfigConstants.THROW_ON_ERROR_CONFIG,
      Type.BOOLEAN,
      ElasticSinkConfigConstants.THROW_ON_ERROR_DEFAULT,
      Importance.MEDIUM,
      ElasticSinkConfigConstants.THROW_ON_ERROR_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      ElasticSinkConfigConstants.THROW_ON_ERROR_DISPLAY)
    .define(
      ElasticSinkConfigConstants.EXPORT_ROUTE_QUERY,
      Type.STRING,
      Importance.HIGH,
      ElasticSinkConfigConstants.EXPORT_ROUTE_QUERY_DOC,
      "Target",
      1,
      ConfigDef.Width.LONG,
      ElasticSinkConfigConstants.EXPORT_ROUTE_QUERY)
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
