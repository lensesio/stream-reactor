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

package com.datamountaineer.streamreactor.connect.elastic6.config

import java.util

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.config.base.traits._
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
      ElasticConfigConstants.CLIENT_TYPE_CONFIG,
      Type.STRING,
      ElasticConfigConstants.CLIENT_TYPE_CONFIG_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.CLIENT_TYPE_CONFIG_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.CLIENT_TYPE_CONFIG)
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
      ElasticConfigConstants.ERROR_POLICY_CONFIG,
      Type.STRING,
      ElasticConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      ElasticConfigConstants.ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.ERROR_POLICY_CONFIG)
    .define(
      ElasticConfigConstants.NBR_OF_RETRIES_CONFIG,
      Type.INT,
      ElasticConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.SHORT,
      ElasticConfigConstants.NBR_OF_RETRIES_CONFIG
    )
    .define(ElasticConfigConstants.ERROR_RETRY_INTERVAL, Type.INT,
      ElasticConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT, Importance.MEDIUM,
      ElasticConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Error", 3, ConfigDef.Width.LONG, ElasticConfigConstants.ERROR_RETRY_INTERVAL)
    .define(
      ElasticConfigConstants.KCQL,
      Type.STRING,
      Importance.HIGH,
      ElasticConfigConstants.KCQL_DOC,
      "KCQL",
      1,
      ConfigDef.Width.LONG,
      ElasticConfigConstants.KCQL)
    .define(
      ElasticConfigConstants.PK_JOINER_SEPARATOR,
      Type.STRING,
      ElasticConfigConstants.PK_JOINER_SEPARATOR_DEFAULT,
      Importance.LOW,
      ElasticConfigConstants.PK_JOINER_SEPARATOR_DOC,
      "KCQL",
      2,
      ConfigDef.Width.SHORT,
      ElasticConfigConstants.PK_JOINER_SEPARATOR)
    .define(
      ElasticConfigConstants.ES_CLUSTER_XPACK_SETTINGS,
      Type.PASSWORD,
      ElasticConfigConstants.ES_CLUSTER_XPACK_SETTINGS_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.ES_CLUSTER_XPACK_SETTINGS_DOC,
      "XPack",
      1,
      ConfigDef.Width.LONG,
      ElasticConfigConstants.ES_CLUSTER_XPACK_SETTINGS_DOC)

    .define(
      ElasticConfigConstants.ES_CLUSTER_XPACK_PLUGINS,
      Type.STRING,
      ElasticConfigConstants.ES_CLUSTER_XPACK_PLUGINS_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.ES_CLUSTER_XPACK_PLUGINS_DOC,
      "XPack",
      2,
      ConfigDef.Width.LONG,
      ElasticConfigConstants.ES_CLUSTER_XPACK_PLUGINS_DOC)
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
    with WriteTimeoutSettings
    with ErrorPolicySettings
    with NumberRetriesSettings {
  val kcqlConstant: String = ElasticConfigConstants.KCQL

  def getKcql(): Seq[Kcql] = {
    getString(kcqlConstant).split(";").filter(_.trim.nonEmpty).map(Kcql.parse)
  }
}
