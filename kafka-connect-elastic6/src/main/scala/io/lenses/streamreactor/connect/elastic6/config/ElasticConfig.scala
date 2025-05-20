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
package io.lenses.streamreactor.connect.elastic6.config

import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.config.base.traits.BaseConfig
import io.lenses.streamreactor.common.config.base.traits.ErrorPolicySettings
import io.lenses.streamreactor.common.config.base.traits.NumberRetriesSettings
import io.lenses.streamreactor.common.config.base.traits.WriteTimeoutSettings
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

object ElasticConfig {

  val config: ConfigDef = new ConfigDef()
    .define(
      ElasticConfigConstants.PROTOCOL,
      Type.STRING,
      ElasticConfigConstants.PROTOCOL_DEFAULT,
      Importance.LOW,
      ElasticConfigConstants.PROTOCOL_DOC,
      "Connection",
      1,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.PROTOCOL,
    )
    .define(
      ElasticConfigConstants.HOSTS,
      Type.STRING,
      ElasticConfigConstants.HOSTS_DEFAULT,
      Importance.HIGH,
      ElasticConfigConstants.HOSTS_DOC,
      "Connection",
      2,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.HOSTS,
    )
    .define(
      ElasticConfigConstants.ES_PORT,
      Type.INT,
      ElasticConfigConstants.ES_PORT_DEFAULT,
      Importance.HIGH,
      ElasticConfigConstants.ES_PORT_DOC,
      "Connection",
      3,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.HOSTS,
    )
    .define(
      ElasticConfigConstants.ES_PREFIX,
      Type.STRING,
      ElasticConfigConstants.ES_PREFIX_DEFAULT,
      Importance.HIGH,
      ElasticConfigConstants.ES_PREFIX_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.HOSTS,
    )
    .define(
      ElasticConfigConstants.ES_CLUSTER_NAME,
      Type.STRING,
      ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT,
      Importance.HIGH,
      ElasticConfigConstants.ES_CLUSTER_NAME_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.ES_CLUSTER_NAME,
    )
    .define(
      ElasticConfigConstants.WRITE_TIMEOUT_CONFIG,
      Type.INT,
      ElasticConfigConstants.WRITE_TIMEOUT_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.WRITE_TIMEOUT_DOC,
      "Connection",
      6,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.WRITE_TIMEOUT_DISPLAY,
    )
    .define(
      ElasticConfigConstants.BATCH_SIZE_CONFIG,
      Type.INT,
      ElasticConfigConstants.BATCH_SIZE_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.BATCH_SIZE_DOC,
      "Connection",
      7,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.BATCH_SIZE_DISPLAY,
    )
    .define(
      ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME,
      Type.STRING,
      ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME_DEFAULT,
      Importance.LOW,
      ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME_DOC,
      "Connection",
      8,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_USERNAME,
    )
    .define(
      ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_PASSWORD,
      Type.STRING,
      ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_PASSWORD_DEFAULT,
      Importance.LOW,
      ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_PASSWORD_DOC,
      "Connection",
      9,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.CLIENT_HTTP_BASIC_AUTH_PASSWORD,
    )
    .define(
      ElasticConfigConstants.ERROR_POLICY_CONFIG,
      Type.STRING,
      ElasticConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      ElasticConfigConstants.ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.ERROR_POLICY_CONFIG,
    )
    .define(
      ElasticConfigConstants.NBR_OF_RETRIES_CONFIG,
      Type.INT,
      ElasticConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.SHORT,
      ElasticConfigConstants.NBR_OF_RETRIES_CONFIG,
    )
    .define(
      ElasticConfigConstants.ERROR_RETRY_INTERVAL,
      Type.INT,
      ElasticConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      ElasticConfigConstants.ERROR_RETRY_INTERVAL,
    )
    .define(
      ElasticConfigConstants.KCQL,
      Type.STRING,
      Importance.HIGH,
      ElasticConfigConstants.KCQL_DOC,
      "KCQL",
      1,
      ConfigDef.Width.LONG,
      ElasticConfigConstants.KCQL,
    )
    .define(
      ElasticConfigConstants.PK_JOINER_SEPARATOR,
      Type.STRING,
      ElasticConfigConstants.PK_JOINER_SEPARATOR_DEFAULT,
      Importance.LOW,
      ElasticConfigConstants.PK_JOINER_SEPARATOR_DOC,
      "KCQL",
      2,
      ConfigDef.Width.SHORT,
      ElasticConfigConstants.PK_JOINER_SEPARATOR,
    )
    .define(
      ElasticConfigConstants.PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      ElasticConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      ElasticConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      ElasticConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY,
    )
    .withClientSslSupport()
}

/**
  * <h1>ElasticSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  */
case class ElasticConfig(props: Map[String, String])
    extends BaseConfig(ElasticConfigConstants.CONNECTOR_PREFIX, ElasticConfig.config, props)
    with WriteTimeoutSettings
    with ErrorPolicySettings
    with NumberRetriesSettings {
  val kcqlConstant: String = ElasticConfigConstants.KCQL

  def getKcql(): Seq[Kcql] =
    getString(kcqlConstant).split(";").filter(_.trim.nonEmpty).map(Kcql.parse).toIndexedSeq
}
