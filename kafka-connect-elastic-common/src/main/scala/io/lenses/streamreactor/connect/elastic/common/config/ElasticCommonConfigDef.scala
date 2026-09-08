/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.elastic.common.config

import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

/**
 * Builds the shared portion of the ConfigDef for all elastic/opensearch connectors.
 *
 * @param prefix      The connector-specific prefix, e.g. "connect.elastic" or "connect.opensearch"
 * @param portDefault The default port value (elastic6: 9200, elastic7: 9300, opensearch: 9200)
 * @param includeClusterName Whether to include the legacy cluster.name key (elastic6/7: true, opensearch: false)
 */
object ElasticCommonConfigDef {

  def builder(prefix: String, portDefault: Int, includeClusterName: Boolean = true): ConfigDef = {
    val base = new ConfigDef()
      .define(
        s"$prefix.$PROTOCOL_SUFFIX",
        Type.STRING,
        PROTOCOL_DEFAULT,
        Importance.LOW,
        PROTOCOL_DOC,
        "Connection",
        1,
        ConfigDef.Width.MEDIUM,
        s"$prefix.$PROTOCOL_SUFFIX",
      )
      .define(
        s"$prefix.$HOSTS_SUFFIX",
        Type.STRING,
        HOSTS_DEFAULT,
        Importance.HIGH,
        HOSTS_DOC,
        "Connection",
        2,
        ConfigDef.Width.MEDIUM,
        s"$prefix.$HOSTS_SUFFIX",
      )
      .define(
        s"$prefix.$ES_PORT_SUFFIX",
        Type.INT,
        portDefault,
        Importance.HIGH,
        ES_PORT_DOC,
        "Connection",
        3,
        ConfigDef.Width.MEDIUM,
        s"$prefix.$ES_PORT_SUFFIX",
      )
      .define(
        s"$prefix.$ES_PREFIX_SUFFIX",
        Type.STRING,
        ES_PREFIX_DEFAULT,
        Importance.HIGH,
        ES_PREFIX_DOC,
        "Connection",
        4,
        ConfigDef.Width.MEDIUM,
        s"$prefix.$ES_PREFIX_SUFFIX",
      )
      .define(
        s"$prefix.$WRITE_TIMEOUT_SUFFIX_",
        Type.INT,
        WRITE_TIMEOUT_DEFAULT,
        ConfigDef.Range.atLeast(WRITE_TIMEOUT_MIN),
        Importance.MEDIUM,
        WRITE_TIMEOUT_DOC,
        "Connection",
        6,
        ConfigDef.Width.MEDIUM,
        WRITE_TIMEOUT_DISPLAY,
      )
      .define(
        s"$prefix.$BATCH_SIZE_SUFFIX",
        Type.INT,
        BATCH_SIZE_DEFAULT,
        Importance.MEDIUM,
        BATCH_SIZE_DOC,
        "Connection",
        7,
        ConfigDef.Width.MEDIUM,
        BATCH_SIZE_DISPLAY,
      )
      .define(
        s"$prefix.$CLIENT_HTTP_BASIC_AUTH_USERNAME_SUFFIX",
        Type.STRING,
        CLIENT_HTTP_BASIC_AUTH_USERNAME_DEFAULT,
        Importance.LOW,
        CLIENT_HTTP_BASIC_AUTH_USERNAME_DOC,
        "Connection",
        8,
        ConfigDef.Width.MEDIUM,
        s"$prefix.$CLIENT_HTTP_BASIC_AUTH_USERNAME_SUFFIX",
      )
      .define(
        s"$prefix.$CLIENT_HTTP_BASIC_AUTH_PASSWORD_SUFFIX",
        Type.PASSWORD,
        CLIENT_HTTP_BASIC_AUTH_PASSWORD_DEFAULT,
        Importance.LOW,
        CLIENT_HTTP_BASIC_AUTH_PASSWORD_DOC,
        "Connection",
        9,
        ConfigDef.Width.MEDIUM,
        s"$prefix.$CLIENT_HTTP_BASIC_AUTH_PASSWORD_SUFFIX",
      )
      .define(
        s"$prefix.$ERROR_POLICY_SUFFIX",
        Type.STRING,
        ERROR_POLICY_DEFAULT,
        Importance.HIGH,
        ERROR_POLICY_DOC,
        "Error",
        1,
        ConfigDef.Width.MEDIUM,
        s"$prefix.$ERROR_POLICY_SUFFIX",
      )
      .define(
        s"$prefix.$NBR_OF_RETRIES_SUFFIX",
        Type.INT,
        NBR_OF_RETIRES_DEFAULT,
        Importance.MEDIUM,
        NBR_OF_RETRIES_DOC,
        "Error",
        2,
        ConfigDef.Width.SHORT,
        s"$prefix.$NBR_OF_RETRIES_SUFFIX",
      )
      .define(
        s"$prefix.$ERROR_RETRY_INTERVAL_SUFFIX",
        Type.LONG,
        ERROR_RETRY_INTERVAL_DEFAULT,
        Importance.MEDIUM,
        ERROR_RETRY_INTERVAL_DOC,
        "Error",
        3,
        ConfigDef.Width.LONG,
        s"$prefix.$ERROR_RETRY_INTERVAL_SUFFIX",
      )
      .define(
        s"$prefix.$KCQL_SUFFIX",
        Type.STRING,
        Importance.HIGH,
        KCQL_DOC,
        "KCQL",
        1,
        ConfigDef.Width.LONG,
        s"$prefix.$KCQL_SUFFIX",
      )
      .define(
        s"$prefix.$PK_JOINER_SEPARATOR_SUFFIX",
        Type.STRING,
        PK_JOINER_SEPARATOR_DEFAULT,
        Importance.LOW,
        PK_JOINER_SEPARATOR_DOC,
        "KCQL",
        2,
        ConfigDef.Width.SHORT,
        s"$prefix.$PK_JOINER_SEPARATOR_SUFFIX",
      )
      .define(
        PROGRESS_COUNTER_ENABLED,
        Type.BOOLEAN,
        PROGRESS_COUNTER_ENABLED_DEFAULT,
        Importance.MEDIUM,
        PROGRESS_COUNTER_ENABLED_DOC,
        "Metrics",
        1,
        ConfigDef.Width.MEDIUM,
        PROGRESS_COUNTER_ENABLED_DISPLAY,
      )
      .withClientSslSupport()

    if (includeClusterName) {
      base.define(
        s"$prefix.$ES_CLUSTER_NAME_SUFFIX",
        Type.STRING,
        ES_CLUSTER_NAME_DEFAULT,
        Importance.HIGH,
        ES_CLUSTER_NAME_DOC,
        "Connection",
        5,
        ConfigDef.Width.MEDIUM,
        s"$prefix.$ES_CLUSTER_NAME_SUFFIX",
      )
    } else {
      base
    }
  }
}
