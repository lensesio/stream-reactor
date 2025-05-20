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
package io.lenses.streamreactor.connect.azure.documentdb.config

import com.microsoft.azure.documentdb.ConsistencyLevel
import io.lenses.streamreactor.common.config.KcqlWithFieldsSettings
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix
import io.lenses.streamreactor.common.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type

object DocumentDbConfig {
  val config: ConfigDef = new ConfigDef()
    .define(
      DocumentDbConfigConstants.CONNECTION_CONFIG,
      Type.STRING,
      Importance.HIGH,
      DocumentDbConfigConstants.CONNECTION_CONFIG_DOC,
      "Connection",
      1,
      ConfigDef.Width.LONG,
      DocumentDbConfigConstants.CONNECTION_DISPLAY,
    )
    .define(
      DocumentDbConfigConstants.MASTER_KEY_CONFIG,
      Type.PASSWORD,
      Importance.HIGH,
      DocumentDbConfigConstants.MASTER_KEY_DOC,
      "Connection",
      2,
      ConfigDef.Width.LONG,
      DocumentDbConfigConstants.MASTER_KEY_CONFIG,
    )
    .define(
      DocumentDbConfigConstants.CONSISTENCY_CONFIG,
      Type.STRING,
      DocumentDbConfigConstants.CONSISTENCY_DEFAULT,
      Importance.HIGH,
      DocumentDbConfigConstants.CONSISTENCY_DOC,
      "Connection",
      3,
      ConfigDef.Width.LONG,
      DocumentDbConfigConstants.CONSISTENCY_DISPLAY,
    )
    .define(
      DocumentDbConfigConstants.DATABASE_CONFIG,
      Type.STRING,
      Importance.HIGH,
      DocumentDbConfigConstants.DATABASE_CONFIG_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      DocumentDbConfigConstants.DATABASE_CONFIG,
    )
    .define(
      DocumentDbConfigConstants.CREATE_DATABASE_CONFIG,
      Type.BOOLEAN,
      DocumentDbConfigConstants.CREATE_DATABASE_DEFAULT,
      Importance.MEDIUM,
      DocumentDbConfigConstants.CREATE_DATABASE_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      DocumentDbConfigConstants.CREATE_DATABASE_DISPLAY,
    )
    .define(
      DocumentDbConfigConstants.PROXY_HOST_CONFIG,
      Type.STRING,
      null,
      Importance.LOW,
      DocumentDbConfigConstants.PROXY_HOST_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      DocumentDbConfigConstants.PROXY_HOST_DISPLAY,
    )
    .define(
      DocumentDbConfigConstants.ERROR_POLICY_CONFIG,
      Type.STRING,
      DocumentDbConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      DocumentDbConfigConstants.ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.LONG,
      DocumentDbConfigConstants.ERROR_POLICY_CONFIG,
    )
    .define(
      DocumentDbConfigConstants.NBR_OF_RETRIES_CONFIG,
      Type.INT,
      DocumentDbConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      DocumentDbConfigConstants.NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.LONG,
      DocumentDbConfigConstants.NBR_OF_RETRIES_CONFIG,
    )
    .define(
      DocumentDbConfigConstants.ERROR_RETRY_INTERVAL_CONFIG,
      Type.INT,
      DocumentDbConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      DocumentDbConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      DocumentDbConfigConstants.ERROR_RETRY_INTERVAL_CONFIG,
    )
    .define(
      DocumentDbConfigConstants.PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      DocumentDbConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      DocumentDbConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      DocumentDbConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY,
    )
  new io.lenses.streamreactor.common.config.base.KcqlSettings(new ConnectorPrefix(
    DocumentDbConfigConstants.CONNECTOR_PREFIX,
  )).withSettings(config)
}

case class DocumentDbConfig(props: Map[String, String])
    extends BaseConfig(DocumentDbConfigConstants.CONNECTOR_PREFIX, DocumentDbConfig.config, props)
    with KcqlWithFieldsSettings
    with DatabaseSettings
    with NumberRetriesSettings
    with ErrorPolicySettings
    with ConsistencyLevelSettings[ConsistencyLevel]
