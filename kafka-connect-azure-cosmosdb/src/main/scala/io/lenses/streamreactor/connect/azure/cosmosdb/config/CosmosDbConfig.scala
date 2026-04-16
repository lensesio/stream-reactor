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
package io.lenses.streamreactor.connect.azure.cosmosdb.config

import cats.implicits.toBifunctorOps
import com.azure.cosmos.ConsistencyLevel
import io.lenses.streamreactor.common.config.KcqlWithFieldsSettings
import io.lenses.streamreactor.common.config.base.model.ConnectorPrefix
import io.lenses.streamreactor.common.config.base.traits._
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.Importance
import org.apache.kafka.common.config.ConfigDef.Type
import org.apache.kafka.common.config.ConfigException

import scala.util.Try

object CosmosDbConfig {

  def apply(props: Map[String, String]): Either[ConfigException, CosmosDbConfig] =
    Try(new CosmosDbConfig(props)).toEither.leftMap {
      case e: ConfigException => e
      case e => new ConfigException(e.getMessage, e)
    }

  val config: ConfigDef = new ConfigDef()
    .define(
      CosmosDbConfigConstants.CONNECTION_CONFIG,
      Type.STRING,
      Importance.HIGH,
      CosmosDbConfigConstants.CONNECTION_CONFIG_DOC,
      "Connection",
      1,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.CONNECTION_DISPLAY,
    )
    .define(
      CosmosDbConfigConstants.MASTER_KEY_CONFIG,
      Type.PASSWORD,
      Importance.HIGH,
      CosmosDbConfigConstants.MASTER_KEY_DOC,
      "Connection",
      2,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.MASTER_KEY_CONFIG,
    )
    .define(
      CosmosDbConfigConstants.CONSISTENCY_CONFIG,
      Type.STRING,
      CosmosDbConfigConstants.CONSISTENCY_DEFAULT,
      Importance.HIGH,
      CosmosDbConfigConstants.CONSISTENCY_DOC,
      "Connection",
      3,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.CONSISTENCY_DISPLAY,
    )
    .define(
      CosmosDbConfigConstants.DATABASE_CONFIG,
      Type.STRING,
      Importance.HIGH,
      CosmosDbConfigConstants.DATABASE_CONFIG_DOC,
      "Connection",
      4,
      ConfigDef.Width.MEDIUM,
      CosmosDbConfigConstants.DATABASE_CONFIG,
    )
    .define(
      CosmosDbConfigConstants.CREATE_DATABASE_CONFIG,
      Type.BOOLEAN,
      CosmosDbConfigConstants.CREATE_DATABASE_DEFAULT,
      Importance.MEDIUM,
      CosmosDbConfigConstants.CREATE_DATABASE_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      CosmosDbConfigConstants.CREATE_DATABASE_DISPLAY,
    )
    .define(
      CosmosDbConfigConstants.KEY_SOURCE_CONFIG,
      Type.STRING,
      CosmosDbConfigConstants.KEY_SOURCE_DEFAULT,
      Importance.MEDIUM,
      CosmosDbConfigConstants.KEY_SOURCE_DOC,
      "Key Source",
      6,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.KEY_SOURCE_DISPLAY,
    )
    .define(
      CosmosDbConfigConstants.KEY_PATH_CONFIG,
      Type.STRING,
      CosmosDbConfigConstants.KEY_PATH_DEFAULT,
      Importance.MEDIUM,
      CosmosDbConfigConstants.KEY_PATH_DOC,
      "Key Source",
      7,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.KEY_PATH_DISPLAY,
    )
    .define(
      CosmosDbConfigConstants.BULK_CONFIG,
      Type.BOOLEAN,
      CosmosDbConfigConstants.BULK_DEFAULT,
      Importance.MEDIUM,
      CosmosDbConfigConstants.BULK_DOC,
      "Bulk",
      8,
      ConfigDef.Width.SHORT,
      CosmosDbConfigConstants.BULK_DISPLAY,
    )
    .define(
      CosmosDbConfigConstants.PROXY_HOST_CONFIG,
      Type.STRING,
      null,
      Importance.LOW,
      CosmosDbConfigConstants.PROXY_HOST_DOC,
      "Connection",
      5,
      ConfigDef.Width.MEDIUM,
      CosmosDbConfigConstants.PROXY_HOST_DISPLAY,
    )
    .define(
      CosmosDbConfigConstants.ERROR_POLICY_CONFIG,
      Type.STRING,
      CosmosDbConfigConstants.ERROR_POLICY_DEFAULT,
      Importance.HIGH,
      CosmosDbConfigConstants.ERROR_POLICY_DOC,
      "Error",
      1,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.ERROR_POLICY_CONFIG,
    )
    .define(
      CosmosDbConfigConstants.NBR_OF_RETRIES_CONFIG,
      Type.INT,
      CosmosDbConfigConstants.NBR_OF_RETIRES_DEFAULT,
      Importance.MEDIUM,
      CosmosDbConfigConstants.NBR_OF_RETRIES_DOC,
      "Error",
      2,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.NBR_OF_RETRIES_CONFIG,
    )
    .define(
      CosmosDbConfigConstants.ERROR_RETRY_INTERVAL_CONFIG,
      Type.INT,
      CosmosDbConfigConstants.ERROR_RETRY_INTERVAL_DEFAULT,
      Importance.MEDIUM,
      CosmosDbConfigConstants.ERROR_RETRY_INTERVAL_DOC,
      "Error",
      3,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.ERROR_RETRY_INTERVAL_CONFIG,
    )
    .define(
      CosmosDbConfigConstants.PROGRESS_COUNTER_ENABLED,
      Type.BOOLEAN,
      CosmosDbConfigConstants.PROGRESS_COUNTER_ENABLED_DEFAULT,
      Importance.MEDIUM,
      CosmosDbConfigConstants.PROGRESS_COUNTER_ENABLED_DOC,
      "Metrics",
      1,
      ConfigDef.Width.MEDIUM,
      CosmosDbConfigConstants.PROGRESS_COUNTER_ENABLED_DISPLAY,
    )
    .define(
      CosmosDbConfigConstants.MaxQueueSizeProp,
      Type.INT,
      CosmosDbConfigConstants.MaxQueueSizeDefault,
      Importance.MEDIUM,
      CosmosDbConfigConstants.MaxQueueSizeDoc,
      "Bulk",
      1,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.MaxQueueSizeProp,
    )
    .define(
      CosmosDbConfigConstants.MaxQueueOfferTimeoutProp,
      Type.LONG,
      CosmosDbConfigConstants.MaxQueueOfferTimeoutDefault,
      Importance.MEDIUM,
      CosmosDbConfigConstants.MaxQueueOfferTimeoutDoc,
      "Bulk",
      2,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.MaxQueueOfferTimeoutProp,
    )
    .define(
      CosmosDbConfigConstants.COLLECTION_THROUGHPUT_CONFIG,
      Type.INT,
      CosmosDbConfigConstants.COLLECTION_THROUGHPUT_DEFAULT,
      Importance.MEDIUM,
      CosmosDbConfigConstants.COLLECTION_THROUGHPUT_DOC,
      "Collection",
      1,
      ConfigDef.Width.MEDIUM,
      CosmosDbConfigConstants.COLLECTION_THROUGHPUT_CONFIG,
    )
    .define(
      CosmosDbConfigConstants.ExecutorThreadsProp,
      Type.INT,
      CosmosDbConfigConstants.ExecutorThreadsDefault,
      Importance.MEDIUM,
      CosmosDbConfigConstants.ExecutorThreadsDoc,
      "Bulk",
      3,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.ExecutorThreadsProp,
    )
    .define(
      CosmosDbConfigConstants.UploadSyncPeriodProp,
      Type.LONG,
      CosmosDbConfigConstants.UploadSyncPeriodDefault,
      Importance.MEDIUM,
      CosmosDbConfigConstants.UploadSyncPeriodDoc,
      "Bulk",
      4,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.UploadSyncPeriodProp,
    )
    .define(
      CosmosDbConfigConstants.ErrorThresholdProp,
      Type.INT,
      CosmosDbConfigConstants.ErrorThresholdDefault,
      Importance.MEDIUM,
      CosmosDbConfigConstants.ErrorThresholdDoc,
      "Bulk",
      5,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.ErrorThresholdProp,
    )
    .define(
      CosmosDbConfigConstants.EnableFlushCountProp,
      Type.BOOLEAN,
      false,
      Importance.MEDIUM,
      CosmosDbConfigConstants.EnableFlushCountDoc,
      "Bulk",
      6,
      ConfigDef.Width.LONG,
      CosmosDbConfigConstants.EnableFlushCountProp,
    )

  new io.lenses.streamreactor.common.config.base.KcqlSettings(new ConnectorPrefix(
    CosmosDbConfigConstants.CONNECTOR_PREFIX,
  )).withSettings(config)
}

class CosmosDbConfig(props: Map[String, String])
    extends BaseConfig(CosmosDbConfigConstants.CONNECTOR_PREFIX, CosmosDbConfig.config, props)
    with KcqlWithFieldsSettings
    with FlushSettings
    with DatabaseSettings
    with NumberRetriesSettings
    with ErrorPolicySettings
    with ConsistencyLevelSettings[ConsistencyLevel]
    with KeySourceSettings {

  def getConnectorName: String = props.getOrElse("name", "Unknown-CosmosDB-Sink")

  override def getDatabase: String = getString(CosmosDbConfigConstants.DATABASE_CONFIG)
}
