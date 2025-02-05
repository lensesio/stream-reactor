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
package io.lenses.streamreactor.connect.azure.cosmosdb.config

import com.azure.cosmos.ConsistencyLevel
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.errors.ErrorPolicy
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigException

import scala.concurrent.duration.FiniteDuration

case class CosmosDbSinkSettings(
  endpoint:             String,
  masterKey:            String,
  database:             String,
  kcql:                 Seq[Kcql],
  fields:               Map[String, Map[String, String]],
  ignoredField:         Map[String, Set[String]],
  errorPolicy:          ErrorPolicy,
  consistency:          ConsistencyLevel,
  createDatabase:       Boolean,
  proxy:                Option[String],
  keySource:            KeySource,
  taskRetries:          Int = CosmosDbConfigConstants.NBR_OF_RETIRES_DEFAULT,
  bulkEnabled:          Boolean,
  maxQueueSize:         Int,
  maxQueueOfferTimeout: FiniteDuration,
  executorThreads:      Int,
  delay:                FiniteDuration,
  errorThreshold:       Int,
) {}

object CosmosDbSinkSettings extends StrictLogging {

  def apply(config: CosmosDbConfig): CosmosDbSinkSettings = {
    val endpoint = config.getString(CosmosDbConfigConstants.CONNECTION_CONFIG)
    require(endpoint.nonEmpty, s"Invalid endpoint provided. [${CosmosDbConfigConstants.CONNECTION_CONFIG_DOC}]")

    val masterKey = Option(config.getPassword(CosmosDbConfigConstants.MASTER_KEY_CONFIG))
      .map(_.value())
      .getOrElse(throw new ConfigException(s"Missing [${CosmosDbConfigConstants.MASTER_KEY_CONFIG}]"))
    if (masterKey.trim.isEmpty)
      throw new ConfigException(s"Invalid [${CosmosDbConfigConstants.MASTER_KEY_CONFIG}]")

    val database = config.getDatabase

    if (database.isEmpty) {
      throw new ConfigException(s"Missing [${CosmosDbConfigConstants.DATABASE_CONFIG}]")
    }

    val kcql             = config.getKCQL
    val errorPolicy      = config.getErrorPolicy
    val retries          = config.getNumberRetries
    val fieldsMap        = config.getFieldsMap()
    val ignoreFields     = config.getIgnoreFieldsMap()
    val consistencyLevel = config.getConsistencyLevel.get

    new CosmosDbSinkSettings(endpoint,
                             masterKey,
                             database,
                             kcql.toSeq,
                             fieldsMap,
                             ignoreFields,
                             errorPolicy,
                             consistencyLevel,
                             config.getBoolean(CosmosDbConfigConstants.CREATE_DATABASE_CONFIG),
                             Option(config.getString(CosmosDbConfigConstants.PROXY_HOST_CONFIG)),
                             config.getKeySource(),
                             retries,
                             config.getBoolean(CosmosDbConfigConstants.BULK_CONFIG),
                             config.getInt(CosmosDbConfigConstants.MaxQueueSizeProp),
                             FiniteDuration(
                               config.getLong(CosmosDbConfigConstants.MaxQueueOfferTimeoutProp),
                               scala.concurrent.duration.MILLISECONDS,
                             ),
                             config.getInt(CosmosDbConfigConstants.ExecutorThreadsProp),
                             FiniteDuration(
                               config.getLong(CosmosDbConfigConstants.UploadSyncPeriodProp),
                               scala.concurrent.duration.MILLISECONDS,
                             ),
                             config.getInt(CosmosDbConfigConstants.ErrorThresholdProp),
    )
  }
}
