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
import io.lenses.streamreactor.common.batch.BatchPolicy
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

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
  taskRetryInterval:    Long,
  bulkEnabled:          Boolean,
  maxQueueSize:         Int,
  maxQueueOfferTimeout: FiniteDuration,
  executorThreads:      Int,
  delay:                FiniteDuration,
  errorThreshold:       Int,
  commitPolicy:         Kcql => BatchPolicy,
  sinkName:             String,
  collectionThroughput: Int,
)

object CosmosDbSinkSettings extends StrictLogging {

  def apply(config: CosmosDbConfig): Either[Throwable, CosmosDbSinkSettings] =
    for {
      endpoint <- Try(config.getString(CosmosDbConfigConstants.CONNECTION_CONFIG)).toEither
      _ <-
        if (endpoint.nonEmpty)
          Right(())
        else
          Left(new ConfigException(s"Invalid endpoint provided. [${CosmosDbConfigConstants.CONNECTION_CONFIG_DOC}]"))
      masterKey <- Option(config.getPassword(CosmosDbConfigConstants.MASTER_KEY_CONFIG))
        .map(_.value())
        .toRight(new ConfigException(s"Missing [${CosmosDbConfigConstants.MASTER_KEY_CONFIG}]"))
      _ <-
        if (masterKey.trim.nonEmpty) Right(())
        else Left(new ConfigException(s"Invalid [${CosmosDbConfigConstants.MASTER_KEY_CONFIG}]"))
      database <- Try(config.getDatabase).toEither
      _ <-
        if (database.nonEmpty) Right(())
        else Left(new IllegalArgumentException(s"Missing [${CosmosDbConfigConstants.DATABASE_CONFIG}]"))
      kcql        <- Try(config.getKCQL).toEither
      errorPolicy  = config.getErrorPolicy
      retries      = config.getNumberRetries
      fieldsMap    = config.getFieldsMap()
      ignoreFields = config.getIgnoreFieldsMap()
      consistencyLevel <- Try(config.getConsistencyLevel).toEither
        .flatMap(_.toRight(new ConfigException("Missing consistency level")))
    } yield new CosmosDbSinkSettings(
      endpoint          = endpoint,
      masterKey         = masterKey,
      database          = database,
      kcql              = kcql.toSeq,
      fields            = fieldsMap,
      ignoredField      = ignoreFields,
      errorPolicy       = errorPolicy,
      consistency       = consistencyLevel,
      createDatabase    = config.getBoolean(CosmosDbConfigConstants.CREATE_DATABASE_CONFIG),
      proxy             = Option(config.getString(CosmosDbConfigConstants.PROXY_HOST_CONFIG)),
      keySource         = config.getKeySource,
      taskRetries       = retries,
      taskRetryInterval = config.getInt(CosmosDbConfigConstants.ERROR_RETRY_INTERVAL_CONFIG).toLong,
      bulkEnabled       = config.getBoolean(CosmosDbConfigConstants.BULK_CONFIG),
      maxQueueSize      = config.getInt(CosmosDbConfigConstants.MaxQueueSizeProp),
      maxQueueOfferTimeout = FiniteDuration(
        config.getLong(CosmosDbConfigConstants.MaxQueueOfferTimeoutProp),
        scala.concurrent.duration.MILLISECONDS,
      ),
      executorThreads = config.getInt(CosmosDbConfigConstants.ExecutorThreadsProp),
      delay = FiniteDuration(
        config.getLong(CosmosDbConfigConstants.UploadSyncPeriodProp),
        scala.concurrent.duration.MILLISECONDS,
      ),
      errorThreshold       = config.getInt(CosmosDbConfigConstants.ErrorThresholdProp),
      commitPolicy         = config.commitPolicy,
      sinkName             = config.getConnectorName,
      collectionThroughput = config.getInt(CosmosDbConfigConstants.COLLECTION_THROUGHPUT_CONFIG),
    )
}
