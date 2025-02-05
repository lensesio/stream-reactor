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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer

import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.connect.azure.cosmosdb.CosmosClientProvider
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfig
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfigConstants
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkTaskContext

import java.util.UUID

//Factory to build
object CosmosDbWriterFactory extends StrictLogging {
  def apply(
    connectorConfig: CosmosDbConfig,
    context:         SinkTaskContext,
    settings:        CosmosDbSinkSettings,
  ): CosmosDbWriterManager = {

    val sinkName = connectorConfig.props.get("name").getOrElse(UUID.randomUUID().toString)

    //if error policy is retry set retry interval
    settings.errorPolicy match {
      case RetryErrorPolicy() =>
        context.timeout(connectorConfig.getLong(CosmosDbConfigConstants.ERROR_RETRY_INTERVAL_CONFIG))
      case _ =>
    }
    logger.info(s"Initialising Document Db writer.")
    val client = CosmosClientProvider.get(settings)
    val configMap: Map[String, Kcql] = settings.kcql
      .map { c =>
        Option(
          client.getDatabase(settings.database).getContainer(c.getTarget),
        ).getOrElse {
          throw new ConnectException(s"Collection [${c.getTarget}] not found!")
        }
        c.getSource -> c
      }.toMap
    new CosmosDbWriterManager(sinkName, configMap, settings, client)
  }
}
