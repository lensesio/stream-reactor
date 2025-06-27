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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink

import com.azure.cosmos.CosmosClient
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.config.Helpers
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.azure.cosmosdb.CosmosClientProvider
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfig
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfigConstants
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector
import io.lenses.streamreactor.common.utils.EitherOps._

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.CosmosDbDatabaseUtils

/**
 * <h1>CosmosDbSinkConnector</h1>
 * Kafka Connect Azure CosmosDb Sink connector
 *
 * Sets up CosmosDbSinkTask and configurations for the tasks.
 */
class CosmosDbSinkConnector extends SinkConnector with StrictLogging with JarManifestProvided {
  private var configProps: util.Map[String, String] = _

  /**
   * States which SinkTask class to use
   */
  override def taskClass(): Class[_ <: Task] = classOf[CosmosDbSinkTask]

  /**
   * Set the configuration for each work and determine the split
   *
   * @param maxTasks The max number of task workers be can spawn
   * @return a List of configuration properties per worker
   */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for [$maxTasks ]workers.")

    val kcql = configProps.get(CosmosDbConfigConstants.KCQL_CONFIG).split(";")

    if (maxTasks == 1 || kcql.length == 1) {
      List(configProps).asJava
    } else {
      val groups = kcql.length / maxTasks + kcql.length % maxTasks
      kcql.grouped(groups)
        .map(_.mkString(";"))
        .map { routes =>
          val taskProps = mutable.Map[String, String]()
          taskProps ++= configProps.asScala
          taskProps.put(CosmosDbConfigConstants.KCQL_CONFIG, routes)
          taskProps.asJava
        }.toList.asJava
    }
  }

  /**
   * Start the sink and set to configuration
   *
   * @param props A map of properties for the connector and worker
   */
  override def start(props: util.Map[String, String]): Unit = {
    val config = Try(CosmosDbConfig(props.asScala.toMap)) match {
      case Failure(f) =>
        throw new ConnectException(s"Couldn't start Azure CosmosDb sink due to configuration error: ${f.getMessage}", f)
      case Success(c) => c
    }
    configProps = props

    //check input topics
    Helpers.checkInputTopics(CosmosDbConfigConstants.KCQL_CONFIG, props.asScala.toMap)

    val settings = CosmosDbSinkSettings(config)

    // Cosmos DB setup logic (ensure DB and collections exist)
    var documentClient: CosmosClient = null
    try {
      documentClient = createCosmosClient(settings)
      val database = CosmosDbDatabaseUtils.readOrCreateDatabase(settings)(documentClient)
        .unpackOrThrow(ex => new ConnectException(s"Failed to read or create database: ${ex.getMessage}", ex))
      CosmosDbDatabaseUtils.readOrCreateCollections(database, settings)
        .unpackOrThrow(ex => new ConnectException(s"Failed to read or create collections: ${ex.getMessage}", ex))
    } finally {
      if (null != documentClient) {
        documentClient.close()
      }
    }
  }

  // Allow test subclasses to override client creation
  protected def createCosmosClient(settings: CosmosDbSinkSettings): CosmosClient =
    CosmosClientProvider.get(settings).unpackOrThrow

  override def stop(): Unit = {}

  override def config(): ConfigDef = CosmosDbConfig.config
}
