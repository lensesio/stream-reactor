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

import cats.implicits.toBifunctorOps
import com.azure.cosmos.CosmosClient
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.config.Helpers
import io.lenses.streamreactor.common.utils.EitherOps._
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.azure.cosmosdb.CosmosClientProvider
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfig
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfigConstants
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.CosmosDbDatabaseUtils._

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
    logger.info("Starting Azure CosmosDb sink connector initialization. Parsing connector configuration.")

    val setup = for {
      config <- CosmosDbConfig(props.asScala.toMap).leftMap(ex =>
        new ConnectException(s"Couldn't start Azure CosmosDb sink due to configuration error: ${ex.getMessage}", ex),
      )
      _ = {
        logger.info("Connector configuration parsed successfully. Checking input topics from KCQL configuration.")
        configProps = props
      }
      _            <- Helpers.checkInputTopicsEither(CosmosDbConfigConstants.KCQL_CONFIG, props.asScala.toMap)
      _             = logger.info("Input topics check passed. Creating CosmosDbSinkSettings from configuration.")
      settings     <- CosmosDbSinkSettings(config)
      _             = logger.info(s"CosmosDbSinkSettings created: $settings. Creating Cosmos DB client.")
      cosmosClient <- createCosmosClient(settings)
      _             = logger.info("Cosmos DB client created. Reading or creating database.")
      database <- readOrCreateDatabase(settings)(cosmosClient).leftMap(ex =>
        new ConnectException(s"Failed to read or create database: ${ex.getMessage}", ex),
      )
      _ = logger.info("Database ready. Reading or creating collections.")
      _ <- readOrCreateCollections(database, settings).leftMap(ex =>
        new ConnectException(s"Failed to read or create collections: ${ex.getMessage}", ex),
      )
      _ = logger.info("Collections ready.")
    } yield ()
    setup.unpackOrThrow
    logger.info("Azure CosmosDb sink connector initialization complete.")
  }

  protected def createCosmosClient(settings: CosmosDbSinkSettings): Either[ConnectException, CosmosClient] =
    CosmosClientProvider.get(settings)

  override def stop(): Unit = {}

  override def config(): ConfigDef = CosmosDbConfig.config
}
