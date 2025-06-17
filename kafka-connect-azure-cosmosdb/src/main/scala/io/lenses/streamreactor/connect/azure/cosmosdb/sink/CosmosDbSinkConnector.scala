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
import com.azure.cosmos.CosmosDatabase
import com.azure.cosmos.models.ThroughputProperties
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.config.Helpers
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.azure.cosmosdb.CosmosClientProvider
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfig
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfigConstants
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkConnector

import java.util
import scala.collection.mutable
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * <h1>CosmosDbSinkConnector</h1>
 * Kafka Connect Azure CosmosDb Sink connector
 *
 * Sets up CosmosDbSinkTask and configurations for the tasks.
 */
class CosmosDbSinkConnector private[sink] (builder: CosmosDbSinkSettings => CosmosClient)
    extends SinkConnector
    with StrictLogging
    with JarManifestProvided {
  private var configProps: util.Map[String, String] = _

  @throws[ConnectException]
  def this() = this(settings =>
    CosmosClientProvider.get(settings) match {
      case Right(client) => client
      case Left(ex)      => throw ex
    },
  )

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

    var documentClient: CosmosClient = null
    try {
      documentClient = builder(settings)
      val database = readOrCreateDatabase(settings)(documentClient)
      readOrCreateCollections(database, settings)
    } finally {
      if (null != documentClient) {
        documentClient.close()
      }
    }
  }

  override def stop(): Unit = {}

  override def config(): ConfigDef = CosmosDbConfig.config

  private def readOrCreateCollections(
    database: CosmosDatabase,
    settings: CosmosDbSinkSettings,
  ): Unit = {
    //check all collection exists and if not create them
    val throughputProperties = ThroughputProperties.createManualThroughput(400)
    settings.kcql.map(_.getTarget).foreach { collectionName =>
      Try(
        database
          .getContainer(collectionName)
          .read(),
      ) match {
        case Failure(ex) =>
          logger.warn(s"Collection [$collectionName] doesn't exist. Creating it...", ex)
          createCollection(database, throughputProperties, collectionName)

        case Success(c) if c == null =>
          logger.warn(s"Collection:$collectionName doesn't exist. Creating it...")
          createCollection(database, throughputProperties, collectionName)
        case _ =>
      }
    }
  }

  private def createCollection(database: CosmosDatabase, throughputProperties: ThroughputProperties, collectionName: String): Unit = {
    Try(database.createContainer(collectionName, "partitionKeyPath", throughputProperties)) match {
      case Failure(t) =>
        throw new RuntimeException(s"Could not create collection [$collectionName]. ${t.getMessage}", t)
      case _ =>
    }

    logger.warn(s"Collection [$collectionName] created")
  }

  private def readOrCreateDatabase(
    settings: CosmosDbSinkSettings,
  )(
    implicit
    documentClient: CosmosClient,
  ): CosmosDatabase = {
    //check database exists
    logger.info(s"Checking ${settings.database} exists...")
    Try {
      documentClient.getDatabase(settings.database)
    } match {
      case Failure(e) =>
        logger.warn(s"Couldn't read the database [${settings.database}]", e)
        if (settings.createDatabase) {
          logger.info(s"Database [${settings.database}] does not exists. Creating it...")
          Try(CreateDatabaseFn(settings.database)) match {
            case Failure(t) =>
              throw new IllegalStateException(s"Could not create database [${settings.database}]. ${t.getMessage}", t)
            case Success(db) =>
              logger.info(s"Database [${settings.database}] created")
              db
          }
        } else {
          throw new RuntimeException(s"Could not find database [${settings.database}]", e)
        }
      case Success(d) =>
        if (d == null) {
          if (settings.createDatabase) {
            logger.info(s"Database ${settings.database} does not exist. Creating it...")
            Try(CreateDatabaseFn(settings.database)) match {
              case Failure(t) =>
                throw new IllegalStateException(s"Could not create database [${settings.database}]. ${t.getMessage}", t)
              case Success(db) =>
                logger.info(s"Database ${settings.database} created")
                db
            }
          } else throw new ConfigException(s"Could not find database [${settings.database}]")
        } else {
          logger.info(s"Database [${settings.database}] already exists...")
          d
        }
    }
  }
}
