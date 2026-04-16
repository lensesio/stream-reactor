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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink

import com.azure.cosmos.CosmosClient
import com.azure.cosmos.CosmosDatabase
import com.azure.cosmos.models.ThroughputProperties
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.common.config.ConfigException
import scala.util.Try
import cats.implicits._

object CosmosDbDatabaseUtils extends StrictLogging {
  def readOrCreateCollections(
    database: CosmosDatabase,
    settings: CosmosDbSinkSettings,
  ): Either[Throwable, Unit] = {
    val collectionNames = settings.kcql.map(_.getTarget)
    logger.info(
      s"Preparing to read or create collections in database [${database.getId}]: ${collectionNames.mkString(", ")}",
    )
    collectionNames.toList.traverse_ { collectionName =>
      logger.info(s"Checking existence of collection [$collectionName] in database [${database.getId}]")
      val result = Try(database.getContainer(collectionName).read()).toEither.leftMap { ex =>
        logger.info(s"Collection [$collectionName] does not exist in database [${database.getId}], creating.")
        ex
      } match {
        case Right(_) =>
          logger.info(s"Collection [$collectionName] already exists in database [${database.getId}]")
          Right(())
        case Left(_) =>
          logger.info(
            s"Creating collection [$collectionName] in database [${database.getId}] with throughput [${settings.collectionThroughput} RU/s]",
          )
          createCollection(database,
                           ThroughputProperties.createManualThroughput(settings.collectionThroughput),
                           collectionName,
          )
      }
      logger.info(s"Finished processing collection [$collectionName] in database [${database.getId}]")
      result
    }
  }

  def createCollection(
    database:       CosmosDatabase,
    throughput:     ThroughputProperties,
    collectionName: String,
  ): Either[Throwable, Unit] = {
    logger.info(
      s"Attempting to create collection [$collectionName] in database [${database.getId}] with throughput [${throughput.getManualThroughput}] RU/s",
    )
    val result = Try(database.createContainer(collectionName, "partitionKeyPath", throughput)).toEither
      .leftMap(t => new RuntimeException(s"Could not create collection [$collectionName]. ${t.getMessage}", t))
      .map(_ => ())
    result match {
      case Right(_) => logger.info(s"Successfully created collection [$collectionName] in database [${database.getId}]")
      case Left(e) => logger.error(
          s"Failed to create collection [$collectionName] in database [${database.getId}]: ${e.getMessage}",
          e,
        )
    }
    result
  }

  def readOrCreateDatabase(
    settings:       CosmosDbSinkSettings,
  )(documentClient: CosmosClient,
  ): Either[Throwable, CosmosDatabase] = {
    logger.info(s"Attempting to read database [${settings.database}]")
    val result = Try(documentClient.getDatabase(settings.database)).toEither.leftFlatMap { _ =>
      logger.info(s"Database [${settings.database}] does not exist, creating.")
      if (settings.createDatabase) {
        for {
          _ <- createDatabase(settings.database, documentClient).leftMap { t =>
            new ConfigException(s"Could not create database [${settings.database}]: ${t.getMessage}")
          }
          db <- Try(documentClient.getDatabase(settings.database)).toEither.leftMap { t =>
            new ConfigException(s"Could not get database after creation [${settings.database}]: ${t.getMessage}")
          }
        } yield db
      } else {
        Left(new ConfigException(s"Database [${settings.database}] does not exist and createDatabase is false."))
      }
    }
    result match {
      case Right(_) => logger.info(s"Database [${settings.database}] is ready.")
      case Left(e)  => logger.error(s"Failed to read or create database [${settings.database}]: ${e.getMessage}", e)
    }
    result
  }

  private[sink] def createDatabase(
    databaseName: String,
    cosmosClient: CosmosClient,
  ): Either[Throwable, CosmosDatabase] = {
    logger.info(s"Attempting to create database [$databaseName]")
    val result = for {
      _  <- Try(cosmosClient.createDatabase(databaseName, null)).toEither
      db <- Try(cosmosClient.getDatabase(databaseName)).toEither
    } yield db
    result match {
      case Right(_) => logger.info(s"Successfully created database [$databaseName]")
      case Left(e)  => logger.error(s"Failed to create database [$databaseName]: ${e.getMessage}", e)
    }
    result
  }
}
