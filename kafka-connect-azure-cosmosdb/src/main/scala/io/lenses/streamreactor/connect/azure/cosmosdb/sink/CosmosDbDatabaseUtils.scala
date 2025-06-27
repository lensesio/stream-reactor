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
    collectionNames.toList.traverse_ { collectionName =>
      for {
        _ <- Try(database.getContainer(collectionName).read()).toEither.leftMap { ex =>
          logger.info(s"Collection [$collectionName] does not exist, creating.")
          ex
        } match {
          case Right(_) => Right(())
          case Left(_)  => createCollection(database, ThroughputProperties.createManualThroughput(400), collectionName)
        }
      } yield ()
    }
  }

  def createCollection(
    database:       CosmosDatabase,
    throughput:     ThroughputProperties,
    collectionName: String,
  ): Either[Throwable, Unit] =
    Try(database.createContainer(collectionName, "partitionKeyPath", throughput)).toEither
      .leftMap(t => new RuntimeException(s"Could not create collection [$collectionName]. ${t.getMessage}", t))
      .map(_ => ())

  def readOrCreateDatabase(
    settings:       CosmosDbSinkSettings,
  )(documentClient: CosmosClient,
  ): Either[Throwable, CosmosDatabase] =
    Try(documentClient.getDatabase(settings.database)).toEither.leftFlatMap { _ =>
      logger.info(s"Database [${settings.database}] does not exist, creating.")
      if (settings.createDatabase) {
        for {
          _ <- Try(CreateDatabaseFn(settings.database)(documentClient)).toEither.leftMap { t =>
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
}
