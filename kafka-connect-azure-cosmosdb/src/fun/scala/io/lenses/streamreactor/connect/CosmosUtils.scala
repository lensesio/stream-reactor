package io.lenses.streamreactor.connect

import cats.effect.{IO, Resource}
import com.azure.cosmos.models.CosmosContainerProperties
import com.azure.cosmos.{CosmosClient, CosmosClientBuilder, CosmosContainer}
import com.typesafe.scalalogging.LazyLogging
import org.scalatest.EitherValues

object CosmosUtils extends EitherValues with LazyLogging {
  def createDatabaseAndContainer(
                                  cosmosClient: CosmosClient,
                                  dbName: String,
                                  containerName: String,
                                  partitionKeyPath: String
                                ): Resource[IO, CosmosContainer] =
    Resource.make {
      for {
        // Delete the existing DB if it exists
        existingDb <- IO(Option(cosmosClient.getDatabase(dbName)))
        _ = existingDb.foreach(_.delete())

        // Recreate the database
        _ <- IO(cosmosClient.createDatabaseIfNotExists(dbName))
        db = cosmosClient.getDatabase(dbName)

        // Create the container
        containerProperties = new CosmosContainerProperties(containerName, partitionKeyPath)
        _ <- IO(db.createContainerIfNotExists(containerProperties))
        container = db.getContainer(containerName)

      } yield container
    } { _ =>
      IO {
        // Delete the whole database to clean up
        cosmosClient.getDatabase(dbName).delete()
        logger.info(s"Deleted database $dbName")
      }
    }
  def createCosmosClientResource(endpoint: String, masterKey: String): Resource[IO, CosmosClient] =
    Resource.fromAutoCloseable(
      IO(
        new CosmosClientBuilder()
          .endpoint(endpoint)
          .key(masterKey)
          .buildClient(),
      ),
    )


}
