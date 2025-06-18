package io.lenses.streamreactor.connect
import _root_.io.confluent.kafka.serializers.KafkaJsonSerializer
import _root_.io.lenses.streamreactor.connect.Configuration.sinkConfig
import _root_.io.lenses.streamreactor.connect.CosmosUtils.createCosmosClientResource
import _root_.io.lenses.streamreactor.connect.CosmosUtils.createDatabaseAndContainer
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.CosmosContainer
import _root_.io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.azure.cosmos.models.CosmosQueryRequestOptions
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.testcontainers.containers.CosmosDBEmulatorContainer

import java.util.UUID
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala

class CosmosTest
    extends AsyncFlatSpec
    with AsyncIOSpec
    with StreamReactorContainerPerSuite
    with Matchers
    with LazyLogging
    with EitherValues
    with TableDrivenPropertyChecks {

  private lazy val container: CosmosDBEmulatorContainer = CosmosContainer()
    .withNetwork(network)
  private val databaseName  = "fancystuff"
  private val containerName = "orders"

  override val schemaRegistryContainer: Option[SchemaRegistryContainer] = None

  override val connectorModule: String = "aws-s3"

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  behavior of "Azure CosmosDB Connector"

  it should "sink records" in {

    val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100, UUID.randomUUID().toString)

    val resources = for {
      cosmosClient <- createCosmosClientResource(container.getEmulatorEndpoint, container.getEmulatorKey)
      producer     <- createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]])
      database     <- createDatabaseAndContainer(cosmosClient, databaseName, containerName, "/id")
      _ <- createConnector(
        sinkConfig(
          "aws-s3-sink",
          container.getEmulatorKey,
          container.getNetworkAliases.asScala.head,
          databaseName,
          "myfiles",
          topicName = "orders",
        ),
      )
    } yield {
      (database, producer)
    }
    resources.use {
      case (container, producer) =>
        var files: List[Order] = Nil

        IO {
          // Write records to topic

          // TODO: HOW do I set the SSL properties for the producer?

          // FOR MONDAY :-)

          producer.send(new ProducerRecord[String, Order]("orders", null, 0L, null, order)).get
          producer.flush()

          eventually {
            files = container.queryItems(
              s"SELECT c.id FROM $databaseName AS c WHERE c.id = 'myfiles/orders/0/000000000000_0_0.json'",
              new CosmosQueryRequestOptions(),
              classOf[Order],
            ).asScala.toList
            logger.debug("files: {}", files)
            assert(files.size == 1)
          }
          files

        }.asserting {
          files =>
            files.headOption shouldEqual order
        }
    }
  }

}
