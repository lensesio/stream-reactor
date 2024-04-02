package io.lenses.streamreactor.connect
import _root_.io.confluent.kafka.serializers.KafkaJsonSerializer
import _root_.io.lenses.streamreactor.connect.Configuration.sinkConfig
import _root_.io.lenses.streamreactor.connect.S3Utils.createBucket
import _root_.io.lenses.streamreactor.connect.S3Utils.createS3ClientResource
import _root_.io.lenses.streamreactor.connect.S3Utils.readKeyToOrder
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import _root_.io.lenses.streamreactor.connect.testcontainers.S3Container
import _root_.io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import software.amazon.awssdk.services.s3.model._

import java.util.UUID

class S3Test
    extends AsyncFlatSpec
    with AsyncIOSpec
    with StreamReactorContainerPerSuite
    with Matchers
    with LazyLogging
    with EitherValues
    with TableDrivenPropertyChecks {

  private lazy val container: S3Container = S3Container()
    .withNetwork(network)
  private val bucketName = "fancystuff"

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

  behavior of "AWS S3 connector"

  it should "sink records" in {

    val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100, UUID.randomUUID().toString)

    val resources = for {
      s3Client <- createS3ClientResource(container.identity, container.getEndpointUrl)
      producer <- createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]])
      _        <- createBucket(s3Client, bucketName)
      _ <- createConnector(
        sinkConfig("aws-s3-sink",
                   container.identity,
                   container.getNetworkAliasUrl.toString,
                   bucketName,
                   "myfiles",
                   topicName = "orders",
        ),
      )
    } yield {
      (s3Client, producer)
    }
    resources.use {
      case (s3Client, producer) =>
        IO {
          // Write records to topic

          producer.send(new ProducerRecord[String, Order]("orders", order)).get
          producer.flush()

          eventually {
            val files =
              s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix("myfiles").build())
            logger.debug("files: {}", files)
            assert(files.contents().size() == 1)
          }

          readKeyToOrder(s3Client, bucketName, "myfiles/orders/0/000000000000.json")
        }.asserting {
          key: Order =>
            key should be(order)
        }
    }
  }

}
