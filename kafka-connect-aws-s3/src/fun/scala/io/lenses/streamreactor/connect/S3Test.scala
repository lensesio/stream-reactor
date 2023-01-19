package io.lenses.streamreactor.connect

import io.lenses.streamreactor.connect.Configuration.sinkConfig
import io.lenses.streamreactor.connect.S3Utils.{createBucket, createS3ClientResource, readKeyToOrder}
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import io.lenses.streamreactor.connect.testcontainers.scalatest.fixtures.connect.withConnector
import io.lenses.streamreactor.connect.testcontainers.{S3Container, SchemaRegistryContainer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import software.amazon.awssdk.services.s3.model._

import scala.util.Using

class S3Test extends AnyFlatSpec with StreamReactorContainerPerSuite with Matchers with LazyLogging with EitherValues with TableDrivenPropertyChecks {

  private lazy val container: S3Container = S3Container()
    .withNetwork(network)
  private val bucketName = "fancystuff"

  override def schemaRegistryContainer(): Option[SchemaRegistryContainer] = None

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
    Using.resources(
      createS3ClientResource(container.identity, container.getEndpointUrl),
      createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]]),
    ) { (s3Client, producer) =>

      createBucket(s3Client, bucketName)
      withConnector("aws-s3-sink", sinkConfig(container.identity, container.getNetworkAliasUrl.toString, bucketName, "myfiles", topicName = "orders")) {
        // Write records to topic
        val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100, UUIDs.timeBased.toString)

        producer.send(new ProducerRecord[String, Order]("orders", order)).get
        producer.flush()

        eventually {
          val files = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix("myfiles").build())
          logger.debug("files: {}", files)
          assert(files.contents().size() == 1)
        }

        readKeyToOrder(s3Client, bucketName, "myfiles/orders/0/0.json") should be (order)
      }
    }
  }

}
