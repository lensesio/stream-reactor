package io.lenses.streamreactor.connect

import _root_.io.confluent.kafka.serializers.KafkaAvroSerializer
import _root_.io.lenses.streamreactor.connect.Configuration.sinkConfig
import _root_.io.lenses.streamreactor.connect.S3Utils.createBucket
import _root_.io.lenses.streamreactor.connect.S3Utils.createS3ClientResource
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.S3Container
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits._
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.scalalogging.LazyLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.EitherValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.prop.TableFor3
import software.amazon.awssdk.services.s3.model._

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Random

class S3CompressionTest
    extends AsyncFlatSpec
    with AsyncIOSpec
    with StreamReactorContainerPerSuite
    with Matchers
    with LazyLogging
    with EitherValues
    with TableDrivenPropertyChecks {

  private lazy val container: S3Container = S3Container()
    .withNetwork(network)

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

  val compressionCodecTestCases: TableFor3[String, String, Boolean] = Table(
    ("format", "codec", "configure"),
    ("avro", "uncompressed", false),
    ("avro", "deflate", true),
    ("avro", "bzip2", false),
    ("avro", "snappy", false),
    ("avro", "xz", true),
    ("avro", "zstd", true),
    ("parquet", "uncompressed", false),
    ("parquet", "snappy", false),
    ("parquet", "gzip", false),
    ("parquet", "lz4", false),
    ("parquet", "zstd", false),
  )

  forAll(compressionCodecTestCases) {
    case (format: String, codec: String, configureLevel: Boolean) =>
      it should s"support $format with $codec compression codec" in {
        val randomTestId = Random.alphanumeric.take(10).mkString.toLowerCase
        val bucketName   = "bucket" + randomTestId
        val prefix       = "prefix" + randomTestId
        val topic        = "topic" + randomTestId
        val resources = for {
          s3Client <- createS3ClientResource(container.identity, container.getEndpointUrl)
          producer <- createProducer[String, GenericRecord](classOf[StringSerializer], classOf[KafkaAvroSerializer])
          _        <- createBucket(s3Client, bucketName)
          _ <- createConnector(
            sinkConfig(
              randomTestId,
              container.identity,
              container.getNetworkAliasUrl.toString,
              bucketName,
              prefix,
              format,
              codec.some,
              Option.when(configureLevel)(5),
              topic,
            ),
          )
        } yield {
          (s3Client, producer)
        }
        resources.use {
          case (s3Client, producer) =>
            IO {
              // Write records to
              val order  = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100, UUIDs.timeBased.toString)
              val record = order.toRecord(order)

              producer.send(new ProducerRecord[String, GenericRecord](topic, record)).get
              producer.flush()

              eventually {
                val files =
                  s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).prefix(prefix).build())
                logger.debug("files: {}", files)
                assert(files.contents().size() == 1)
                val firstFormatFile = files.contents().asScala.head
                // avoid temporary files
                firstFormatFile.key() should endWith(s".$format")
                firstFormatFile
              }
            }
        }.asserting {
          file =>
            file.key() should be(s"$prefix/$topic/0/000000000000.$format")
        }
      }
  }

  override def providedJars(): Seq[String] = ProvidedJars.providedJars
}
