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

  val compressionCodecTestCases = Table(
    ("format", "codec", "configure", "extraCommands"),
    ("avro", "uncompressed", false, Seq.empty[String]),
    ("avro", "deflate", true, Seq.empty[String]),
    ("avro", "bzip2", false, Seq.empty[String]),
    ("avro", "snappy", false, Seq.empty[String]),
    ("avro", "xz", true, Seq.empty[String]),
    ("avro", "zstd", true, Seq.empty[String]),
    ("parquet", "uncompressed", false, Seq.empty[String]),
    ("parquet", "snappy", false, Seq.empty[String]),
    ("parquet", "gzip", false, Seq.empty[String]),
    // ("parquet", "lzo", false, Seq("lzo")),
    // brotli lib is madly out of date and I do not believe we can support it
    // ("parquet", "brotli", false, Seq.empty[String]),
    // ("parquet", "lz4", false, Seq.empty[String]),
    ("parquet", "zstd", false, Seq.empty[String]),
  )

  forAll(compressionCodecTestCases) {
    case (format: String, codec: String, configureLevel: Boolean, extraPackages: Seq[String]) =>
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
              installExtraPackages(extraPackages)

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
                files.contents().asScala.head
              }
            }
        }.asserting {
          file =>
            file.key() should be(s"$prefix/$topic/0/0.avro")
        }
      }
  }

  private def installExtraPackages(extraPackages: Seq[String]): Unit =
    extraPackages.foreach { pkg =>
      val res = kafkaConnectContainer.installPackage(pkg)
      if (res.exitCode != 1) {
        throw new IllegalStateException("Installation of packages not successful")
      }
    }

  override def providedJars(): Seq[String] = ProvidedJars.providedJars
}
