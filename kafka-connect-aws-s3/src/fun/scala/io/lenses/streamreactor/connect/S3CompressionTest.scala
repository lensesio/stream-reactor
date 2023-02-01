package io.lenses.streamreactor.connect

import cats.implicits._
import com.datastax.driver.core.utils.UUIDs
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.lenses.streamreactor.connect.Configuration.sinkConfig
import io.lenses.streamreactor.connect.S3Utils.{createBucket, createS3ClientResource}
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.S3Container
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import io.lenses.streamreactor.connect.testcontainers.scalatest.fixtures.connect.withConnector
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import software.amazon.awssdk.services.s3.model._

import scala.util.{Random, Using}

class S3CompressionTest
    extends AnyFlatSpec
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
    ( "avro", "uncompressed", false, Seq.empty[String]),
    ( "avro", "deflate", true, Seq.empty[String]),
    ( "avro", "bzip2", false, Seq.empty[String]),
    ( "avro", "snappy", false, Seq.empty[String]),
    ( "avro", "xz", true, Seq.empty[String]),
    ( "avro", "zstd", true, Seq.empty[String]),
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

        Using.resources(
          createS3ClientResource(container.identity, container.getEndpointUrl),
          createProducer[String, GenericRecord](classOf[StringSerializer], classOf[KafkaAvroSerializer]),
        ) { (s3Client, producer) =>
          val randomTestId = Random.alphanumeric.take(10).mkString.toLowerCase
          val bucketName   = "bucket" + randomTestId
          val prefix       = "prefix" + randomTestId
          val topic        = "topic" + randomTestId
          createBucket(s3Client, bucketName)
          withConnector(
            "connector" + randomTestId,
            sinkConfig(
              container.identity,
              container.getNetworkAliasUrl.toString,
              bucketName,
              prefix,
              format,
              codec.some,
              Option.when(configureLevel)(5),
              topic,
            ),
          ) {

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
            }
          }
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
