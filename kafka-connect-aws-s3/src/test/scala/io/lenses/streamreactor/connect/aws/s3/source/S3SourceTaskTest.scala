package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.{Format, FormatOptions}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3TestConfig
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.jclouds.blobstore.domain.Blob
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

import java.util
import scala.collection.JavaConverters._

class S3SourceTaskTest extends AnyFlatSpec with Matchers with S3TestConfig with LazyLogging {

  import BucketSetup._

  private val formats = Table(
    ("format", "formatOptionOption"),
    (Format.Avro, None),
    (Format.Json, None),
    (Format.Parquet, None),
    (Format.Csv, Some(FormatOptions.WithHeaders)),
    (Format.Csv, None),
  )

  "blobstore get input stream" should "reveal availability" in {
    setUpBucketData(BucketName, blobStoreContext, Format.Json, None)

    val blob: Blob = blobStoreContext.getBlobStore.getBlob(BucketName, s"$PrefixName/$TopicName/0/399.json")
    val inputStream = blob.getPayload.openStream()

    val initialAvailable = inputStream.available()

    var expectedAvailable = initialAvailable
    while (inputStream.available() > 0) {
      expectedAvailable = expectedAvailable - 1
      inputStream.read()
      inputStream.available() should be(expectedAvailable)
    }
  }

  "task" should "read stored files continuously" in {
    forAll(formats) {
      (format, formatOptions) =>
        setUpBucketData(BucketName, blobStoreContext, format, formatOptions)

        val task = new S3SourceTask()

        val formatExtensionString = generateFormatString(formatOptions)

        val props = DefaultProps
          .combine(
            Map("connect.s3.kcql" -> s"insert into $TopicName select * from $BucketName:$PrefixName STOREAS `${format.entryName}$formatExtensionString` LIMIT 190")
          ).asJava

        task.start(props)
        val sourceRecords1 = task.poll()
        val sourceRecords2 = task.poll()
        val sourceRecords3 = task.poll()
        val sourceRecords4 = task.poll()
        val sourceRecords5 = task.poll()
        val sourceRecords6 = task.poll()
        val sourceRecords7 = task.poll()

        task.stop()

        sourceRecords1 should have size 190
        sourceRecords2 should have size 190
        sourceRecords3 should have size 190
        sourceRecords4 should have size 190
        sourceRecords5 should have size 190
        sourceRecords6 should have size 50
        sourceRecords7 should have size 0

        sourceRecords1.asScala
          .union(sourceRecords2.asScala)
          .union(sourceRecords3.asScala)
          .union(sourceRecords4.asScala)
          .union(sourceRecords5.asScala)
          .union(sourceRecords6.asScala)
          .toSet should have size 1000
    }
  }

  "task" should "resume from a specific offset through initialize" in {
    forAll(formats) {
      (format, formatOptions) =>
        setUpBucketData(BucketName, blobStoreContext, format, formatOptions)
        val formatExtensionString = generateFormatString(formatOptions)

        val task = new S3SourceTask()

        val context = new SourceTaskContext {
          override def configs(): util.Map[String, String] = Map.empty[String, String].asJava

          override def offsetStorageReader(): OffsetStorageReader = new OffsetStorageReader {
            override def offset[T](partition: util.Map[String, T]): util.Map[String, AnyRef] = Map(
              "path" -> s"$PrefixName/$TopicName/0/399.${format.entryName.toLowerCase}",
              "line" -> "9".asInstanceOf[Object]
            ).asJava

            override def offsets[T](partitions: util.Collection[util.Map[String, T]]): util.Map[util.Map[String, T], util.Map[String, AnyRef]] = throw new IllegalStateException("Unexpected call to storage reader")
          }
        }
        task.initialize(context)

        val props = DefaultProps
          .combine(
            Map("connect.s3.kcql" -> s"insert into $TopicName select * from $BucketName:$PrefixName STOREAS `${format.entryName}$formatExtensionString` LIMIT 190")
          ).asJava

        task.start(props)
        val sourceRecords1 = task.poll()
        val sourceRecords2 = task.poll()
        val sourceRecords3 = task.poll()
        val sourceRecords4 = task.poll()
        val sourceRecords5 = task.poll()
        val sourceRecords6 = task.poll()
        val sourceRecords7 = task.poll()

        task.stop()

        sourceRecords1 should have size 190
        sourceRecords2 should have size 190
        sourceRecords3 should have size 190
        sourceRecords4 should have size 190
        sourceRecords5 should have size 30
        sourceRecords6 should have size 0
        sourceRecords7 should have size 0

        sourceRecords1.asScala
          .union(sourceRecords2.asScala)
          .union(sourceRecords3.asScala)
          .union(sourceRecords4.asScala)
          .union(sourceRecords5.asScala)
          .union(sourceRecords6.asScala)
          .toSet should have size 790
    }
  }

  "task" should "read stored bytes files continuously" in {
    val (format, formatOptions) = (Format.Bytes, Some(FormatOptions.ValueOnly));

    setUpBucketData(BucketName, blobStoreContext, format, formatOptions)

    val task = new S3SourceTask()

    val formatExtensionString = generateFormatString(formatOptions)

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $TopicName select * from $BucketName:$PrefixName STOREAS `${format.entryName}$formatExtensionString` LIMIT 190")
      ).asJava

    task.start(props)
    val sourceRecords1 = task.poll()
    val sourceRecords2 = task.poll()

    task.stop()

    sourceRecords1 should have size 5
    sourceRecords2 should have size 0

    val expectedLength = totalFileLengthBytes(format, formatOptions)
    val allLength = sourceRecords1.asScala.map(_.value().asInstanceOf[Array[Byte]].length).sum

    allLength should be(expectedLength)

  }


}
