package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.Format.Bytes
import io.lenses.streamreactor.connect.aws.s3.config.FormatOptions.{KeyAndValueWithSizes, ValueOnly}
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.config.{AuthMode, Format, FormatOptions}
import io.lenses.streamreactor.connect.aws.s3.sink.utils.S3ProxyContext._
import io.lenses.streamreactor.connect.aws.s3.sink.utils.{S3ProxyContext, S3TestConfig}
import org.apache.kafka.connect.source.SourceTaskContext
import org.apache.kafka.connect.storage.OffsetStorageReader
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks._

import java.util
import scala.collection.JavaConverters._

class S3SourceTaskTest extends AnyFlatSpec with Matchers with S3TestConfig with LazyLogging with BeforeAndAfter {

  val bucketSetup = new BucketSetup()
  import bucketSetup._

  val DefaultProps = Map(
    AWS_ACCESS_KEY -> Identity,
    AWS_SECRET_KEY -> Credential,
    AWS_REGION -> "eu-west-1",
    AUTH_MODE -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT -> S3ProxyContext.Uri,
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true"
  )

  private val formats = Table(
    ("format", "formatOptionOption", "dirName"),
    (Format.Avro, None, "avro"),
    (Format.Json, None, "json"),
    (Format.Parquet, None, "parquet"),
    (Format.Csv, Some(FormatOptions.WithHeaders), "csvheaders"),
    (Format.Csv, None, "csvnoheaders"),
  )



  "blobstore get input stream" should "reveal availability" in {

    val inputStream = helper.remoteFileAsStream(BucketName, s"$PrefixName/json/$TopicName/0/399.json")
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
      (format, formatOptions, dir) =>
        val t1 = System.currentTimeMillis()

        val task = new S3SourceTask()

        val formatExtensionString = generateFormatString(formatOptions)

        val props = DefaultProps
          .combine(
            Map("connect.s3.kcql" -> s"insert into $TopicName select * from $BucketName:$PrefixName/$dir STOREAS `${format.entryName}$formatExtensionString` LIMIT 190")
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

        val t2 = System.currentTimeMillis()
        val dur = t2 - t1
        logger.info(s"$format DUR: $dur ms")
    }
  }

  "task" should "resume from a specific offset through initialize" in {
    forAll(formats) {
      (format, formatOptions, dir) =>
        val formatExtensionString = generateFormatString(formatOptions)

        val task = new S3SourceTask()

        val context = new SourceTaskContext {
          override def configs(): util.Map[String, String] = Map.empty[String, String].asJava

          override def offsetStorageReader(): OffsetStorageReader = new OffsetStorageReader {
            override def offset[T](partition: util.Map[String, T]): util.Map[String, AnyRef] = Map(
              "path" -> s"$PrefixName/$dir/$TopicName/0/399.${format.entryName.toLowerCase}",
              "line" -> "9".asInstanceOf[Object]
            ).asJava

            override def offsets[T](partitions: util.Collection[util.Map[String, T]]): util.Map[util.Map[String, T], util.Map[String, AnyRef]] = throw new IllegalStateException("Unexpected call to storage reader")
          }
        }
        task.initialize(context)

        val props = DefaultProps
          .combine(
            Map("connect.s3.kcql" -> s"insert into $TopicName select * from $BucketName:$PrefixName/$dir STOREAS `${format.entryName}$formatExtensionString` LIMIT 190")
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
    val (format, formatOptions) = (Format.Bytes, Some(FormatOptions.ValueOnly))
    val dir = "bytesval"

    val task = new S3SourceTask()

    val formatExtensionString = generateFormatString(formatOptions)

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $TopicName select * from $BucketName:$PrefixName/$dir STOREAS `${format.entryName}$formatExtensionString` LIMIT 190")
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

  "task" should "read stored bytes key/value files continuously" in {
    val (format, formatOptions) = (Format.Bytes, Some(FormatOptions.KeyAndValueWithSizes))

    val dir = "byteskv"
    val task = new S3SourceTask()

    val formatExtensionString = generateFormatString(formatOptions)

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $TopicName select * from $BucketName:$PrefixName/$dir STOREAS `${format.entryName}$formatExtensionString` LIMIT 190")
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

    sourceRecords1.get(0).key should be("myKey".getBytes)
    sourceRecords1.get(0).value() should be("somestring".getBytes)
  }

  "task" should "read stored nested bytes key/value files continuously" in {
    val (format, formatOptions) = (Format.Bytes, Some(FormatOptions.KeyAndValueWithSizes))

    val dir = "nested/byteskv"
    val task = new S3SourceTask()

    val formatExtensionString = generateFormatString(formatOptions)

    val props = DefaultProps
      .combine(
        Map("connect.s3.kcql" -> s"insert into $TopicName select * from $BucketName:$PrefixName/$dir STOREAS `${format.entryName}$formatExtensionString` LIMIT 190")
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

    sourceRecords1.get(0).key should be("myKey".getBytes)
    sourceRecords1.get(0).value() should be("somestring".getBytes)
  }

  override def cleanUpEnabled: Boolean = false

  override def setUpTestData(): Unit = {
    formats.foreach{
      case (format: Format, formatOptions: Option[FormatOptions], dir: String) =>
        setUpBucketData(BucketName, format, formatOptions, dir)
    }

    setUpBucketData(BucketName, Bytes, Some(KeyAndValueWithSizes), "byteskv")
    setUpBucketData(BucketName, Bytes, Some(ValueOnly), "bytesval")
    setUpBucketData(BucketName, Bytes, Some(KeyAndValueWithSizes), "nested/byteskv")
  }
}
