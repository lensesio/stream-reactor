package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.formats.writer.parquet.ParquetOutputFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import io.lenses.streamreactor.connect.cloud.common.stream.CloudByteArrayOutputStream
import org.apache.avro.SchemaBuilder
import org.apache.kafka.connect.source.SourceRecord
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.EitherValues
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class S3SourceParquetEnvelopeTest
    extends S3ProxyContainerTest
    with AnyFlatSpecLike
    with Matchers
    with EitherValues
    with CloudSourceSettingsKeys {

  def DefaultProps: Map[String, String] = defaultProps + (
    SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
  )

  val MyPrefix  = "backups"
  val TopicName = "myTopic"

  private val TransactionSchema = SchemaBuilder.record("transaction").fields()
    .requiredString("id")
    .requiredString("name")
    .requiredString("email")
    .requiredString("card")
    .requiredString("ip")
    .requiredString("country")
    .requiredString("currency")
    .requiredString("timestamp")
    .endRecord()

  private val TransactionIdSchema = SchemaBuilder.record("transactionId").fields()
    .requiredString("id")
    .endRecord()

  private val MetadataSchema = SchemaBuilder.record("metadata").fields()
    .optionalLong("timestamp")
    .requiredString("topic")
    .requiredInt("partition")
    .requiredLong("offset")
    .endRecord()

  private val HeadersSchema = SchemaBuilder.record("headers").fields()
    .requiredString("header1")
    .requiredLong("header2")
    .endRecord()

  private val EnvelopeSchema = SchemaBuilder.record("envelope").fields()
    .name("key").`type`(TransactionIdSchema).noDefault()
    .name("value").`type`(TransactionSchema).noDefault()
    .name("headers").`type`(HeadersSchema).noDefault()
    .name("metadata").`type`(MetadataSchema).noDefault()
    .endRecord()

  override def cleanUp(): Unit = ()

  override def setUpTestData(storageInterface: AwsS3StorageInterface): Either[Throwable, Unit] = {

    val envelope = new org.apache.avro.generic.GenericData.Record(EnvelopeSchema)
    val key      = new org.apache.avro.generic.GenericData.Record(TransactionIdSchema)
    key.put("id", "1")
    envelope.put("key", key)
    val value = new org.apache.avro.generic.GenericData.Record(TransactionSchema)
    value.put("id", "1")
    value.put("name", "John Smith")
    value.put("email", "jn@johnsmith.com")
    value.put("card", "1234567890")
    value.put("ip", "192.168.0.2")
    value.put("country", "UK")
    value.put("currency", "GBP")
    value.put("timestamp", "2020-01-01T00:00:00.000Z")
    envelope.put("value", value)

    val headers = new org.apache.avro.generic.GenericData.Record(HeadersSchema)
    headers.put("header1", "value1")
    headers.put("header2", 123456789L)
    envelope.put("headers", headers)

    val metadata = new org.apache.avro.generic.GenericData.Record(MetadataSchema)
    metadata.put("timestamp", 1234567890L)
    metadata.put("topic", TopicName)
    metadata.put("partition", 3)
    metadata.put("offset", 0L)
    envelope.put("metadata", metadata)

    val file = new java.io.File("00001.parquet")
    try {
      val outputStream   = new java.io.BufferedOutputStream(new java.io.FileOutputStream(file))
      val s3OutputStream = new CloudByteArrayOutputStream
      val outputFile     = new ParquetOutputFile(s3OutputStream)
      val parquetWriter = AvroParquetWriter
        .builder[Any](outputFile)
        .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE.toLong)
        .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
        .withSchema(EnvelopeSchema)
        .withCompressionCodec(CompressionCodecName.GZIP)
        .build()
      parquetWriter.write(envelope)
      parquetWriter.close()

      val data = s3OutputStream.toByteArray
      outputStream.write(data)
      outputStream.flush()
      outputStream.close()
      storageInterface.uploadFile(UploadableFile(file), BucketName, s"$MyPrefix/parquet/0")
      ().asRight
    } finally {
      file.delete()
      ()
    }
  }

  "task" should "extract from parquet files containing the envelope" in {

    val task = new S3SourceTask()

    val props = (defaultProps ++
      Map(
        "connect.s3.kcql"                                   -> s"insert into $TopicName select * from $BucketName:$MyPrefix/parquet STOREAS `Parquet` LIMIT 1000 PROPERTIES ('store.envelope'=true)",
        "connect.s3.source.partition.search.recurse.levels" -> "0",
        "connect.s3.source.partition.search.continuous"     -> "false",
      ))
      .asJava

    task.start(props)

    var sourceRecords: Seq[SourceRecord] = List.empty
    try {
      eventually {
        do {
          sourceRecords = sourceRecords ++ task.poll().asScala
        } while (sourceRecords.size != 1)
        task.poll() should be(empty)
      }
    } finally {
      task.stop()
    }

    //assert the record matches the envelope
    val sourceRecord = sourceRecords.head
    sourceRecord.keySchema().name() should be("transactionId")
    sourceRecord.key().asInstanceOf[org.apache.kafka.connect.data.Struct].getString("id") should be("1")

    sourceRecord.valueSchema().name() should be("transaction")
    val valStruct = sourceRecord.value().asInstanceOf[org.apache.kafka.connect.data.Struct]
    valStruct.getString("id") should be("1")
    valStruct.getString("name") should be("John Smith")
    valStruct.getString("email") should be("jn@johnsmith.com")
    valStruct.getString("card") should be("1234567890")
    valStruct.getString("ip") should be("192.168.0.2")
    valStruct.getString("country") should be("UK")
    valStruct.getString("currency") should be("GBP")
    valStruct.getString("timestamp") should be("2020-01-01T00:00:00.000Z")

    sourceRecord.headers().asScala.map(h => h.key() -> h.value()).toMap should be(Map("header1" -> "value1",
                                                                                      "header2" -> 123456789L,
    ))

    sourceRecord.sourcePartition().asScala shouldBe Map("container" -> BucketName, "prefix" -> s"$MyPrefix/parquet/")
    val sourceOffsetMap = sourceRecord.sourceOffset().asScala
    sourceOffsetMap("path") shouldBe s"$MyPrefix/parquet/0"
    sourceOffsetMap("line") shouldBe "0"
    sourceOffsetMap("ts").toString.toLong < Instant.now().toEpochMilli shouldBe true

    sourceRecord.topic() shouldBe TopicName
    sourceRecord.kafkaPartition() shouldBe 3
    sourceRecord.timestamp() shouldBe 1234567890L
  }

}
