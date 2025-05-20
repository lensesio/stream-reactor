package io.lenses.streamreactor.connect.aws.s3.source

import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.SpanSugar.convertIntToGrainOfTime

import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class S3SourceAvroEnvelopeTest
    extends S3ProxyContainerTest
    with AnyFlatSpecLike
    with Matchers
    with EitherValues
    with CloudSourceSettingsKeys
    with TempFileHelper {

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

    withFile("00001.avro") { file =>
      val outputStream = new java.io.BufferedOutputStream(new java.io.FileOutputStream(file))
      val writer: GenericDatumWriter[Any] = new GenericDatumWriter[Any](EnvelopeSchema)
      val fileWriter: DataFileWriter[Any] =
        new DataFileWriter[Any](writer).setCodec(CodecFactory.snappyCodec()).create(EnvelopeSchema, outputStream)

      fileWriter.append(envelope)
      fileWriter.flush()
      fileWriter.close()

      file.exists() shouldBe true
      storageInterface.uploadFile(UploadableFile(file), BucketName, s"$MyPrefix/avro/0")
    }
  }

  "task" should "extract from avro files containing the envelope" in {

    val task = new S3SourceTask()

    val props = (defaultProps ++ Map(
      "connect.s3.kcql"                                   -> s"insert into $TopicName select * from $BucketName:$MyPrefix/avro STOREAS `AVRO` LIMIT 1000 PROPERTIES ('store.envelope'=true)",
      "connect.s3.source.partition.search.recurse.levels" -> "0",
      "connect.s3.source.partition.search.continuous"     -> "false",
    )).asJava

    task.start(props)

    try {
      val sourceRecords =
        SourceRecordsLoop.loop(task, 30.seconds.toMillis, 1).getOrElse(fail("No records returned within timeout"))
      task.poll() should be(empty)
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

      sourceRecord.headers().asScala.map(h => h.key() -> h.value()).toMap should be(Map[String, Any](
        "header1" -> "value1",
        "header2" -> 123456789L,
      ))

      sourceRecord.sourcePartition().asScala shouldBe Map("container" -> BucketName, "prefix" -> s"$MyPrefix/avro/")
      val sourceOffsetMap = sourceRecord.sourceOffset().asScala
      sourceOffsetMap("path") shouldBe s"$MyPrefix/avro/0"
      sourceOffsetMap("line") shouldBe "0"
      sourceOffsetMap("ts").toString.toLong < Instant.now().toEpochMilli shouldBe true

      sourceRecord.topic() shouldBe TopicName
      sourceRecord.kafkaPartition() shouldBe 3
      sourceRecord.timestamp() shouldBe 1234567890L
    } finally {
      task.stop()
    }
  }
}
