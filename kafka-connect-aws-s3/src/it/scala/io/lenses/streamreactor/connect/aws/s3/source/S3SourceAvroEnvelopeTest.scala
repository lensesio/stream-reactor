package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits._
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.EitherValues
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class S3SourceAvroEnvelopeTest extends S3ProxyContainerTest with AnyFlatSpecLike with Matchers with EitherValues {

  def DefaultProps: Map[String, String] = Map(
    AWS_ACCESS_KEY                          -> Identity,
    AWS_SECRET_KEY                          -> Credential,
    AWS_REGION                              -> "eu-west-1",
    AUTH_MODE                               -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT                         -> uri(),
    ENABLE_VIRTUAL_HOST_BUCKETS             -> "true",
    TASK_INDEX                              -> "0:1",
    "name"                                  -> "s3-source",
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
  override def cleanUpEnabled: Boolean = false

  override def setUpTestData(): Unit = {

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

    val file = new java.io.File("00001.avro")
    try {
      val outputStream = new java.io.BufferedOutputStream(new java.io.FileOutputStream(file))
      val writer: GenericDatumWriter[Any] = new GenericDatumWriter[Any](EnvelopeSchema)
      val fileWriter: DataFileWriter[Any] =
        new DataFileWriter[Any](writer).setCodec(CodecFactory.snappyCodec()).create(EnvelopeSchema, outputStream)

      fileWriter.append(envelope)
      fileWriter.flush()
      fileWriter.close()

      storageInterface.uploadFile(file, BucketName, s"$MyPrefix/avro/0")
      ()
    } finally {
      file.delete()
      ()
    }
  }

  "task" should "extract from avro files containing the envelope" in {

    val task = new S3SourceTaskDeprecated()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql"                            -> s"insert into $TopicName select * from $BucketName:$MyPrefix/avro STOREAS `AVRO` LIMIT 1000 PROPERTIES ('store.envelope'=true)",
          "connect.s3.partition.search.recurse.levels" -> "0",
          "connect.partition.search.continuous"        -> "false",
        ),
      ).asJava

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

    sourceRecord.sourcePartition().asScala shouldBe Map("container" -> BucketName, "prefix" -> s"$MyPrefix/avro/")
    val sourceOffsetMap = sourceRecord.sourceOffset().asScala
    sourceOffsetMap("path") shouldBe s"$MyPrefix/avro/0"
    sourceOffsetMap("line") shouldBe "0"
    sourceOffsetMap("ts").toString.toLong < Instant.now().toEpochMilli shouldBe true

    sourceRecord.topic() shouldBe TopicName
    sourceRecord.kafkaPartition() shouldBe 3
    sourceRecord.timestamp() shouldBe 1234567890L
  }

}
