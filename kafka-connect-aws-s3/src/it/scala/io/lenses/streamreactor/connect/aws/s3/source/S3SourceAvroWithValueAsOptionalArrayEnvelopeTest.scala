package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.toBifunctorOps
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.source.config.CloudSourceSettingsKeys
import org.apache.avro
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.kafka.connect.data.Schema
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class S3SourceAvroWithValueAsOptionalArrayEnvelopeTest
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

  //optional bytes avro
  private val OptionalArraySchema: avro.Schema = SchemaBuilder.nullable().bytesType()

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
    .name("key").`type`(OptionalArraySchema).noDefault()
    .name("value").`type`(OptionalArraySchema).noDefault()
    .name("headers").`type`(HeadersSchema).noDefault()
    .name("metadata").`type`(MetadataSchema).noDefault()
    .endRecord()

  override def cleanUp(): Unit = ()

  override def setUpTestData(storageInterface: AwsS3StorageInterface): Either[Throwable, Unit] = {

    val envelope = new org.apache.avro.generic.GenericData.Record(EnvelopeSchema)
    envelope.put("key", null)
    envelope.put("value", null)

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

      storageInterface.uploadFile(UploadableFile(file), BucketName, s"$MyPrefix/avro/0")
        .leftMap(e => new UploadException(e))
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
      val sourceRecords = SourceRecordsLoop.loop(task, 10.seconds.toMillis, 1).value

      task.poll() should be(empty)
      //assert the record matches the envelope
      val sourceRecord = sourceRecords.head
      sourceRecord.keySchema().`type`() should be(Schema.Type.BYTES)
      sourceRecord.key() shouldBe null

      sourceRecord.valueSchema().`type`() should be(Schema.Type.BYTES)
      val value: Array[Byte] = sourceRecord.value().asInstanceOf[Array[Byte]]
      value shouldBe null

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
    } finally {
      task.stop()
    }

  }
}

class S3SourceAvroWithValueAsOptionalArrayMixValuesEnvelopeTest
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

  //optional bytes avro
  private val OptionalArraySchema: avro.Schema = SchemaBuilder.nullable().bytesType()

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
    .name("key").`type`(OptionalArraySchema).noDefault()
    .name("value").`type`(OptionalArraySchema).noDefault()
    .name("headers").`type`(HeadersSchema).noDefault()
    .name("metadata").`type`(MetadataSchema).noDefault()
    .endRecord()

  override def cleanUp(): Unit = ()

  override def setUpTestData(storageInterface: AwsS3StorageInterface): Either[Throwable, Unit] = {

    val envelope = new org.apache.avro.generic.GenericData.Record(EnvelopeSchema)
    envelope.put("key", null)
    val buffer = java.nio.ByteBuffer.wrap("value".getBytes())
    envelope.put("value", buffer)

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

      storageInterface.uploadFile(UploadableFile(file), BucketName, s"$MyPrefix/avro/0")
        .leftMap(e => new UploadException(e))
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
      val sourceRecords = SourceRecordsLoop.loop(task, 10.seconds.toMillis, 1).value
      task.poll() should be(empty)
      val sourceRecord = sourceRecords.head
      sourceRecord.keySchema().`type`() should be(Schema.Type.BYTES)
      sourceRecord.key() shouldBe null

      sourceRecord.valueSchema().`type`() should be(Schema.Type.BYTES)
      val value: Array[Byte] = sourceRecord.value().asInstanceOf[Array[Byte]]
      value shouldBe "value".getBytes()

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
    } finally {
      task.stop()
    }
  }
}
