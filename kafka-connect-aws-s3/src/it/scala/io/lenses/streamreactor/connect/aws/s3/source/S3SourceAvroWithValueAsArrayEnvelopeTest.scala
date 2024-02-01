package io.lenses.streamreactor.connect.aws.s3.source

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.source.config.SourcePartitionSearcherSettingsKeys
import io.lenses.streamreactor.connect.aws.s3.storage.AwsS3StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.EitherValues
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class S3SourceAvroWithValueAsArrayEnvelopeTest
    extends S3ProxyContainerTest
    with AnyFlatSpecLike
    with Matchers
    with EitherValues
    with SourcePartitionSearcherSettingsKeys {

  def DefaultProps: Map[String, String] = defaultProps + (
    SOURCE_PARTITION_SEARCH_INTERVAL_MILLIS -> "1000",
  )

  val MyPrefix  = "backups"
  val TopicName = "myTopic"

  private val ArraySchema = SchemaBuilder.builder().bytesType()

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
    .name("key").`type`(ArraySchema).noDefault()
    .name("value").`type`(ArraySchema).noDefault()
    .name("headers").`type`(HeadersSchema).noDefault()
    .name("metadata").`type`(MetadataSchema).noDefault()
    .endRecord()

  override def cleanUp(): Unit = ()

  override def setUpTestData(storageInterface: AwsS3StorageInterface): Either[Throwable, Unit] = {

    val envelope = new org.apache.avro.generic.GenericData.Record(EnvelopeSchema)
    val key      = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9))
    envelope.put("key", key)
    val value = ByteBuffer.wrap(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9))
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

      storageInterface.uploadFile(UploadableFile(file), BucketName, s"$MyPrefix/avro/0")
      ().asRight
    } finally {
      file.delete()
      ()
    }

  }

  "task" should "extract from avro files containing the envelope" in {

    val task = new S3SourceTask()

    val props = (defaultProps ++ Map(
      "connect.s3.kcql"                            -> s"insert into $TopicName select * from $BucketName:$MyPrefix/avro STOREAS `AVRO` LIMIT 1000 PROPERTIES ('store.envelope'=true)",
      "connect.s3.partition.search.recurse.levels" -> "0",
      "connect.partition.search.continuous"        -> "false",
    )).asJava

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
    sourceRecord.keySchema().`type`() should be(Schema.Type.BYTES)
    sourceRecord.key().asInstanceOf[Array[Byte]] should be(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9))

    sourceRecord.valueSchema().`type`() should be(Schema.Type.BYTES)
    val value: Array[Byte] = sourceRecord.value().asInstanceOf[Array[Byte]]
    value should be(Array[Byte](1, 2, 3, 4, 5, 6, 7, 8, 9))

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
