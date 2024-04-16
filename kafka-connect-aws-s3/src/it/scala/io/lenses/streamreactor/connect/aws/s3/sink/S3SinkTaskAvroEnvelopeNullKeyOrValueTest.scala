package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.AuthMode
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings._
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushInterval
import io.lenses.streamreactor.connect.cloud.common.formats.AvroFormatReader
import io.lenses.streamreactor.connect.cloud.common.utils.ITSampleSchemaAndData._
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class S3SinkTaskAvroEnvelopeNullKeyOrValueTest
    extends AnyFlatSpec
    with Matchers
    with S3ProxyContainerTest
    with MockitoSugar
    with LazyLogging {

  private val avroFormatReader = new AvroFormatReader()

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "myTopic"

  private def DefaultProps = Map(
    AWS_ACCESS_KEY              -> s3Container.identity.identity,
    AWS_SECRET_KEY              -> s3Container.identity.credential,
    AUTH_MODE                   -> AuthMode.Credentials.toString,
    CUSTOM_ENDPOINT             -> s3Container.getEndpointUrl.toString,
    ENABLE_VIRTUAL_HOST_BUCKETS -> "true",
    "name"                      -> "s3SinkTaskBuildLocalTest",
    AWS_REGION                  -> "eu-west-1",
    TASK_INDEX                  -> "1:1",
  )

  "S3SinkTask" should "write to avro format with null value" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS AVRO PROPERTIES('store.envelope'=true, 'padding.length.partition'='12', 'padding.length.offset'='12', '${FlushInterval.entryName}'=1, '${FlushCount.entryName}'=3)",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val struct1 = new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43)
    val struct2 = new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06)
    val struct3 = new Struct(schema).put("name", "tom").put("title", null).put("salary", 395.44)

    val record1 =
      new SinkRecord(TopicName, 1, schema, struct1, schema, struct1, 1L, 10001L, TimestampType.CREATE_TIME)
    record1.headers.add("h1", new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header1"))
    record1.headers().add("h2", new SchemaAndValue(Schema.INT64_SCHEMA, 1L))

    val record2 = new SinkRecord(TopicName, 1, schema, struct2, schema, struct2, 2L, 10002L, TimestampType.CREATE_TIME)
    record2.headers().add("h1", new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header2")).add(
      "h2",
      new SchemaAndValue(Schema.INT64_SCHEMA, 2L),
    )

    val record3 = new SinkRecord(TopicName, 1, schema, struct3, null, null, 3L, 10003L, TimestampType.CREATE_TIME)
    record3.headers()
      .add("h1", new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header3"))
      .add("h2", new SchemaAndValue(Schema.INT64_SCHEMA, 3L))
    task.put(List(record1, record2, record3).asJava)

    Thread.sleep(1500)
    task.put(List[SinkRecord]().asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)

    val bytes1 = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.avro")

    val genericRecords1 = avroFormatReader.read(bytes1)
    genericRecords1.size should be(2)

    val actual1 = genericRecords1.head
    actual1.get("key") should be(a[GenericRecord])
    actual1.get("value") should be(a[GenericRecord])
    actual1.get("metadata") should be(a[GenericRecord])
    actual1.get("headers") should be(a[GenericRecord])

    val key1 = actual1.get("key").asInstanceOf[GenericRecord]
    key1.get("name").toString should be("sam")
    key1.get("title").toString should be("mr")
    key1.get("salary") should be(100.43)

    val val1 = actual1.get("value").asInstanceOf[GenericRecord]
    val1.get("name").toString should be("sam")
    val1.get("title").toString should be("mr")
    val1.get("salary") should be(100.43)

    val metadata1 = actual1.get("metadata").asInstanceOf[GenericRecord]
    metadata1.get("topic").toString should be(TopicName)
    metadata1.get("partition") should be(1)
    metadata1.get("offset") should be(1L)
    metadata1.get("timestamp") should be(10001L)

    val headers1 = actual1.get("headers").asInstanceOf[GenericRecord]
    headers1.get("h1").toString should be("record1-header1")
    headers1.get("h2") should be(1L)

    val actual2 = genericRecords1(1)
    actual2.get("key") should be(a[GenericRecord])
    actual2.get("value") should be(a[GenericRecord])
    actual2.get("metadata") should be(a[GenericRecord])
    actual2.get("headers") should be(a[GenericRecord])

    val key2 = actual2.get("key").asInstanceOf[GenericRecord]
    key2.get("name").toString should be("laura")
    key2.get("title").toString should be("ms")
    key2.get("salary") should be(429.06)

    val val2 = actual2.get("value").asInstanceOf[GenericRecord]
    val2.get("name").toString should be("laura")
    val2.get("title").toString should be("ms")
    val2.get("salary") should be(429.06)

    val metadata2 = actual2.get("metadata").asInstanceOf[GenericRecord]
    metadata2.get("topic").toString should be(TopicName)
    metadata2.get("partition") should be(1)
    metadata2.get("offset") should be(2L)
    metadata2.get("timestamp") should be(10002L)

    val headers2 = actual2.get("headers").asInstanceOf[GenericRecord]
    headers2.get("h1").toString should be("record1-header2")
    headers2.get("h2") should be(2L)

    val bytes2 = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.avro")

    val genericRecords2 = avroFormatReader.read(bytes2)
    genericRecords2.size should be(1)

    val actual3 = genericRecords2.head
    actual3.get("key") should be(a[GenericRecord])
    actual3.getSchema.getField("value") should be(null)
    actual3.get("metadata") should be(a[GenericRecord])
    actual3.get("headers") should be(a[GenericRecord])

    val key3 = actual3.get("key").asInstanceOf[GenericRecord]
    key3.get("name").toString should be("tom")
    key3.get("title") should be(null)
    key3.get("salary") should be(395.44)

    val metadata3 = actual3.get("metadata").asInstanceOf[GenericRecord]
    metadata3.get("topic").toString should be(TopicName)
    metadata3.get("partition") should be(1)
    metadata3.get("offset") should be(3L)
    metadata3.get("timestamp") should be(10003L)

    val headers3 = actual3.get("headers").asInstanceOf[GenericRecord]
    headers3.get("h1").toString should be("record1-header3")
    headers3.get("h2") should be(3L)
  }

  "S3SinkTask" should "write to avro format with null key" in {

    val task = new S3SinkTask()

    val props = DefaultProps
      .combine(
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS AVRO PROPERTIES('store.envelope'=true, 'padding.length.partition'='12', 'padding.length.offset'='12','${FlushInterval.entryName}'=1, '${FlushCount.entryName}'=3)",
        ),
      ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val struct1 = new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43)
    val struct2 = new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06)
    val struct3 = new Struct(schema).put("name", "tom").put("title", null).put("salary", 395.44)

    val record1 =
      new SinkRecord(TopicName, 1, schema, struct1, schema, struct1, 1L, 10001L, TimestampType.CREATE_TIME)
    record1.headers.add("h1", new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header1"))
    record1.headers().add("h2", new SchemaAndValue(Schema.INT64_SCHEMA, 1L))

    val record2 = new SinkRecord(TopicName, 1, schema, struct2, schema, struct2, 2L, 10002L, TimestampType.CREATE_TIME)
    record2.headers().add("h1", new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header2")).add(
      "h2",
      new SchemaAndValue(Schema.INT64_SCHEMA, 2L),
    )

    val record3 = new SinkRecord(TopicName, 1, null, null, schema, struct3, 3L, 10003L, TimestampType.CREATE_TIME)
    record3.headers()
      .add("h1", new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header3"))
      .add("h2", new SchemaAndValue(Schema.INT64_SCHEMA, 3L))
    task.put(List(record1, record2, record3).asJava)

    Thread.sleep(1500)
    task.put(List[SinkRecord]().asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)

    val bytes1 = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000002.avro")

    val genericRecords1 = avroFormatReader.read(bytes1)
    genericRecords1.size should be(2)

    val actual1 = genericRecords1.head
    actual1.get("key") should be(a[GenericRecord])
    actual1.get("value") should be(a[GenericRecord])
    actual1.get("metadata") should be(a[GenericRecord])
    actual1.get("headers") should be(a[GenericRecord])

    val key1 = actual1.get("key").asInstanceOf[GenericRecord]
    key1.get("name").toString should be("sam")
    key1.get("title").toString should be("mr")
    key1.get("salary") should be(100.43)

    val val1 = actual1.get("value").asInstanceOf[GenericRecord]
    val1.get("name").toString should be("sam")
    val1.get("title").toString should be("mr")
    val1.get("salary") should be(100.43)

    val metadata1 = actual1.get("metadata").asInstanceOf[GenericRecord]
    metadata1.get("topic").toString should be(TopicName)
    metadata1.get("partition") should be(1)
    metadata1.get("offset") should be(1L)
    metadata1.get("timestamp") should be(10001L)

    val headers1 = actual1.get("headers").asInstanceOf[GenericRecord]
    headers1.get("h1").toString should be("record1-header1")
    headers1.get("h2") should be(1L)

    val actual2 = genericRecords1(1)
    actual2.get("key") should be(a[GenericRecord])
    actual2.get("value") should be(a[GenericRecord])
    actual2.get("metadata") should be(a[GenericRecord])
    actual2.get("headers") should be(a[GenericRecord])

    val key2 = actual2.get("key").asInstanceOf[GenericRecord]
    key2.get("name").toString should be("laura")
    key2.get("title").toString should be("ms")
    key2.get("salary") should be(429.06)

    val val2 = actual2.get("value").asInstanceOf[GenericRecord]
    val2.get("name").toString should be("laura")
    val2.get("title").toString should be("ms")
    val2.get("salary") should be(429.06)

    val metadata2 = actual2.get("metadata").asInstanceOf[GenericRecord]
    metadata2.get("topic").toString should be(TopicName)
    metadata2.get("partition") should be(1)
    metadata2.get("offset") should be(2L)
    metadata2.get("timestamp") should be(10002L)

    val headers2 = actual2.get("headers").asInstanceOf[GenericRecord]
    headers2.get("h1").toString should be("record1-header2")
    headers2.get("h2") should be(2L)

    val bytes2 = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.avro")

    val genericRecords2 = avroFormatReader.read(bytes2)
    genericRecords2.size should be(1)

    val actual3 = genericRecords2.head
    actual3.getSchema.getField("key") should be(null)
    actual3.get("value") should be(a[GenericRecord])
    actual3.get("metadata") should be(a[GenericRecord])
    actual3.get("headers") should be(a[GenericRecord])

    val value3 = actual3.get("value").asInstanceOf[GenericRecord]
    value3.get("name").toString should be("tom")
    value3.get("title") should be(null)
    value3.get("salary") should be(395.44)

    val metadata3 = actual3.get("metadata").asInstanceOf[GenericRecord]
    metadata3.get("topic").toString should be(TopicName)
    metadata3.get("partition") should be(1)
    metadata3.get("offset") should be(3L)
    metadata3.get("timestamp") should be(10003L)

    val headers3 = actual3.get("headers").asInstanceOf[GenericRecord]
    headers3.get("h1").toString should be("record1-header3")
    headers3.get("h2") should be(3L)
  }
}
