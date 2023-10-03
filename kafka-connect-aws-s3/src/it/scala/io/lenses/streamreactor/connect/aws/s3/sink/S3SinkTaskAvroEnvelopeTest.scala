/*
 * Copyright 2020 Lenses.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.lenses.streamreactor.connect.aws.s3.sink

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.utils.ITSampleSchemaAndData._
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.formats.AvroFormatReader
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

class S3SinkTaskAvroEnvelopeTest
    extends AnyFlatSpec
    with Matchers
    with S3ProxyContainerTest
    with MockitoSugar
    with LazyLogging {

  private val avroFormatReader = new AvroFormatReader()

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "myTopic"

  private def toSinkRecord(
    user:      Struct,
    topic:     String,
    partition: Int,
    offset:    Long,
    timestamp: Long,
    headers:   (String, SchemaAndValue)*,
  ) = {
    val record =
      new SinkRecord(topic, partition, schema, user, schema, user, offset, timestamp, TimestampType.CREATE_TIME)
    headers.foreach {
      case (name, schemaAndValue) =>
        record.headers().add(name, schemaAndValue)
    }
    record
  }

  private def toSinkRecord(
    value:     Any,
    topic:     String,
    partition: Int,
    offset:    Long,
    timestamp: Long,
    headers:   (String, SchemaAndValue)*,
  ) = {
    val record =
      new SinkRecord(topic, partition, null, value, null, value, offset, timestamp, TimestampType.CREATE_TIME)
    headers.foreach {
      case (name, schemaAndValue) =>
        record.headers().add(name, schemaAndValue)
    }
    record
  }

  "S3SinkTask" should "write to avro format" in {

    val task = new S3SinkTask()

    val props = (
      defaultProps +
        ("connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS AVRO WITH_FLUSH_COUNT = 3 PROPERTIES('store.envelope'=true, 'padding.length.partition'='12', 'padding.length.offset'='12')")
    ).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val struct1 = new Struct(schema).put("name", "sam").put("title", "mr").put("salary", 100.43)
    val struct2 = new Struct(schema).put("name", "laura").put("title", "ms").put("salary", 429.06)
    val struct3 = new Struct(schema).put("name", "tom").put("title", null).put("salary", 395.44)

    val record1 = toSinkRecord(
      struct1,
      TopicName,
      1,
      1L,
      10001L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header1"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 1L),
    )
    val record2 = toSinkRecord(
      struct2,
      TopicName,
      1,
      2L,
      10002L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header2"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 2L),
    )
    val record3 = toSinkRecord(
      struct3,
      TopicName,
      1,
      3L,
      10003L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header3"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 3L),
    )
    task.put(List(record1, record2, record3).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(1)

    val bytes = remoteFileAsBytes(BucketName, "streamReactorBackups/myTopic/000000000001/000000000003.avro")

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(3)

    val actual1 = genericRecords.head
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

    val actual2 = genericRecords(1)
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

    val actual3 = genericRecords(2)
    actual3.get("key") should be(a[GenericRecord])
    actual3.get("value") should be(a[GenericRecord])
    actual3.get("metadata") should be(a[GenericRecord])
    actual3.get("headers") should be(a[GenericRecord])

    val key3 = actual3.get("key").asInstanceOf[GenericRecord]
    key3.get("name").toString should be("tom")
    key3.get("title") should be(null)
    key3.get("salary") should be(395.44)

    val val3 = actual3.get("value").asInstanceOf[GenericRecord]
    val3.get("name").toString should be("tom")
    val3.get("title") should be(null)
    val3.get("salary") should be(395.44)

    val metadata3 = actual3.get("metadata").asInstanceOf[GenericRecord]
    metadata3.get("topic").toString should be(TopicName)
    metadata3.get("partition") should be(1)
    metadata3.get("offset") should be(3L)
    metadata3.get("timestamp") should be(10003L)

    val headers3 = actual3.get("headers").asInstanceOf[GenericRecord]
    headers3.get("h1").toString should be("record1-header3")
    headers3.get("h2") should be(3L)
  }

  "S3SinkTask" should "write to avro format when input is java Map" in {

    val task = new S3SinkTask()

    val props = (defaultProps + (
      "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS AVRO WITH_FLUSH_INTERVAL = 1 WITH_FLUSH_COUNT = 3 PROPERTIES('store.envelope'=true,'padding.length.partition'='12', 'padding.length.offset'='12')",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)
    val map1 = Map[String, Any]("name" -> "sam", "title" -> "mr", "salary" -> 100.43).asJava
    val map2 = Map[String, Any]("name" -> "laura", "title" -> "ms", "salary" -> 429.06).asJava
    val map3 = Map[String, Any]("name" -> "tom", "title" -> null.asInstanceOf[String], "salary" -> 395.44).asJava

    val record1 = toSinkRecord(
      map1,
      TopicName,
      1,
      1L,
      10001L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header1"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 1L),
    )
    val record2 = toSinkRecord(
      map2,
      TopicName,
      1,
      2L,
      10002L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header2"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 2L),
    )
    val record3 = toSinkRecord(
      map3,
      TopicName,
      1,
      3L,
      10003L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header3"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 3L),
    )
    task.put(List(record1, record2, record3).asJava)
    Thread.sleep(2000)
    task.put(List.empty[SinkRecord].asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/").size should be(2)

    val genericRecords1 =
      avroFormatReader.read(remoteFileAsBytes(BucketName,
                                              "streamReactorBackups/myTopic/000000000001/000000000002.avro",
      ))
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

    val genericRecords2 =
      avroFormatReader.read(remoteFileAsBytes(BucketName,
                                              "streamReactorBackups/myTopic/000000000001/000000000003.avro",
      ))
    genericRecords2.size should be(1)

    val actual3 = genericRecords2.head
    actual3.get("key") should be(a[GenericRecord])
    actual3.get("value") should be(a[GenericRecord])
    actual3.get("metadata") should be(a[GenericRecord])
    actual3.get("headers") should be(a[GenericRecord])

    val key3 = actual3.get("key").asInstanceOf[GenericRecord]
    key3.get("name").toString should be("tom")
    key3.getSchema().getField("title") should be(null)
    key3.get("salary") should be(395.44)

    val val3 = actual3.get("value").asInstanceOf[GenericRecord]
    val3.get("name").toString should be("tom")
    val3.getSchema().getField("title") should be(null)
    val3.get("salary") should be(395.44)

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
