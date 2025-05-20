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
import io.lenses.streamreactor.connect.aws.s3.utils.S3ProxyContainerTest
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.cloud.common.formats.AvroFormatReader
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class S3SinkTaskAvroSchemaOptimizationTest
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
      new SinkRecord(topic,
                     partition,
                     user.schema(),
                     user,
                     user.schema(),
                     user,
                     offset,
                     timestamp,
                     TimestampType.CREATE_TIME,
      )
    headers.foreach {
      case (name, schemaAndValue) =>
        record.headers().add(name, schemaAndValue)
    }
    record
  }

  "S3SinkTask" should "write to avro format using the latest schema" in {
    val props = (
      defaultProps ++
        Map(
          "connect.s3.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS AVRO PROPERTIES('padding.length.partition'='12','padding.length.offset'='12','${FlushCount.entryName}'=4)",
          "connect.s3.latest.schema.optimization.enabled" -> "true",
        )
    ).asJava
    val expectedFile = "streamReactorBackups/myTopic/000000000001/000000000004_10001_10004.avro"

    val task = new S3SinkTask()
    val ctx  = mock[SinkTaskContext]
    task.initialize(ctx)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    val schema1 = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .version(1)
      .build()

    val schema2 = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .version(2)
      .build()

    val metadataSchema = SchemaBuilder.struct()
      .field("topic", Schema.STRING_SCHEMA)
      .field("partition", Schema.INT32_SCHEMA)
      .field("offset", Schema.INT64_SCHEMA)
      .field("timestamp", Schema.INT64_SCHEMA)
      .optional()
      .build()

    val schema3 = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .field("metadata", metadataSchema)
      .version(3)
      .build()

    val struct1 = new Struct(schema1).put("name", "sam").put("title", "mr")
    val struct2 = new Struct(schema2).put("name", "laura").put("title", "ms").put("salary", 429.06)
    val struct3 = new Struct(schema3).put("name", "tom").put("title", null).put("salary", 395.44)
    val metadataStruct =
      new Struct(metadataSchema).put("topic", TopicName).put("partition", 1).put("offset", 1L).put("timestamp", 10001L)

    val struct4 = new Struct(schema3).put("name", "jack").put("title", "mr").put("salary", 500.0)
      .put("metadata", metadataStruct)

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
    val record4 = toSinkRecord(
      struct4,
      TopicName,
      1,
      4L,
      10004L,
      "h1" -> new SchemaAndValue(Schema.STRING_SCHEMA, "record1-header4"),
      "h2" -> new SchemaAndValue(Schema.INT64_SCHEMA, 4L),
    )
    task.put(List(record1, record2, record3, record4).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val files = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")
    files.size should be(1)
    val bytes = remoteFileAsBytes(BucketName, expectedFile)

    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(4)

    val val1 = genericRecords.head
    val1.get("name").toString should be("sam")
    val1.get("title").toString should be("mr")
    val1.get("salary") shouldBe null
    val1.get("metadata") should be(null)

    val val2 = genericRecords(1)
    val2.get("name").toString should be("laura")
    val2.get("title").toString should be("ms")
    val2.get("salary") should be(429.06)
    val2.get("metadata") should be(null)

    val val3 = genericRecords(2)
    val3.get("name").toString should be("tom")
    val3.get("title") should be(null)
    val3.get("salary") should be(395.44)
    val3.get("metadata") should be(null)

    val val4 = genericRecords(3)
    val4.get("name").toString should be("jack")
    val4.get("title").toString should be("mr")
    val4.get("salary") should be(500.0)
    val valMetadata4 = val4.get("metadata").asInstanceOf[GenericRecord]
    valMetadata4.get("topic").toString should be(TopicName)
    valMetadata4.get("partition") should be(1)
    valMetadata4.get("offset") should be(1L)
    valMetadata4.get("timestamp") should be(10001L)

  }

  "S3SinkTask" should "handle nested meta changes between schema3 and schema4 with new id field" in {
    val props = (
      defaultProps ++
        Map(
          "connect.s3.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS AVRO PROPERTIES('padding.length.partition'='12','padding.length.offset'='12','${FlushCount.entryName}'=4)",
          "connect.s3.latest.schema.optimization.enabled" -> "true",
        )
    ).asJava
    val expectedFile = "streamReactorBackups/myTopic/000000000001/000000000004_10001_10004.avro"

    val task = new S3SinkTask()
    val ctx  = mock[SinkTaskContext]
    task.initialize(ctx)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // ——— define schemas ———
    val schema1 = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .version(1)
      .build()

    val schema2 = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .version(2)
      .build()

    // meta for schema3
    val metadataSchema3 = SchemaBuilder.struct()
      .field("topic", Schema.STRING_SCHEMA)
      .field("partition", Schema.INT32_SCHEMA)
      .field("offset", Schema.INT64_SCHEMA)
      .field("timestamp", Schema.INT64_SCHEMA)
      .optional()
      .build()

    // extended meta for schema4: adds an extra metaData field
    val metadataSchema4 = SchemaBuilder.struct()
      .field("topic", Schema.STRING_SCHEMA)
      .field("partition", Schema.INT32_SCHEMA)
      .field("offset", Schema.INT64_SCHEMA)
      .field("timestamp", Schema.INT64_SCHEMA)
      .field("metadata", SchemaBuilder.string().optional().build())
      .optional()
      .build()

    val schema3 = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .field("meta", metadataSchema3)
      .version(3)
      .build()

    // schema4 adds both the extended meta and a new optional id field
    val schema4 = SchemaBuilder.struct()
      .field("name", SchemaBuilder.string().required().build())
      .field("title", SchemaBuilder.string().optional().build())
      .field("salary", SchemaBuilder.float64().optional().build())
      .field("meta", metadataSchema4)
      .field("id", SchemaBuilder.int32().optional().build())
      .version(4)
      .build()

    val struct1 = new Struct(schema1).put("name", "sam").put("title", "mr")
    val struct2 = new Struct(schema2).put("name", "laura").put("title", "ms").put("salary", 429.06)

    val meta3 = new Struct(metadataSchema3)
      .put("topic", TopicName).put("partition", 1).put("offset", 3L).put("timestamp", 10003L)
    val struct3 = new Struct(schema3).put("name", "tom").put("title", null).put("salary", 395.44)
      .put("meta", meta3)

    // schema4 struct with extended meta + id
    val meta4 = new Struct(metadataSchema4)
      .put("topic", TopicName)
      .put("partition", 1)
      .put("offset", 4L)
      .put("timestamp", 10004L)
      .put("metadata", "extra-info")
    val struct4 = new Struct(schema4)
      .put("name", "jack")
      .put("title", "dr")
      .put("salary", 500.0)
      .put("meta", meta4)
      .put("id", 42)

    val record1 = toSinkRecord(struct1, TopicName, 1, 1L, 10001L)
    val record2 = toSinkRecord(struct2, TopicName, 1, 2L, 10002L)
    val record3 = toSinkRecord(struct3, TopicName, 1, 3L, 10003L)
    val record4 = toSinkRecord(struct4, TopicName, 1, 4L, 10004L)

    task.put(List(record1, record2, record3, record4).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val files = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")
    files.size should be(1)
    val bytes          = remoteFileAsBytes(BucketName, expectedFile)
    val genericRecords = avroFormatReader.read(bytes)
    genericRecords.size should be(4)

    // record1 & 2 & 3 → no meta, no id
    genericRecords.take(3).foreach { rec =>
      rec.get("id") should be(null)
    }
    genericRecords.take(2).foreach { rec =>
      rec.get("meta") should be(null)
    }

    // record 3 → has meta, no id
    val rec3 = genericRecords(2)
    rec3.get("name").toString should be("tom")
    rec3.get("title") should be(null)
    rec3.get("salary") should be(395.44)
    val metaRec3 = rec3.get("meta").asInstanceOf[GenericRecord]
    metaRec3.get("topic").toString should be(TopicName)
    metaRec3.get("partition") should be(1)
    metaRec3.get("offset") should be(3L)
    metaRec3.get("timestamp") should be(10003L)
    metaRec3.get("metadata") should be(null)
    rec3.get("id") should be(null)

    // record4 → has extended meta + id
    val rec4 = genericRecords(3)
    rec4.get("name").toString should be("jack")
    rec4.get("salary") should be(500.0)
    val metaRec4 = rec4.get("meta").asInstanceOf[GenericRecord]
    metaRec4.get("topic").toString should be(TopicName)
    metaRec4.get("offset") should be(4L)
    metaRec4.get("metadata").toString should be("extra-info")
    rec4.get("id") should be(42)
  }

}
