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
import io.lenses.streamreactor.connect.cloud.common.formats.reader.ParquetFormatReader
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

/**
 * Integration test for Parquet format with schema optimization enabled.
 *
 * This test verifies that the fix for ArrayIndexOutOfBoundsException works correctly
 * when latest.schema.optimization.enabled is true and records with different schema
 * versions are interleaved, requiring adaptation to the latest schema.
 */
class S3SinkTaskParquetSchemaOptimizationTest
    extends AnyFlatSpec
    with Matchers
    with S3ProxyContainerTest
    with MockitoSugar
    with LazyLogging {

  private val parquetFormatReader = new ParquetFormatReader()

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "schemaEvolutionTopic"

  private def toSinkRecord(
    value:     Struct,
    topic:     String,
    partition: Int,
    offset:    Long,
    timestamp: Long,
    headers:   (String, SchemaAndValue)*,
  ): SinkRecord = {
    val record =
      new SinkRecord(topic,
                     partition,
                     value.schema(),
                     value,
                     value.schema(),
                     value,
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

  "S3SinkTask" should "write to parquet format using latest schema optimization with interleaved schema versions" in {
    val props = (
      defaultProps ++
        Map(
          "connect.s3.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS PARQUET PROPERTIES('padding.length.partition'='12','padding.length.offset'='12','${FlushCount.entryName}'=6)",
          "connect.s3.latest.schema.optimization.enabled" -> "true",
        )
    ).asJava
    val expectedFile = "streamReactorBackups/schemaEvolutionTopic/000000000001/000000000006_10001_10006.parquet"

    val task = new S3SinkTask()
    val ctx  = mock[SinkTaskContext]
    task.initialize(ctx)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Schema V1: Basic user schema
    val schemaV1 = SchemaBuilder.struct()
      .name("com.example.User")
      .field("name", SchemaBuilder.string().required().build())
      .field("age", SchemaBuilder.int32().optional().build())
      .version(1)
      .build()

    // Schema V2: Adds optional email field
    val schemaV2 = SchemaBuilder.struct()
      .name("com.example.User")
      .field("name", SchemaBuilder.string().required().build())
      .field("age", SchemaBuilder.int32().optional().build())
      .field("email", SchemaBuilder.string().optional().build())
      .version(2)
      .build()

    // Schema V3: Adds nested address struct
    val addressSchema = SchemaBuilder.struct()
      .name("com.example.Address")
      .field("street", Schema.OPTIONAL_STRING_SCHEMA)
      .field("city", Schema.OPTIONAL_STRING_SCHEMA)
      .field("zipCode", Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build()

    val schemaV3 = SchemaBuilder.struct()
      .name("com.example.User")
      .field("name", SchemaBuilder.string().required().build())
      .field("age", SchemaBuilder.int32().optional().build())
      .field("email", SchemaBuilder.string().optional().build())
      .field("address", addressSchema)
      .version(3)
      .build()

    // Create records with different schema versions interleaved
    // Record 1: V1 schema
    val struct1 = new Struct(schemaV1).put("name", "Alice").put("age", 30)

    // Record 2: V2 schema (introduces email)
    val struct2 = new Struct(schemaV2).put("name", "Bob").put("age", 25).put("email", "bob@example.com")

    // Record 3: V1 schema again (should be adapted to latest)
    val struct3 = new Struct(schemaV1).put("name", "Charlie").put("age", 35)

    // Record 4: V3 schema (introduces address)
    val address4 = new Struct(addressSchema).put("street", "123 Main St").put("city", "Seattle").put("zipCode", "98101")
    val struct4 =
      new Struct(schemaV3).put("name", "Diana").put("age", 28).put("email", "diana@example.com").put("address",
                                                                                                     address4,
      )

    // Record 5: V2 schema again (should be adapted to latest V3)
    val struct5 = new Struct(schemaV2).put("name", "Eve").put("age", 32).put("email", "eve@example.com")

    // Record 6: V1 schema again (should be adapted to latest V3)
    val struct6 = new Struct(schemaV1).put("name", "Frank").put("age", 40)

    val record1 = toSinkRecord(struct1, TopicName, 1, 1L, 10001L)
    val record2 = toSinkRecord(struct2, TopicName, 1, 2L, 10002L)
    val record3 = toSinkRecord(struct3, TopicName, 1, 3L, 10003L)
    val record4 = toSinkRecord(struct4, TopicName, 1, 4L, 10004L)
    val record5 = toSinkRecord(struct5, TopicName, 1, 5L, 10005L)
    val record6 = toSinkRecord(struct6, TopicName, 1, 6L, 10006L)

    task.put(List(record1, record2, record3, record4, record5, record6).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    // Verify output - all records should be in a single file with the latest schema
    val files = listBucketPath(BucketName, "streamReactorBackups/schemaEvolutionTopic/000000000001/")
    files.size should be(1)
    val bytes = remoteFileAsBytes(BucketName, expectedFile)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(6)

    // All records should have been adapted to Schema V3
    // Record 1: V1 -> V3 (age present, email and address null)
    val rec1 = genericRecords.head
    rec1.get("name").toString should be("Alice")
    rec1.get("age") should be(30)
    rec1.get("email") should be(null)
    rec1.get("address") should be(null)

    // Record 2: V2 -> V3 (email present, address null)
    val rec2 = genericRecords(1)
    rec2.get("name").toString should be("Bob")
    rec2.get("age") should be(25)
    rec2.get("email").toString should be("bob@example.com")
    rec2.get("address") should be(null)

    // Record 3: V1 -> V3
    val rec3 = genericRecords(2)
    rec3.get("name").toString should be("Charlie")
    rec3.get("age") should be(35)
    rec3.get("email") should be(null)
    rec3.get("address") should be(null)

    // Record 4: V3 (full schema)
    val rec4        = genericRecords(3)
    val address4Rec = rec4.get("address").asInstanceOf[GenericRecord]
    rec4.get("name").toString should be("Diana")
    rec4.get("age") should be(28)
    rec4.get("email").toString should be("diana@example.com")
    address4Rec.get("street").toString should be("123 Main St")
    address4Rec.get("city").toString should be("Seattle")
    address4Rec.get("zipCode").toString should be("98101")

    // Record 5: V2 -> V3
    val rec5 = genericRecords(4)
    rec5.get("name").toString should be("Eve")
    rec5.get("age") should be(32)
    rec5.get("email").toString should be("eve@example.com")
    rec5.get("address") should be(null)

    // Record 6: V1 -> V3
    val rec6 = genericRecords(5)
    rec6.get("name").toString should be("Frank")
    rec6.get("age") should be(40)
    rec6.get("email") should be(null)
    rec6.get("address") should be(null)
  }

  "S3SinkTask" should "handle nested struct evolution with parquet and schema optimization" in {
    val props = (
      defaultProps ++
        Map(
          "connect.s3.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS PARQUET PROPERTIES('padding.length.partition'='12','padding.length.offset'='12','${FlushCount.entryName}'=4)",
          "connect.s3.latest.schema.optimization.enabled" -> "true",
        )
    ).asJava
    val expectedFile = "streamReactorBackups/schemaEvolutionTopic/000000000001/000000000004_20001_20004.parquet"

    val task = new S3SinkTask()
    val ctx  = mock[SinkTaskContext]
    task.initialize(ctx)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Schema V1: metadata with basic fields
    val metadataSchemaV1 = SchemaBuilder.struct()
      .name("com.example.Metadata")
      .field("topic", Schema.STRING_SCHEMA)
      .field("partition", Schema.INT32_SCHEMA)
      .optional()
      .build()

    val orderSchemaV1 = SchemaBuilder.struct()
      .name("com.example.Order")
      .field("orderId", Schema.STRING_SCHEMA)
      .field("amount", Schema.FLOAT64_SCHEMA)
      .field("metadata", metadataSchemaV1)
      .version(1)
      .build()

    // Schema V2: metadata adds offset and timestamp fields
    val metadataSchemaV2 = SchemaBuilder.struct()
      .name("com.example.Metadata")
      .field("topic", Schema.STRING_SCHEMA)
      .field("partition", Schema.INT32_SCHEMA)
      .field("offset", Schema.OPTIONAL_INT64_SCHEMA)
      .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
      .optional()
      .build()

    val orderSchemaV2 = SchemaBuilder.struct()
      .name("com.example.Order")
      .field("orderId", Schema.STRING_SCHEMA)
      .field("amount", Schema.FLOAT64_SCHEMA)
      .field("metadata", metadataSchemaV2)
      .version(2)
      .build()

    // Record 1: V1 schema
    val meta1   = new Struct(metadataSchemaV1).put("topic", TopicName).put("partition", 1)
    val struct1 = new Struct(orderSchemaV1).put("orderId", "ORD-001").put("amount", 100.50).put("metadata", meta1)

    // Record 2: V2 schema (introduces offset and timestamp in metadata)
    val meta2 =
      new Struct(metadataSchemaV2).put("topic", TopicName).put("partition", 1).put("offset", 2L).put("timestamp",
                                                                                                     20002L,
      )
    val struct2 = new Struct(orderSchemaV2).put("orderId", "ORD-002").put("amount", 250.75).put("metadata", meta2)

    // Record 3: V1 schema again (should be adapted to V2)
    val meta3   = new Struct(metadataSchemaV1).put("topic", TopicName).put("partition", 1)
    val struct3 = new Struct(orderSchemaV1).put("orderId", "ORD-003").put("amount", 75.00).put("metadata", meta3)

    // Record 4: V2 schema
    val meta4 =
      new Struct(metadataSchemaV2).put("topic", TopicName).put("partition", 1).put("offset", 4L).put("timestamp",
                                                                                                     20004L,
      )
    val struct4 = new Struct(orderSchemaV2).put("orderId", "ORD-004").put("amount", 500.00).put("metadata", meta4)

    val record1 = toSinkRecord(struct1, TopicName, 1, 1L, 20001L)
    val record2 = toSinkRecord(struct2, TopicName, 1, 2L, 20002L)
    val record3 = toSinkRecord(struct3, TopicName, 1, 3L, 20003L)
    val record4 = toSinkRecord(struct4, TopicName, 1, 4L, 20004L)

    task.put(List(record1, record2, record3, record4).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val files = listBucketPath(BucketName, "streamReactorBackups/schemaEvolutionTopic/000000000001/")
    files.size should be(1)
    val bytes = remoteFileAsBytes(BucketName, expectedFile)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(4)

    // Record 1: V1 -> V2 (metadata.offset and metadata.timestamp should be null)
    val rec1     = genericRecords.head
    val metaRec1 = rec1.get("metadata").asInstanceOf[GenericRecord]
    rec1.get("orderId").toString should be("ORD-001")
    rec1.get("amount") should be(100.50)
    metaRec1.get("topic").toString should be(TopicName)
    metaRec1.get("partition") should be(1)
    metaRec1.get("offset") should be(null)
    metaRec1.get("timestamp") should be(null)

    // Record 2: V2 (full metadata)
    val rec2     = genericRecords(1)
    val metaRec2 = rec2.get("metadata").asInstanceOf[GenericRecord]
    rec2.get("orderId").toString should be("ORD-002")
    rec2.get("amount") should be(250.75)
    metaRec2.get("topic").toString should be(TopicName)
    metaRec2.get("partition") should be(1)
    metaRec2.get("offset") should be(2L)
    metaRec2.get("timestamp") should be(20002L)

    // Record 3: V1 -> V2
    val rec3     = genericRecords(2)
    val metaRec3 = rec3.get("metadata").asInstanceOf[GenericRecord]
    rec3.get("orderId").toString should be("ORD-003")
    rec3.get("amount") should be(75.00)
    metaRec3.get("topic").toString should be(TopicName)
    metaRec3.get("partition") should be(1)
    metaRec3.get("offset") should be(null)
    metaRec3.get("timestamp") should be(null)

    // Record 4: V2
    val rec4     = genericRecords(3)
    val metaRec4 = rec4.get("metadata").asInstanceOf[GenericRecord]
    rec4.get("orderId").toString should be("ORD-004")
    rec4.get("amount") should be(500.00)
    metaRec4.get("topic").toString should be(TopicName)
    metaRec4.get("partition") should be(1)
    metaRec4.get("offset") should be(4L)
    metaRec4.get("timestamp") should be(20004L)
  }

}
