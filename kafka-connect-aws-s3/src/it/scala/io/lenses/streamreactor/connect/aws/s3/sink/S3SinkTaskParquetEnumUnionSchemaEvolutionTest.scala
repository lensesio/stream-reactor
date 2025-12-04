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
  * Integration test for Parquet format with schema evolution from enum to union of [enum, string].
  *
  * This test verifies that the AttachLatestSchemaOptimizer correctly handles the transition
  * from a simple enum field to a union field containing both the enum and a string type,
  * which is a common backward-compatible schema evolution pattern in Avro.
  *
  * In Kafka Connect's representation:
  * - Avro enum -> STRING schema with name and enum parameters
  * - Avro union [enum, string] -> STRUCT with name "io.confluent.connect.avro.Union"
  */
class S3SinkTaskParquetEnumUnionSchemaEvolutionTest
    extends AnyFlatSpec
    with Matchers
    with S3ProxyContainerTest
    with MockitoSugar
    with LazyLogging {

  private val parquetFormatReader = new ParquetFormatReader()

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "ordersTopic"

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

  "S3SinkTask" should "write to parquet format handling enum to union schema evolution" in {
    val props = (
      defaultProps ++
        Map(
          "connect.s3.kcql" -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS PARQUET PROPERTIES('${FlushCount.entryName}'=4)",
          "connect.s3.latest.schema.optimization.enabled" -> "true",
        )
    ).asJava

    val task = new S3SinkTask()
    val ctx  = mock[SinkTaskContext]
    task.initialize(ctx)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Schema V1: status is an enum (represented as STRING in Kafka Connect)
    val enumSchema = SchemaBuilder
      .string()
      .name("com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum", "com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum.PENDING", "PENDING")
      .parameter("io.confluent.connect.avro.Enum.PROCESSING", "PROCESSING")
      .parameter("io.confluent.connect.avro.Enum.SHIPPED", "SHIPPED")
      .parameter("io.confluent.connect.avro.Enum.DELIVERED", "DELIVERED")
      .parameter("io.confluent.connect.avro.Enum.CANCELLED", "CANCELLED")
      .build()

    val orderSchemaV1 = SchemaBuilder
      .struct()
      .name("com.example.Order")
      .version(1)
      .field("orderId", Schema.STRING_SCHEMA)
      .field("customerName", Schema.STRING_SCHEMA)
      .field("status", enumSchema)
      .build()

    // Schema V2: status is a union [enum, string] (represented as STRUCT in Kafka Connect)
    val unionEnumBranchSchema = SchemaBuilder
      .string()
      .name("com.example.OrderStatus")
      .optional()
      .parameter("io.confluent.connect.avro.Enum", "com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum.PENDING", "PENDING")
      .parameter("io.confluent.connect.avro.Enum.PROCESSING", "PROCESSING")
      .parameter("io.confluent.connect.avro.Enum.SHIPPED", "SHIPPED")
      .parameter("io.confluent.connect.avro.Enum.DELIVERED", "DELIVERED")
      .parameter("io.confluent.connect.avro.Enum.CANCELLED", "CANCELLED")
      .build()

    val unionSchema = SchemaBuilder
      .struct()
      .name("io.confluent.connect.avro.Union")
      .field("com.example.OrderStatus", unionEnumBranchSchema)
      .field("string", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val orderSchemaV2 = SchemaBuilder
      .struct()
      .name("com.example.Order")
      .version(2)
      .field("orderId", Schema.STRING_SCHEMA)
      .field("customerName", Schema.STRING_SCHEMA)
      .field("status", unionSchema)
      .build()

    // Record 1: Schema V1 with enum status
    val order1 = new Struct(orderSchemaV1)
    order1.put("orderId", "ORD-001")
    order1.put("customerName", "Alice")
    order1.put("status", "PENDING")

    // Record 2: Schema V1 with enum status
    val order2 = new Struct(orderSchemaV1)
    order2.put("orderId", "ORD-002")
    order2.put("customerName", "Bob")
    order2.put("status", "PROCESSING")

    // Record 3: Schema V2 with union status (using enum branch)
    val statusUnion3 = new Struct(unionSchema)
    statusUnion3.put("com.example.OrderStatus", "SHIPPED")
    statusUnion3.put("string", null)

    val order3 = new Struct(orderSchemaV2)
    order3.put("orderId", "ORD-003")
    order3.put("customerName", "Charlie")
    order3.put("status", statusUnion3)

    // Record 4: Schema V2 with union status (using string branch - custom status)
    val statusUnion4 = new Struct(unionSchema)
    statusUnion4.put("com.example.OrderStatus", null)
    statusUnion4.put("string", "CUSTOM_HOLD")

    val order4 = new Struct(orderSchemaV2)
    order4.put("orderId", "ORD-004")
    order4.put("customerName", "Diana")
    order4.put("status", statusUnion4)

    val record1 = toSinkRecord(order1, TopicName, 1, 1L, 10001L)
    val record2 = toSinkRecord(order2, TopicName, 1, 2L, 10002L)
    val record3 = toSinkRecord(order3, TopicName, 1, 3L, 10003L)
    val record4 = toSinkRecord(order4, TopicName, 1, 4L, 10004L)

    task.put(List(record1, record2, record3, record4).asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    // Verify output
    val files = listBucketPath(BucketName, "streamReactorBackups/ordersTopic/1/")
    files.size should be(1)
    val bytes = remoteFileAsBytes(BucketName, files.head)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(4)

    // All records should have been adapted to Schema V2 (union schema)
    // When read back from Parquet, Avro unions are stored as the actual value type,
    // not wrapped in a struct. The value will be either an EnumSymbol or a String.

    // Record 1: Enum "PENDING" promoted to union
    val rec1 = genericRecords.head
    rec1.get("orderId").toString should be("ORD-001")
    rec1.get("customerName").toString should be("Alice")
    rec1.get("status").toString should be("PENDING")

    // Record 2: Enum "PROCESSING" promoted to union
    val rec2 = genericRecords(1)
    rec2.get("orderId").toString should be("ORD-002")
    rec2.get("customerName").toString should be("Bob")
    rec2.get("status").toString should be("PROCESSING")

    // Record 3: Already union with enum branch
    val rec3 = genericRecords(2)
    rec3.get("orderId").toString should be("ORD-003")
    rec3.get("customerName").toString should be("Charlie")
    rec3.get("status").toString should be("SHIPPED")

    // Record 4: Union with string branch
    val rec4 = genericRecords(3)
    rec4.get("orderId").toString should be("ORD-004")
    rec4.get("customerName").toString should be("Diana")
    rec4.get("status").toString should be("CUSTOM_HOLD")
  }
}

