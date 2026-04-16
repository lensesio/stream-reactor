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
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * Integration test that reproduces the VersionSchemaChangeDetector bug.
 *
 * This test sends records with schema v3 first, then v2.
 * When processed with schema.change.detector=version (without the fix),
 * it would fail because:
 *   - v3 written first, file initialized with v3 schema
 *   - v2 arrives, detector checks: 2 > 3 = False (no schema change detected)
 *   - v2 record written to v3 file → ArrayIndexOutOfBoundsException
 *
 * With latest.schema.optimization.enabled=true, records should be adapted
 * to the latest schema seen (v3) and written correctly.
 *
 * Based on the Python test producer script that reproduces the bug with:
 *   - DslSDPEvent schema v2 (without businessUnit field)
 *   - DslSDPEvent schema v3 (with businessUnit field)
 */
class S3SinkTaskParquetV3ThenV2SchemaOptimizationTest
    extends AnyFlatSpec
    with Matchers
    with S3ProxyContainerTest
    with MockitoSugar
    with LazyLogging {

  private val parquetFormatReader = new ParquetFormatReader()

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "dsl-events"

  private def toSinkRecord(
    value:     Struct,
    topic:     String,
    partition: Int,
    offset:    Long,
    timestamp: Long,
  ): SinkRecord =
    new SinkRecord(
      topic,
      partition,
      value.schema(),
      value,
      value.schema(),
      value,
      offset,
      timestamp,
      TimestampType.CREATE_TIME,
    )

  // EventMetadata nested schema
  private val eventMetadataSchema = SchemaBuilder.struct()
    .name("com.walmart.ssat.events.EventMetadata")
    .field("id", Schema.STRING_SCHEMA)
    .field("source", Schema.STRING_SCHEMA)
    .field("type", Schema.STRING_SCHEMA)
    .field("specversion", SchemaBuilder.float32().defaultValue(1.0f).build())
    .field("time", Schema.STRING_SCHEMA)
    .build()

  // Store nested schema
  private val storeSchema = SchemaBuilder.struct()
    .name("com.walmart.ssat.events.Store")
    .field("countryCode", Schema.STRING_SCHEMA)
    .field("storeNumber", Schema.INT32_SCHEMA)
    .field("gln", Schema.OPTIONAL_STRING_SCHEMA)
    .build()

  // Schema v2 - WITHOUT businessUnit field
  private val dslEventSchemaV2 = SchemaBuilder.struct()
    .name("com.walmart.ssat.events.DslSDPEvent")
    .field("metadata", eventMetadataSchema)
    .field("store", storeSchema)
    .field("action", Schema.OPTIONAL_STRING_SCHEMA)
    .field("labelId", Schema.STRING_SCHEMA)
    .field("wmCorrelationId", Schema.OPTIONAL_STRING_SCHEMA)
    .field("wmClientId", Schema.OPTIONAL_STRING_SCHEMA)
    .field("testMode", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("eventTimeStamp", Schema.OPTIONAL_STRING_SCHEMA)
    .field("state", Schema.OPTIONAL_STRING_SCHEMA)
    .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
    .field("errorCode", Schema.OPTIONAL_STRING_SCHEMA)
    .field("errorDescription", Schema.OPTIONAL_STRING_SCHEMA)
    .field("deviceHealth", Schema.OPTIONAL_STRING_SCHEMA)
    .field("clientTransactionIdentifier", Schema.OPTIONAL_STRING_SCHEMA)
    .version(2)
    .build()

  // Schema v3 - WITH businessUnit field (added at the end)
  private val dslEventSchemaV3 = SchemaBuilder.struct()
    .name("com.walmart.ssat.events.DslSDPEvent")
    .field("metadata", eventMetadataSchema)
    .field("store", storeSchema)
    .field("action", Schema.OPTIONAL_STRING_SCHEMA)
    .field("labelId", Schema.STRING_SCHEMA)
    .field("wmCorrelationId", Schema.OPTIONAL_STRING_SCHEMA)
    .field("wmClientId", Schema.OPTIONAL_STRING_SCHEMA)
    .field("testMode", Schema.OPTIONAL_BOOLEAN_SCHEMA)
    .field("eventTimeStamp", Schema.OPTIONAL_STRING_SCHEMA)
    .field("state", Schema.OPTIONAL_STRING_SCHEMA)
    .field("payload", Schema.OPTIONAL_STRING_SCHEMA)
    .field("errorCode", Schema.OPTIONAL_STRING_SCHEMA)
    .field("errorDescription", Schema.OPTIONAL_STRING_SCHEMA)
    .field("deviceHealth", Schema.OPTIONAL_STRING_SCHEMA)
    .field("clientTransactionIdentifier", Schema.OPTIONAL_STRING_SCHEMA)
    .field("businessUnit", Schema.OPTIONAL_STRING_SCHEMA) // NEW field in v3
    .version(3)
    .build()

  private def createEventV2(labelId: String, storeNumber: Int): Struct = {
    val now = java.time.Instant.now().toString

    val metadata = new Struct(eventMetadataSchema)
      .put("id", UUID.randomUUID().toString)
      .put("source", "com.test.producer")
      .put("type", "dsl.test.event")
      .put("specversion", 1.0f)
      .put("time", now)

    val store = new Struct(storeSchema)
      .put("countryCode", "US")
      .put("storeNumber", storeNumber)
      .put("gln", null)

    new Struct(dslEventSchemaV2)
      .put("metadata", metadata)
      .put("store", store)
      .put("action", "update")
      .put("labelId", labelId)
      .put("wmCorrelationId", UUID.randomUUID().toString)
      .put("wmClientId", "TestProducer")
      .put("testMode", true)
      .put("eventTimeStamp", now)
      .put("state", "PENDING")
      .put("payload", s"""{"test": true, "schemaVersion": 2}""")
      .put("errorCode", null)
      .put("errorDescription", null)
      .put("deviceHealth", "OK")
      .put("clientTransactionIdentifier", UUID.randomUUID().toString)
  }

  private def createEventV3(labelId: String, storeNumber: Int, businessUnit: String): Struct = {
    val now = java.time.Instant.now().toString

    val metadata = new Struct(eventMetadataSchema)
      .put("id", UUID.randomUUID().toString)
      .put("source", "com.test.producer")
      .put("type", "dsl.test.event")
      .put("specversion", 1.0f)
      .put("time", now)

    val store = new Struct(storeSchema)
      .put("countryCode", "US")
      .put("storeNumber", storeNumber)
      .put("gln", null)

    new Struct(dslEventSchemaV3)
      .put("metadata", metadata)
      .put("store", store)
      .put("action", "update")
      .put("labelId", labelId)
      .put("wmCorrelationId", UUID.randomUUID().toString)
      .put("wmClientId", "TestProducer")
      .put("testMode", true)
      .put("eventTimeStamp", now)
      .put("state", "PENDING")
      .put("payload", s"""{"test": true, "schemaVersion": 3}""")
      .put("errorCode", null)
      .put("errorDescription", null)
      .put("deviceHealth", "OK")
      .put("clientTransactionIdentifier", UUID.randomUUID().toString)
      .put("businessUnit", businessUnit)
  }

  /**
   * Test that reproduces the exact bug scenario:
   * - Send 5 v3 records first (with businessUnit field)
   * - Then send 5 v2 records (without businessUnit field)
   *
   * Without the fix, this would cause ArrayIndexOutOfBoundsException because:
   * - VersionSchemaChangeDetector only checks newVersion > oldVersion
   * - 2 > 3 is False, so no schema change is detected
   * - v2 record gets written to v3-initialized Parquet file
   *
   * With latest.schema.optimization.enabled=true, all records should be
   * adapted to v3 schema and written correctly.
   */
  "S3SinkTask" should "handle v3 then v2 schema sequence with parquet and schema optimization" in {
    val props = (
      defaultProps ++
        Map(
          "connect.s3.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS PARQUET PROPERTIES('padding.length.partition'='12','padding.length.offset'='12','${FlushCount.entryName}'=10)",
          "connect.s3.latest.schema.optimization.enabled" -> "true",
        )
    ).asJava
    val expectedFile = "streamReactorBackups/dsl-events/000000000001/000000000010_10001_10010.parquet"

    val task = new S3SinkTask()
    val ctx  = mock[SinkTaskContext]
    task.initialize(ctx)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Phase 1: Send 5 v3 records first (with businessUnit field)
    val v3Records = (0 until 5).map { i =>
      val labelId = f"LABEL-V3-$i%04d"
      val event   = createEventV3(labelId, storeNumber = 1000 + i, businessUnit = "WM")
      toSinkRecord(event, TopicName, 1, (i + 1).toLong, 10001L + i)
    }

    // Phase 2: Send 5 v2 records (without businessUnit field)
    // These should trigger ArrayIndexOutOfBoundsException without the fix
    val v2Records = (0 until 5).map { i =>
      val labelId = f"LABEL-V2-$i%04d"
      val event   = createEventV2(labelId, storeNumber = 2000 + i)
      toSinkRecord(event, TopicName, 1, (i + 6).toLong, 10006L + i)
    }

    // Send all records: v3 first, then v2
    val allRecords = v3Records ++ v2Records
    task.put(allRecords.toList.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    // Verify output - all 10 records should be in a single file with v3 schema
    val files = listBucketPath(BucketName, "streamReactorBackups/dsl-events/000000000001/")
    files.size should be(1)
    val bytes = remoteFileAsBytes(BucketName, expectedFile)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(10)

    // Verify v3 records (first 5) - should have businessUnit set
    (0 until 5).foreach { i =>
      val rec = genericRecords(i)
      rec.get("labelId").toString should be(f"LABEL-V3-$i%04d")

      val store = rec.get("store").asInstanceOf[GenericRecord]
      store.get("storeNumber") should be(1000 + i)

      // v3 has businessUnit field set
      rec.get("businessUnit").toString should be("WM")
    }

    // Verify v2 records (last 5) - should have businessUnit as null (adapted to v3 schema)
    (0 until 5).foreach { i =>
      val rec = genericRecords(i + 5)
      rec.get("labelId").toString should be(f"LABEL-V2-$i%04d")

      val store = rec.get("store").asInstanceOf[GenericRecord]
      store.get("storeNumber") should be(2000 + i)

      // v2 adapted to v3 should have businessUnit as null
      rec.get("businessUnit") should be(null)
    }
  }

  /**
   * Test with interleaved v3 and v2 records to ensure consistent handling.
   * Pattern: v3, v2, v3, v2, v3, v2, v3, v2
   */
  "S3SinkTask" should "handle interleaved v3 and v2 records with parquet and schema optimization" in {
    val props = (
      defaultProps ++
        Map(
          "connect.s3.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS PARQUET PROPERTIES('padding.length.partition'='12','padding.length.offset'='12','${FlushCount.entryName}'=8)",
          "connect.s3.latest.schema.optimization.enabled" -> "true",
        )
    ).asJava
    val expectedFile = "streamReactorBackups/dsl-events/000000000001/000000000008_20001_20008.parquet"

    val task = new S3SinkTask()
    val ctx  = mock[SinkTaskContext]
    task.initialize(ctx)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Interleave v3 and v2 records
    val records = (0 until 8).map { i =>
      if (i % 2 == 0) {
        // Even indices: v3 records
        val labelId = f"LABEL-V3-$i%04d"
        val event   = createEventV3(labelId, storeNumber = 3000 + i, businessUnit = "SM")
        toSinkRecord(event, TopicName, 1, (i + 1).toLong, 20001L + i)
      } else {
        // Odd indices: v2 records
        val labelId = f"LABEL-V2-$i%04d"
        val event   = createEventV2(labelId, storeNumber = 4000 + i)
        toSinkRecord(event, TopicName, 1, (i + 1).toLong, 20001L + i)
      }
    }

    task.put(records.toList.asJava)
    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val files = listBucketPath(BucketName, "streamReactorBackups/dsl-events/000000000001/")
    files.size should be(1)
    val bytes = remoteFileAsBytes(BucketName, expectedFile)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(8)

    // Verify all records
    (0 until 8).foreach { i =>
      val rec   = genericRecords(i)
      val store = rec.get("store").asInstanceOf[GenericRecord]

      if (i % 2 == 0) {
        // v3 records
        rec.get("labelId").toString should be(f"LABEL-V3-$i%04d")
        store.get("storeNumber") should be(3000 + i)
        rec.get("businessUnit").toString should be("SM")
      } else {
        // v2 records adapted to v3
        rec.get("labelId").toString should be(f"LABEL-V2-$i%04d")
        store.get("storeNumber") should be(4000 + i)
        rec.get("businessUnit") should be(null)
      }
    }
  }

  /**
   * Test multiple batches: first batch v3, second batch v2, simulating
   * the real-world scenario where consumers process in batches.
   */
  "S3SinkTask" should "handle multiple batches with v3 first then v2 batch" in {
    val props = (
      defaultProps ++
        Map(
          "connect.s3.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS PARQUET PROPERTIES('padding.length.partition'='12','padding.length.offset'='12','${FlushCount.entryName}'=6)",
          "connect.s3.latest.schema.optimization.enabled" -> "true",
        )
    ).asJava
    val expectedFile = "streamReactorBackups/dsl-events/000000000001/000000000006_30001_30006.parquet"

    val task = new S3SinkTask()
    val ctx  = mock[SinkTaskContext]
    task.initialize(ctx)
    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Batch 1: v3 records
    val batch1 = (0 until 3).map { i =>
      val labelId = f"BATCH1-V3-$i%04d"
      val event   = createEventV3(labelId, storeNumber = 5000 + i, businessUnit = "WM")
      toSinkRecord(event, TopicName, 1, (i + 1).toLong, 30001L + i)
    }

    // Send first batch
    task.put(batch1.toList.asJava)

    // Batch 2: v2 records (simulating later arrival of older schema records)
    val batch2 = (0 until 3).map { i =>
      val labelId = f"BATCH2-V2-$i%04d"
      val event   = createEventV2(labelId, storeNumber = 6000 + i)
      toSinkRecord(event, TopicName, 1, (i + 4).toLong, 30004L + i)
    }

    // Send second batch
    task.put(batch2.toList.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    val files = listBucketPath(BucketName, "streamReactorBackups/dsl-events/000000000001/")
    files.size should be(1)
    val bytes = remoteFileAsBytes(BucketName, expectedFile)

    val genericRecords = parquetFormatReader.read(bytes)
    genericRecords.size should be(6)

    // Verify batch 1 (v3 records)
    (0 until 3).foreach { i =>
      val rec = genericRecords(i)
      rec.get("labelId").toString should be(f"BATCH1-V3-$i%04d")
      rec.get("businessUnit").toString should be("WM")
    }

    // Verify batch 2 (v2 records adapted to v3)
    (0 until 3).foreach { i =>
      val rec = genericRecords(i + 3)
      rec.get("labelId").toString should be(f"BATCH2-V2-$i%04d")
      rec.get("businessUnit") should be(null)
    }
  }

}
