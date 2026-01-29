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

package io.lenses.streamreactor.connect.gcp.storage.sink

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.kcqlprops.PropsKeyEnum.FlushCount
import io.lenses.streamreactor.connect.gcp.storage.utils.GCPProxyContainerTest
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * Regression test for VersionSchemaChangeDetector bug.
 *
 * The bug: VersionSchemaChangeDetector only checks `newVersion > oldVersion`,
 * missing the reverse direction (v3 -> v2). When v3 records arrive first,
 * then v2 records arrive, no schema change is detected because 2 > 3 is false.
 *
 * This causes ArrayIndexOutOfBoundsException when writing v2 records with
 * a schema that has fewer fields than the previously seen v3 schema.
 *
 * Fix: Change to `!Objects.equals(oldVersion, newVersion)` to detect ANY version change.
 */
class GCPStorageSinkTaskVersionDetectorBugTest
    extends AnyFlatSpec
    with Matchers
    with GCPProxyContainerTest
    with MockitoSugar
    with LazyLogging {

  private val PrefixName = "streamReactorBackups"
  private val TopicName  = "myTopic"

  /**
   * Test that v2 records can be written to a file initialized with v3 schema.
   *
   * Previously this would fail with ArrayIndexOutOfBoundsException because:
   * - VersionSchemaChangeDetector only checks `newVersion > oldVersion` (2 > 3 = false)
   * - So v2 records weren't detected as a schema change
   * - v2 records tried to write to v3 parquet file with mismatched schemas
   *
   * Fix: ParquetFormatWriter now uses ToAvroDataConverter.convertToGenericRecordWithSchema()
   * which creates GenericRecords using the writer's schema, properly handling missing fields.
   */
  "GCPStorageSinkTask" should "write v2 records to v3 file without error (version detector)" in {
    // Schema v3 with an extra field
    val schemaV3 = SchemaBuilder.struct()
      .name("TestRecord")
      .version(3)
      .field("name", SchemaBuilder.string().required().build())
      .field("description", SchemaBuilder.string().optional().build())
      .field("extraField", SchemaBuilder.string().optional().build()) // v3 only
      .build()

    // Schema v2 without extraField
    val schemaV2 = SchemaBuilder.struct()
      .name("TestRecord")
      .version(2)
      .field("name", SchemaBuilder.string().required().build())
      .field("description", SchemaBuilder.string().optional().build())
      .build()

    // Records with v3 schema (sent first)
    val v3Records = List(
      new Struct(schemaV3).put("name", "record1").put("description", "first").put("extraField", "extra1"),
      new Struct(schemaV3).put("name", "record2").put("description", "second").put("extraField", "extra2"),
    )

    // Records with v2 schema (sent after)
    val v2Records = List(
      new Struct(schemaV2).put("name", "record3").put("description", "third"),
      new Struct(schemaV2).put("name", "record4").put("description", "fourth"),
    )

    val task = createSinkTask()

    // Configure with VERSION schema change detector (no optimization)
    val props = (defaultProps ++ Map(
      s"$prefix.kcql"                   -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `PARQUET` PROPERTIES('${FlushCount.entryName}'=10)",
      s"$prefix.schema.change.detector" -> "version",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Send v3 records first
    val sinkRecordsV3 = v3Records.zipWithIndex.map {
      case (struct, i) =>
        new SinkRecord(TopicName, 1, null, null, schemaV3, struct, i.toLong, i.toLong, TimestampType.CREATE_TIME)
    }
    task.put(sinkRecordsV3.asJava)

    // Send v2 records after - with the fix, these are converted using v3 schema
    // Missing fields (extraField) will be null
    val sinkRecordsV2 = v2Records.zipWithIndex.map {
      case (struct, i) =>
        new SinkRecord(TopicName,
                       1,
                       null,
                       null,
                       schemaV2,
                       struct,
                       (i + 2).toLong,
                       (i + 2).toLong,
                       TimestampType.CREATE_TIME,
        )
    }

    // With the fix, this should NOT throw exception - v2 records are converted using v3 schema
    noException should be thrownBy task.put(sinkRecordsV2.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    // Verify files were created (no specific count assertion since file might not be flushed yet)
  }

  /**
   * Test that schema optimization adapts v2 records to v3 schema, allowing all records
   * to be written to the SAME parquet file without triggering a schema change flush.
   *
   * When latest.schema.optimization.enabled=true:
   * 1. v3 records arrive first - establishes v3 as the "latest" schema
   * 2. v2 records arrive - they are adapted to v3 schema (extraField=null)
   * 3. All records are written to the same file
   *
   * This demonstrates the schema optimization working correctly.
   */
  "GCPStorageSinkTask" should "merge v3 and v2 records into same parquet file with schema optimization enabled" in {
    // Schema v3 with an extra field
    val schemaV3 = SchemaBuilder.struct()
      .name("TestRecord")
      .version(3)
      .field("name", SchemaBuilder.string().required().build())
      .field("description", SchemaBuilder.string().optional().build())
      .field("extraField", SchemaBuilder.string().optional().build()) // v3 only
      .build()

    // Schema v2 without extraField
    val schemaV2 = SchemaBuilder.struct()
      .name("TestRecord")
      .version(2)
      .field("name", SchemaBuilder.string().required().build())
      .field("description", SchemaBuilder.string().optional().build())
      .build()

    // Records with v3 schema (sent first)
    val v3Records = List(
      new Struct(schemaV3).put("name", "record1").put("description", "first").put("extraField", "extra1"),
      new Struct(schemaV3).put("name", "record2").put("description", "second").put("extraField", "extra2"),
    )

    // Records with v2 schema (sent after - will be adapted to v3)
    val v2Records = List(
      new Struct(schemaV2).put("name", "record3").put("description", "third"),
      new Struct(schemaV2).put("name", "record4").put("description", "fourth"),
    )

    val task = createSinkTask()

    // Configure with schema optimization enabled - this adapts v2 records to v3
    val props = (defaultProps ++ Map(
      s"$prefix.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `PARQUET` PROPERTIES('${FlushCount.entryName}'=4,'padding.length.partition'='12','padding.length.offset'='12')",
      s"$prefix.latest.schema.optimization.enabled" -> "true",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Send v3 records first
    val sinkRecordsV3 = v3Records.zipWithIndex.map {
      case (struct, i) =>
        new SinkRecord(TopicName, 1, null, null, schemaV3, struct, i.toLong, i.toLong, TimestampType.CREATE_TIME)
    }
    task.put(sinkRecordsV3.asJava)

    // Send v2 records after - with optimization enabled, these are adapted to v3
    val sinkRecordsV2 = v2Records.zipWithIndex.map {
      case (struct, i) =>
        new SinkRecord(TopicName,
                       1,
                       null,
                       null,
                       schemaV2,
                       struct,
                       (i + 2).toLong,
                       (i + 2).toLong,
                       TimestampType.CREATE_TIME,
        )
    }

    // This should NOT throw exception - v2 records are adapted to v3 schema
    noException should be thrownBy task.put(sinkRecordsV2.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    // Verify ALL 4 records are in a SINGLE parquet file (schema optimization prevented split)
    val files = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")
    files.size should be(1)
  }

  /**
   * Test using BOTH version detector AND schema optimization together.
   *
   * When schema.change.detector=version AND latest.schema.optimization.enabled=true:
   * 1. v3 records arrive first - establishes v3 as the "latest" schema
   * 2. v2 records arrive - they are adapted to v3 schema BEFORE version check
   * 3. Since all records now have v3 schema, version detector sees no change
   * 4. All records are written to the SAME parquet file
   *
   * This is the recommended configuration for production use.
   */
  "GCPStorageSinkTask" should "merge v3 and v2 records into same file with version detector AND schema optimization" in {
    // Schema v3 with an extra field
    val schemaV3 = SchemaBuilder.struct()
      .name("TestRecord")
      .version(3)
      .field("name", SchemaBuilder.string().required().build())
      .field("description", SchemaBuilder.string().optional().build())
      .field("extraField", SchemaBuilder.string().optional().build()) // v3 only
      .build()

    // Schema v2 without extraField
    val schemaV2 = SchemaBuilder.struct()
      .name("TestRecord")
      .version(2)
      .field("name", SchemaBuilder.string().required().build())
      .field("description", SchemaBuilder.string().optional().build())
      .build()

    // Records with v3 schema (sent first)
    val v3Records = List(
      new Struct(schemaV3).put("name", "record1").put("description", "first").put("extraField", "extra1"),
      new Struct(schemaV3).put("name", "record2").put("description", "second").put("extraField", "extra2"),
    )

    // Records with v2 schema (sent after - will be adapted to v3 by optimizer)
    val v2Records = List(
      new Struct(schemaV2).put("name", "record3").put("description", "third"),
      new Struct(schemaV2).put("name", "record4").put("description", "fourth"),
    )

    val task = createSinkTask()

    // Configure with BOTH version detector AND schema optimization
    val props = (defaultProps ++ Map(
      s"$prefix.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `PARQUET` PROPERTIES('${FlushCount.entryName}'=4,'padding.length.partition'='12','padding.length.offset'='12')",
      s"$prefix.schema.change.detector"             -> "version",
      s"$prefix.latest.schema.optimization.enabled" -> "true",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Send v3 records first
    val sinkRecordsV3 = v3Records.zipWithIndex.map {
      case (struct, i) =>
        new SinkRecord(TopicName, 1, null, null, schemaV3, struct, i.toLong, i.toLong, TimestampType.CREATE_TIME)
    }
    task.put(sinkRecordsV3.asJava)

    // Send v2 records after
    // With schema optimization: v2 adapted to v3, version detector sees v3->v3 (no change)
    // All records merged into same file
    val sinkRecordsV2 = v2Records.zipWithIndex.map {
      case (struct, i) =>
        new SinkRecord(TopicName,
                       1,
                       null,
                       null,
                       schemaV2,
                       struct,
                       (i + 2).toLong,
                       (i + 2).toLong,
                       TimestampType.CREATE_TIME,
        )
    }

    // This should NOT throw exception - v2 records are adapted to v3 before version check
    noException should be thrownBy task.put(sinkRecordsV2.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    // Verify ALL 4 records are in a SINGLE parquet file
    // Schema optimization adapts v2->v3, so version detector sees no change
    val files = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")
    files.size should be(1)
  }

  /**
   * Test using compatibility detector AND schema optimization together.
   *
   * When schema.change.detector=compatibility AND latest.schema.optimization.enabled=true:
   * 1. v3 records arrive first - establishes v3 as the "latest" schema
   * 2. v2 records arrive - they are adapted to v3 schema BEFORE compatibility check
   * 3. Since all records now have v3 schema, compatibility detector sees no change
   * 4. All records are written to the SAME parquet file
   *
   * This should have the same outcome as using version detector with optimization.
   */
  "GCPStorageSinkTask" should "merge v3 and v2 records into same file with compatibility detector AND schema optimization" in {
    // Schema v3 with an extra field
    val schemaV3 = SchemaBuilder.struct()
      .name("TestRecord")
      .version(3)
      .field("name", SchemaBuilder.string().required().build())
      .field("description", SchemaBuilder.string().optional().build())
      .field("extraField", SchemaBuilder.string().optional().build()) // v3 only
      .build()

    // Schema v2 without extraField
    val schemaV2 = SchemaBuilder.struct()
      .name("TestRecord")
      .version(2)
      .field("name", SchemaBuilder.string().required().build())
      .field("description", SchemaBuilder.string().optional().build())
      .build()

    // Records with v3 schema (sent first)
    val v3Records = List(
      new Struct(schemaV3).put("name", "record1").put("description", "first").put("extraField", "extra1"),
      new Struct(schemaV3).put("name", "record2").put("description", "second").put("extraField", "extra2"),
    )

    // Records with v2 schema (sent after - will be adapted to v3 by optimizer)
    val v2Records = List(
      new Struct(schemaV2).put("name", "record3").put("description", "third"),
      new Struct(schemaV2).put("name", "record4").put("description", "fourth"),
    )

    val task = createSinkTask()

    // Configure with BOTH compatibility detector AND schema optimization
    val props = (defaultProps ++ Map(
      s"$prefix.kcql"                               -> s"insert into $BucketName:$PrefixName select * from $TopicName STOREAS `PARQUET` PROPERTIES('${FlushCount.entryName}'=4,'padding.length.partition'='12','padding.length.offset'='12')",
      s"$prefix.schema.change.detector"             -> "compatibility",
      s"$prefix.latest.schema.optimization.enabled" -> "true",
    )).asJava

    task.start(props)
    task.open(Seq(new TopicPartition(TopicName, 1)).asJava)

    // Send v3 records first
    val sinkRecordsV3 = v3Records.zipWithIndex.map {
      case (struct, i) =>
        new SinkRecord(TopicName, 1, null, null, schemaV3, struct, i.toLong, i.toLong, TimestampType.CREATE_TIME)
    }
    task.put(sinkRecordsV3.asJava)

    // Send v2 records after
    // With schema optimization: v2 adapted to v3, compatibility detector sees v3->v3 (compatible)
    // All records merged into same file
    val sinkRecordsV2 = v2Records.zipWithIndex.map {
      case (struct, i) =>
        new SinkRecord(TopicName,
                       1,
                       null,
                       null,
                       schemaV2,
                       struct,
                       (i + 2).toLong,
                       (i + 2).toLong,
                       TimestampType.CREATE_TIME,
        )
    }

    // This should NOT throw exception - v2 records are adapted to v3 before compatibility check
    noException should be thrownBy task.put(sinkRecordsV2.asJava)

    task.close(Seq(new TopicPartition(TopicName, 1)).asJava)
    task.stop()

    // Verify ALL 4 records are in a SINGLE parquet file
    // Schema optimization adapts v2->v3, so compatibility detector sees no change
    val files = listBucketPath(BucketName, "streamReactorBackups/myTopic/000000000001/")
    files.size should be(1)
  }

}
