/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats.writer

import io.lenses.streamreactor.connect.avro.AvroDataFactory
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodec
import io.lenses.streamreactor.connect.cloud.common.model.CompressionCodecName.UNCOMPRESSED
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.optimization.AttachLatestSchemaOptimizer
import io.lenses.streamreactor.connect.cloud.common.stream.BuildLocalOutputStream
import org.apache.avro.Schema.Parser
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream
import scala.jdk.CollectionConverters._

/**
 * Tests for ParquetFormatWriter with schema evolution scenarios.
 *
 * This test reproduces the ArrayIndexOutOfBoundsException that occurs when:
 * 1. The latest.schema.optimization.enabled feature is enabled
 * 2. Records with different schema versions are interleaved
 * 3. The AttachLatestSchemaOptimizer adapts older schema records to the latest schema
 * 4. The Parquet writer receives records with schemas that have the same logical structure
 *    but different Avro schema objects
 */
class ParquetFormatWriterSchemaEvolutionTest extends AnyFlatSpec with Matchers with EitherValues {

  implicit val compressionCodec: CompressionCodec = UNCOMPRESSED.toCodec()
  val topicPartition:            TopicPartition   = Topic("testTopic").withPartition(1)
  val topic:                     Topic            = Topic("testTopic")

  private def createSchema(name: String, version: Int, fields: (String, Schema)*): Schema = {
    val builder = SchemaBuilder.struct().name(name).version(version)
    fields.foreach {
      case (fieldName, fieldSchema) =>
        builder.field(fieldName, fieldSchema)
    }
    builder.build()
  }

  private def createStruct(schema: Schema, fields: (String, Any)*): Struct = {
    val struct = new Struct(schema)
    fields.foreach {
      case (fieldName, fieldValue) =>
        struct.put(fieldName, fieldValue)
    }
    struct
  }

  private def createTempFile(): File = {
    val file = File.createTempFile("parquet-test", ".parquet")
    file.deleteOnExit()
    file
  }

  private def toBufferedOutputStream(file: File): BufferedOutputStream =
    new BufferedOutputStream(new FileOutputStream(file))

  /**
   * This test reproduces the issue where writing records with adapted schemas
   * causes ArrayIndexOutOfBoundsException.
   *
   * Scenario:
   * 1. Create schema v2 with 3 fields (simulating the "latest" schema)
   * 2. Write a record with schema v2 (initializes the Parquet writer)
   * 3. Create a struct originally from v1 schema (2 fields) but adapted to v2 schema
   * 4. Write the adapted record - this triggers the bug because the Avro GenericRecord
   *    is created with a different schema object than the one used to initialize the writer
   */
  "ParquetFormatWriter" should "handle writing records adapted from older schema versions" in {
    val tempFile = createTempFile()

    // Schema v2: evolved schema with 3 fields (simulating the "latest" schema)
    // Note: Schema v1 would have only field1 and field2, but we test the adapted scenario
    val schemaV2 = createSchema(
      "com.example.TestRecord",
      2,
      "field1" -> Schema.STRING_SCHEMA,
      "field2" -> Schema.INT32_SCHEMA,
      "field3" -> Schema.OPTIONAL_STRING_SCHEMA,
    )

    // Create a struct with v2 schema (first record - initializes writer)
    val structV2 = createStruct(schemaV2, "field1" -> "value1", "field2" -> 42, "field3" -> "extra")

    // Simulate what AttachLatestSchemaOptimizer does:
    // Take data from v1 schema and adapt it to v2 schema
    // This creates a NEW Struct with v2 schema, containing values from v1 data
    val adaptedStruct = new Struct(schemaV2)
    adaptedStruct.put("field1", "value2")
    adaptedStruct.put("field2", 100)
    adaptedStruct.put("field3", null) // New field gets null value

    val blobStream          = new BuildLocalOutputStream(toBufferedOutputStream(tempFile), topicPartition)
    val parquetFormatWriter = new ParquetFormatWriter(blobStream)

    // Write first record with v2 schema - this initializes the Parquet writer
    val writeResult1 = parquetFormatWriter.write(MessageDetail(
      NullSinkData(None),
      StructSinkData(structV2),
      Map.empty,
      None,
      topic,
      1,
      Offset(1),
    ))
    writeResult1 shouldBe Right(())

    // Write second record - adapted from v1 to v2 schema
    // This is where the ArrayIndexOutOfBoundsException occurs in production
    val writeResult2 = parquetFormatWriter.write(MessageDetail(
      NullSinkData(None),
      StructSinkData(adaptedStruct),
      Map.empty,
      None,
      topic,
      1,
      Offset(2),
    ))
    writeResult2 shouldBe Right(())

    // Complete the writer
    parquetFormatWriter.complete() shouldBe Right(())
  }

  /**
   * Test using the actual AttachLatestSchemaOptimizer to adapt records.
   * This reproduces the exact scenario from production where:
   * 1. Batch 1: Record with v2 schema arrives - writer initialized with v2
   * 2. Batch 2: Record with v1 schema arrives - optimizer adapts it to v2
   *
   * The key difference is that the optimizer creates ENTIRELY NEW Schema and Struct objects,
   * which may produce different Avro schema objects when converted.
   */
  "ParquetFormatWriter" should "handle records adapted by AttachLatestSchemaOptimizer" in {
    val tempFile = createTempFile()

    val optimizer = new AttachLatestSchemaOptimizer()

    // Schema v1 with 2 fields
    val schemaV1 = createSchema(
      "TestRecord",
      1,
      "field1" -> Schema.STRING_SCHEMA,
      "field2" -> Schema.INT32_SCHEMA,
    )

    // Schema v2 with 3 fields (adds field3)
    val schemaV2 = createSchema(
      "TestRecord",
      2,
      "field1" -> Schema.STRING_SCHEMA,
      "field2" -> Schema.INT32_SCHEMA,
      "field3" -> Schema.OPTIONAL_STRING_SCHEMA,
    )

    // Create structs
    val structV1 = createStruct(schemaV1, "field1" -> "value1", "field2" -> 100)
    val structV2 = createStruct(schemaV2, "field1" -> "value2", "field2" -> 200, "field3" -> "extra")

    // Simulate Batch 1: v2 record arrives first
    val batch1Records = List(
      new SinkRecord("testTopic", 0, null, null, schemaV2, structV2, 0L, 0L, TimestampType.CREATE_TIME, null),
    )
    val optimizedBatch1 = optimizer.update(batch1Records)

    // Simulate Batch 2: v1 record arrives - optimizer adapts it to v2
    val batch2Records = List(
      new SinkRecord("testTopic", 0, null, null, schemaV1, structV1, 1L, 0L, TimestampType.CREATE_TIME, null),
    )
    val optimizedBatch2 = optimizer.update(batch2Records)

    // Verify the optimizer adapted the schema
    optimizedBatch2.head.valueSchema().version() shouldBe 2 // Should be adapted to v2

    val blobStream          = new BuildLocalOutputStream(toBufferedOutputStream(tempFile), topicPartition)
    val parquetFormatWriter = new ParquetFormatWriter(blobStream)

    // Write the first batch (v2)
    optimizedBatch1.foreach { record =>
      val struct = record.value().asInstanceOf[Struct]
      val writeResult = parquetFormatWriter.write(MessageDetail(
        NullSinkData(None),
        StructSinkData(struct),
        Map.empty,
        None,
        topic,
        1,
        Offset(record.kafkaOffset()),
      ))
      writeResult shouldBe Right(())
    }

    // Write the second batch (adapted from v1 to v2)
    // This is where the ArrayIndexOutOfBoundsException occurred in production
    optimizedBatch2.foreach { record =>
      val struct = record.value().asInstanceOf[Struct]
      val writeResult = parquetFormatWriter.write(MessageDetail(
        NullSinkData(None),
        StructSinkData(struct),
        Map.empty,
        None,
        topic,
        1,
        Offset(record.kafkaOffset()),
      ))
      writeResult shouldBe Right(())
    }

    parquetFormatWriter.complete() shouldBe Right(())
  }

  /**
   * PRODUCTION BUG REPRODUCTION TEST
   *
   * This test uses the EXACT Avro schemas from production, converted through Confluent's
   * AvroData converter to Connect schemas. This matches exactly how Schema Registry
   * deserializes records in the Kafka Connect pipeline.
   *
   * Production schemas:
   * - V2: 14 fields (includes clientTransactionIdentifier)
   * - V3: 15 fields (adds businessUnit)
   *
   * Bug scenario: V3 records first, then V2 records arrive.
   * With latest.schema.optimization.enabled=true, V2 is adapted to V3.
   * Without the fix, ArrayIndexOutOfBoundsException occurs.
   */
  "ParquetFormatWriter" should "handle production DslSDPEvent schemas V3 then V2 with AvroData conversion" in {
    val tempFile = createTempFile()

    // Production Avro schema V2 (14 fields - with clientTransactionIdentifier)
    val avroSchemaV2Json = """{
      "type": "record",
      "name": "DslSDPEvent",
      "namespace": "com.walmart.ssat.events",
      "doc": "Digital shelf label (DSL) action status",
      "fields": [
        {
          "name": "metadata",
          "type": {
            "type": "record",
            "name": "EventMetadata",
            "fields": [
              {"name": "id", "type": {"type": "string", "avro.java.string": "String"}, "doc": "Identifies the event; type 4 UUID."},
              {"name": "source", "type": {"type": "string", "avro.java.string": "String"}, "doc": "Identifies the source of the event."},
              {"name": "type", "type": {"type": "string", "avro.java.string": "String"}, "doc": "The type of event."},
              {"name": "specversion", "type": "float", "doc": "CloudEvents spec version.", "default": 1.0},
              {"name": "time", "type": {"type": "string", "avro.java.string": "String"}, "doc": "Timestamp of the event."}
            ]
          }
        },
        {
          "name": "store",
          "type": {
            "type": "record",
            "name": "Store",
            "fields": [
              {"name": "countryCode", "type": {"type": "string", "avro.java.string": "String"}, "doc": "Country code."},
              {"name": "storeNumber", "type": "int", "doc": "Store number."},
              {"name": "gln", "type": ["null", {"type": "string", "avro.java.string": "String"}], "doc": "GLN.", "default": null}
            ]
          }
        },
        {"name": "action", "type": ["null", {"type": "string", "avro.java.string": "String"}], "doc": "Action.", "default": null},
        {"name": "labelId", "type": {"type": "string", "avro.java.string": "String"}},
        {"name": "wmCorrelationId", "type": ["null", {"type": "string", "avro.java.string": "String"}], "doc": "Correlation ID.", "default": null},
        {"name": "wmClientId", "type": ["null", {"type": "string", "avro.java.string": "String"}], "doc": "Client ID."},
        {"name": "testMode", "type": ["null", "boolean"], "default": null},
        {"name": "eventTimeStamp", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "state", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "payload", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "errorCode", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "errorDescription", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "deviceHealth", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "clientTransactionIdentifier", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null}
      ]
    }"""

    // Production Avro schema V3 (15 fields - adds businessUnit)
    val avroSchemaV3Json = """{
      "type": "record",
      "name": "DslSDPEvent",
      "namespace": "com.walmart.ssat.events",
      "doc": "Digital shelf label (DSL) action status",
      "fields": [
        {
          "name": "metadata",
          "type": {
            "type": "record",
            "name": "EventMetadata",
            "fields": [
              {"name": "id", "type": {"type": "string", "avro.java.string": "String"}, "doc": "Identifies the event; type 4 UUID."},
              {"name": "source", "type": {"type": "string", "avro.java.string": "String"}, "doc": "Identifies the source of the event."},
              {"name": "type", "type": {"type": "string", "avro.java.string": "String"}, "doc": "The type of event."},
              {"name": "specversion", "type": "float", "doc": "CloudEvents spec version.", "default": 1},
              {"name": "time", "type": {"type": "string", "avro.java.string": "String"}, "doc": "Timestamp of the event."}
            ]
          }
        },
        {
          "name": "store",
          "type": {
            "type": "record",
            "name": "Store",
            "fields": [
              {"name": "countryCode", "type": {"type": "string", "avro.java.string": "String"}, "doc": "Country code."},
              {"name": "storeNumber", "type": "int", "doc": "Store number."},
              {"name": "gln", "type": ["null", {"type": "string", "avro.java.string": "String"}], "doc": "GLN.", "default": null}
            ]
          }
        },
        {"name": "action", "type": ["null", {"type": "string", "avro.java.string": "String"}], "doc": "Action.", "default": null},
        {"name": "labelId", "type": {"type": "string", "avro.java.string": "String"}},
        {"name": "wmCorrelationId", "type": ["null", {"type": "string", "avro.java.string": "String"}], "doc": "Correlation ID.", "default": null},
        {"name": "wmClientId", "type": ["null", {"type": "string", "avro.java.string": "String"}], "doc": "Client ID."},
        {"name": "testMode", "type": ["null", "boolean"], "default": null},
        {"name": "eventTimeStamp", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "state", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "payload", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "errorCode", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "errorDescription", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "deviceHealth", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "clientTransactionIdentifier", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "businessUnit", "type": ["null", {"type": "string", "avro.java.string": "String"}], "doc": "Business unit (WM/SM).", "default": null}
      ]
    }"""

    // Parse Avro schemas
    val parser       = new Parser()
    val avroSchemaV2 = parser.parse(avroSchemaV2Json)
    val avroSchemaV3 = new Parser().parse(avroSchemaV3Json) // Use new parser to avoid cache

    val avroDataV3 = AvroDataFactory.create()
    val avroDataV2 = AvroDataFactory.create() // Separate instance to simulate different deserialization

    // Convert Avro schemas to Connect schemas (this is what Schema Registry does)
    val connectSchemaV3 = avroDataV3.toConnectSchema(avroSchemaV3)
    val connectSchemaV2 = avroDataV2.toConnectSchema(avroSchemaV2)

    // Set versions on Connect schemas (Schema Registry does this)
    val connectSchemaV3WithVersion = new SchemaBuilder(connectSchemaV3.`type`())
      .name(connectSchemaV3.name())
      .doc(connectSchemaV3.doc())
      .version(3)
    connectSchemaV3.fields().asScala.foreach(f => connectSchemaV3WithVersion.field(f.name(), f.schema()))
    val schemaV3 = connectSchemaV3WithVersion.build()

    val connectSchemaV2WithVersion = new SchemaBuilder(connectSchemaV2.`type`())
      .name(connectSchemaV2.name())
      .doc(connectSchemaV2.doc())
      .version(2)
    connectSchemaV2.fields().asScala.foreach(f => connectSchemaV2WithVersion.field(f.name(), f.schema()))
    val schemaV2 = connectSchemaV2WithVersion.build()

    // Verify schemas have correct field counts
    schemaV3.fields().size() shouldBe 15
    schemaV2.fields().size() shouldBe 14

    // Helper to create V3 struct
    def createV3Struct(idx: Int): Struct = {
      val metadataSchema = schemaV3.field("metadata").schema()
      val storeSchema    = schemaV3.field("store").schema()

      val metadata = new Struct(metadataSchema)
        .put("id", s"uuid-$idx")
        .put("source", "com.test.producer")
        .put("type", "dsl.test.event")
        .put("specversion", 1.0f)
        .put("time", "2026-01-29T12:00:00Z")

      val store = new Struct(storeSchema)
        .put("countryCode", "US")
        .put("storeNumber", 1000 + idx)
        .put("gln", null)

      new Struct(schemaV3)
        .put("metadata", metadata)
        .put("store", store)
        .put("action", "update")
        .put("labelId", s"LABEL-V3-$idx")
        .put("wmCorrelationId", s"corr-$idx")
        .put("wmClientId", "TestClient")
        .put("testMode", true)
        .put("eventTimeStamp", "2026-01-29T12:00:00Z")
        .put("state", "PENDING")
        .put("payload", s"""{"test": true, "idx": $idx}""")
        .put("errorCode", null)
        .put("errorDescription", null)
        .put("deviceHealth", "OK")
        .put("clientTransactionIdentifier", s"txn-$idx")
        .put("businessUnit", "WM")
    }

    // Helper to create V2 struct
    def createV2Struct(idx: Int): Struct = {
      val metadataSchema = schemaV2.field("metadata").schema()
      val storeSchema    = schemaV2.field("store").schema()

      val metadata = new Struct(metadataSchema)
        .put("id", s"uuid-v2-$idx")
        .put("source", "com.test.producer")
        .put("type", "dsl.test.event")
        .put("specversion", 1.0f)
        .put("time", "2026-01-29T12:00:00Z")

      val store = new Struct(storeSchema)
        .put("countryCode", "CA")
        .put("storeNumber", 2000 + idx)
        .put("gln", null)

      new Struct(schemaV2)
        .put("metadata", metadata)
        .put("store", store)
        .put("action", "flash")
        .put("labelId", s"LABEL-V2-$idx")
        .put("wmCorrelationId", s"corr-v2-$idx")
        .put("wmClientId", "TestClient")
        .put("testMode", false)
        .put("eventTimeStamp", "2026-01-29T12:00:00Z")
        .put("state", "COMPLETE")
        .put("payload", s"""{"test": false, "idx": $idx}""")
        .put("errorCode", null)
        .put("errorDescription", null)
        .put("deviceHealth", "OK")
        .put("clientTransactionIdentifier", s"txn-v2-$idx")
    }

    val optimizer           = new AttachLatestSchemaOptimizer()
    val blobStream          = new BuildLocalOutputStream(toBufferedOutputStream(tempFile), topicPartition)
    val parquetFormatWriter = new ParquetFormatWriter(blobStream)

    // Batch 1: V3 records arrive first
    val batch1 = List(
      new SinkRecord("testTopic", 0, null, null, schemaV3, createV3Struct(1), 0L, 0L, TimestampType.CREATE_TIME, null),
      new SinkRecord("testTopic", 0, null, null, schemaV3, createV3Struct(2), 1L, 0L, TimestampType.CREATE_TIME, null),
    )
    val optimizedBatch1 = optimizer.update(batch1)

    for (record <- optimizedBatch1) {
      val struct = record.value().asInstanceOf[Struct]
      val writeResult = parquetFormatWriter.write(MessageDetail(
        NullSinkData(None),
        StructSinkData(struct),
        Map.empty,
        None,
        topic,
        1,
        Offset(record.kafkaOffset()),
      ))
      writeResult shouldBe Right(())
    }

    // Batch 2: V2 records arrive - will be adapted to V3 by optimizer
    // This is where the production bug manifests: ArrayIndexOutOfBoundsException
    val batch2 = List(
      new SinkRecord("testTopic", 0, null, null, schemaV2, createV2Struct(1), 2L, 0L, TimestampType.CREATE_TIME, null),
      new SinkRecord("testTopic", 0, null, null, schemaV2, createV2Struct(2), 3L, 0L, TimestampType.CREATE_TIME, null),
    )
    val optimizedBatch2 = optimizer.update(batch2)

    // Verify V2 records were adapted to V3
    optimizedBatch2.foreach { record =>
      record.valueSchema().version() shouldBe 3
      record.valueSchema().fields().size() shouldBe 15
    }

    for (record <- optimizedBatch2) {
      val struct = record.value().asInstanceOf[Struct]
      // This write fails with ArrayIndexOutOfBoundsException without the fix
      val writeResult = parquetFormatWriter.write(MessageDetail(
        NullSinkData(None),
        StructSinkData(struct),
        Map.empty,
        None,
        topic,
        1,
        Offset(record.kafkaOffset()),
      ))
      writeResult shouldBe Right(())
    }

    parquetFormatWriter.complete() shouldBe Right(())
  }
}
