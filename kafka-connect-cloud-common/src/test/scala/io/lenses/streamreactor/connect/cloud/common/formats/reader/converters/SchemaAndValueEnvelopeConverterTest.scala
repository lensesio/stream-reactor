/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.formats.reader.converters

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaAndValue
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class SchemaAndValueEnvelopeConverterTest extends AnyFunSuite with Matchers {
  implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val HeadersSchema = SchemaBuilder.struct()
    .field("header1", Schema.STRING_SCHEMA)
    .field("header2", Schema.STRING_SCHEMA)
    .field("header3", Schema.INT64_SCHEMA)
    .build()

  private val MetadataSchema = SchemaBuilder.struct()
    .field("timestamp", Schema.INT64_SCHEMA).optional()
    .field("topic", Schema.STRING_SCHEMA)
    .field("partition", Schema.INT32_SCHEMA)
    .field("offset", Schema.INT64_SCHEMA)
    .build()

  private val Header1Value = "header1"
  private val Header2Value = "header2"
  private val Header3Value = 123456789L

  private val SourceTopic = "my-topic"
  private val Timestamp   = 1234567890L
  private val Partition   = 11
  private val Offset      = 111L

  private val TargetTopic           = "target-topic"
  private val TargetPartition       = 0
  private val cloudLocation         = CloudLocation("bucket", "prefix".some, "/a/b/c.avro".some)
  private val LastModifiedTimestamp = Instant.ofEpochMilli(1001)

  test("envelope with key, value, headers and metadata is mapped to a SourceRecord") {
    val allEnvelopeFieldsSchema = SchemaBuilder.struct()
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .field("value", Schema.OPTIONAL_STRING_SCHEMA)
      .field("headers", HeadersSchema)
      .field(
        "metadata",
        MetadataSchema,
      )
      .build()
    val envelope = new Struct(allEnvelopeFieldsSchema)

    val key   = "key"
    val value = "value"

    envelope.put("key", key)
    envelope.put("value", value)

    envelope.put("headers", createHeaders())

    envelope.put("metadata", createMetadata())

    val schemaAndValueEnveloper = new SchemaAndValue(allEnvelopeFieldsSchema, envelope)

    val sourceRecord = createConverter().convert(schemaAndValueEnveloper, 0L)
    assertOffsets(sourceRecord)
    sourceRecord.topic() shouldBe TargetTopic
    sourceRecord.kafkaPartition() shouldBe Partition
    sourceRecord.timestamp() shouldBe Timestamp
    sourceRecord.keySchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    sourceRecord.key() shouldBe key
    sourceRecord.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    sourceRecord.value() shouldBe value
    assertHeaders(sourceRecord)
  }

  test("envelope with key, value, headers and metadata is mapped to a SourceRecord with null key") {

    val envelopeSchema = SchemaBuilder.struct()
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .field("value", Schema.OPTIONAL_STRING_SCHEMA)
      .field("headers", HeadersSchema)
      .field(
        "metadata",
        MetadataSchema,
      )
      .build()
    val envelope = new Struct(envelopeSchema)

    envelope.put("headers", createHeaders())

    val value = "value"
    envelope.put("value", value)

    envelope.put("metadata", createMetadata())

    val schemaAndValueEnveloper = new SchemaAndValue(envelopeSchema, envelope)

    val sourceRecord = createConverter().convert(schemaAndValueEnveloper, 0L)
    assertOffsets(sourceRecord)
    sourceRecord.topic() shouldBe TargetTopic
    sourceRecord.kafkaPartition() shouldBe Partition
    sourceRecord.timestamp() shouldBe Timestamp
    sourceRecord.keySchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    sourceRecord.key() shouldBe null
    sourceRecord.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    sourceRecord.value() shouldBe value
    assertHeaders(sourceRecord)
  }
  test("envelope with key, metadata, and headers does not populate the SourceRecord value") {

    val envelopeSchema = SchemaBuilder.struct()
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .field("headers", HeadersSchema)
      .field(
        "metadata",
        MetadataSchema,
      )
      .build()
    val envelope = new Struct(envelopeSchema)

    val key = "lorem ipsum"
    envelope.put("key", key)
    envelope.put("headers", createHeaders())
    envelope.put("metadata", createMetadata())
    val schemaAndValueEnveloper = new SchemaAndValue(envelopeSchema, envelope)

    val sourceRecord = createConverter().convert(schemaAndValueEnveloper, 0L)
    assertOffsets(sourceRecord)
    sourceRecord.topic() shouldBe TargetTopic
    sourceRecord.kafkaPartition() shouldBe Partition
    sourceRecord.timestamp() shouldBe Timestamp
    sourceRecord.keySchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    sourceRecord.key() shouldBe key
    sourceRecord.valueSchema() shouldBe null
    sourceRecord.value() shouldBe null
    assertHeaders(sourceRecord)
  }

  test("an envelope without metadata uses the targetPartition and a current Instant value") {

    val envelopeSchema = SchemaBuilder.struct()
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .field("value", Schema.OPTIONAL_STRING_SCHEMA)
      .field("headers", HeadersSchema)
      .build()
    val envelope = new Struct(envelopeSchema)

    val key   = "key"
    val value = "value"

    envelope.put("key", key)
    envelope.put("value", value)

    envelope.put("headers", createHeaders())

    val schemaAndValueEnveloper = new SchemaAndValue(envelopeSchema, envelope)

    val sourceRecord = createConverter(() => LastModifiedTimestamp).convert(schemaAndValueEnveloper, 0L)
    assertOffsets(sourceRecord)
    sourceRecord.topic() shouldBe TargetTopic
    sourceRecord.kafkaPartition() shouldBe TargetPartition
    sourceRecord.timestamp() shouldBe LastModifiedTimestamp.toEpochMilli
    sourceRecord.keySchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    sourceRecord.key() shouldBe key
    sourceRecord.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    sourceRecord.value() shouldBe value
    assertHeaders(sourceRecord)
  }
  test("envelope without headers returns empty SourceRecord headers") {
    val envelopeSchema = SchemaBuilder.struct()
      .field("key", Schema.OPTIONAL_STRING_SCHEMA)
      .field("value", Schema.OPTIONAL_STRING_SCHEMA)
      .field("metadata", MetadataSchema)
      .build()
    val envelope = new Struct(envelopeSchema)

    val key   = "key"
    val value = "value"

    envelope.put("key", key)
    envelope.put("value", value)
    envelope.put("metadata", createMetadata())

    val schemaAndValueEnveloper = new SchemaAndValue(envelopeSchema, envelope)

    val sourceRecord = createConverter().convert(schemaAndValueEnveloper, 0L)
    assertOffsets(sourceRecord)
    sourceRecord.topic() shouldBe TargetTopic
    sourceRecord.kafkaPartition() shouldBe Partition
    sourceRecord.timestamp() shouldBe Timestamp
    sourceRecord.keySchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    sourceRecord.key() shouldBe key
    sourceRecord.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    sourceRecord.value() shouldBe value
    sourceRecord.headers().size() shouldBe 0
  }
  test("non-Struct input returns an exception") {
    val schemaAndValueEnveloper = new SchemaAndValue(Schema.STRING_SCHEMA, "lorem ipsum")
    assertThrows[RuntimeException] {
      createConverter().convert(schemaAndValueEnveloper, 0L)
    }
  }
  private def createConverter(instantF: () => Instant = () => Instant.now()): SchemaAndValueEnvelopeConverter =
    new SchemaAndValueEnvelopeConverter(Map("partition" -> "abc").asJava,
                                        Topic(TargetTopic),
                                        TargetPartition,
                                        cloudLocation,
                                        LastModifiedTimestamp,
                                        instantF,
    )
  private def assertOffsets(sourceRecord: SourceRecord): Assertion = {
    sourceRecord.sourcePartition().asScala shouldBe Map("partition" -> "abc")
    sourceRecord.sourceOffset().asScala shouldBe Map("path" -> "/a/b/c.avro",
                                                     "line" -> "0",
                                                     "ts"   -> LastModifiedTimestamp.toEpochMilli.toString,
    )
  }
  private def assertHeaders(record: SourceRecord): Assertion =
    record.headers().asScala.map(h => h.key() -> h.value()).toMap shouldBe Map(
      "header1" -> Header1Value,
      "header2" -> Header2Value,
      "header3" -> Header3Value,
    )
  private def createHeaders(): Struct = {
    val headers = new Struct(HeadersSchema)
    headers.put("header1", Header1Value)
    headers.put("header2", Header2Value)
    headers.put("header3", Header3Value)
    headers
  }

  private def createMetadata(): Struct = {
    val metadata = new Struct(MetadataSchema)
    metadata.put("timestamp", Timestamp)
    metadata.put("topic", SourceTopic)
    metadata.put("partition", Partition)
    metadata.put("offset", Offset)
    metadata
  }

}
