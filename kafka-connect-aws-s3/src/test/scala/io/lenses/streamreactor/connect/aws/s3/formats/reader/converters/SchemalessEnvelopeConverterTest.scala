/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.formats.reader.converters

import cats.implicits.catsSyntaxOptionId
import io.circe.Json
import io.lenses.streamreactor.connect.aws.s3.model.location.S3LocationValidator
import io.lenses.streamreactor.connect.cloud.formats.reader.converters.SchemalessEnvelopeConverter
import io.lenses.streamreactor.connect.cloud.model.Topic
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocationValidator
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util.Base64
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.MapHasAsScala

class SchemalessEnvelopeConverterTest extends AnyFunSuite with Matchers {
  implicit val cloudLocationValidator: CloudLocationValidator = S3LocationValidator

  private val Header1Value = "header1"
  private val Header2Value = "header2"
  private val Header3Value = 123456789L

  private val SourceTopic = "my-topic"
  private val Timestamp   = 1234567890L
  private val Partition   = 11
  private val Offset      = 111L

  private val TargetTopic           = "target-topic"
  private val TargetPartition       = 0
  private val s3Location            = CloudLocation("bucket", "prefix".some, "/a/b/c.avro".some)
  private val LastModifiedTimestamp = Instant.ofEpochMilli(1001)

  test("envelope with key, value, headers and metadata is mapped to a SourceRecord") {
    val json = Json.obj(
      "key" -> Json.fromString("key"),
      "value" -> Json.obj(
        "field1" -> Json.fromString("value1"),
        "field2" -> Json.fromInt(2),
      ),
      "headers"  -> createHeaders(),
      "metadata" -> createMetadata(),
    )

    val actual = createConverter().convert(json.noSpaces, 0)
    assertOffsets(actual)
    actual.topic() shouldBe TargetTopic
    actual.kafkaPartition() shouldBe Partition
    actual.timestamp() shouldBe Timestamp
    actual.keySchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.key() shouldBe "key"
    actual.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.value() shouldBe """{"field1":"value1","field2":2}"""
    assertHeaders(actual)
  }

  test("envelope with key, value and metadata is mapped to a SourceRecord") {
    val json = Json.obj(
      "key" -> Json.fromString("key"),
      "value" -> Json.obj(
        "field1" -> Json.fromString("value1"),
        "field2" -> Json.fromInt(2),
      ),
      "metadata" -> createMetadata(),
    )

    val actual = createConverter().convert(json.noSpaces, 0)
    assertOffsets(actual)
    actual.topic() shouldBe TargetTopic
    actual.kafkaPartition() shouldBe Partition
    actual.timestamp() shouldBe Timestamp
    actual.keySchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.key() shouldBe "key"
    actual.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.value() shouldBe """{"field1":"value1","field2":2}"""
    actual.headers().asScala.map(h => h.key() -> h.value()).toMap shouldBe Map.empty
  }
  test("envelope with key, value and headers is mapped to a SourceRecord") {
    val json = Json.obj(
      "key" -> Json.fromString("key"),
      "value" -> Json.obj(
        "field1" -> Json.fromString("value1"),
        "field2" -> Json.fromInt(2),
      ),
      "headers" -> createHeaders(),
    )

    val now    = Instant.now()
    val actual = createConverter(() => now).convert(json.noSpaces, 0)
    assertOffsets(actual)
    actual.topic() shouldBe TargetTopic
    actual.kafkaPartition() shouldBe TargetPartition
    actual.timestamp() shouldBe now.toEpochMilli
    actual.keySchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.key() shouldBe "key"
    actual.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.value() shouldBe """{"field1":"value1","field2":2}"""
    assertHeaders(actual)
  }
  test("envelope with key and value is mapped to a SourceRecord") {
    val json = Json.obj(
      "key" -> Json.fromString("key"),
      "value" -> Json.obj(
        "field1" -> Json.fromString("value1"),
        "field2" -> Json.fromInt(2),
      ),
    )

    val now    = Instant.now()
    val actual = createConverter(() => now).convert(json.noSpaces, 0)
    assertOffsets(actual)
    actual.topic() shouldBe TargetTopic
    actual.kafkaPartition() shouldBe TargetPartition
    actual.timestamp() shouldBe now.toEpochMilli
    actual.keySchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.key() shouldBe "key"
    actual.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.value() shouldBe """{"field1":"value1","field2":2}"""
    actual.headers().asScala.map(h => h.key() -> h.value()).toMap shouldBe Map.empty
  }
  test("envelope with value and headers is mapped to a SourceRecord") {
    val json = Json.obj(
      "value" -> Json.obj(
        "field1" -> Json.fromString("value1"),
        "field2" -> Json.fromInt(2),
      ),
      "headers" -> createHeaders(),
    )

    val now    = Instant.now()
    val actual = createConverter(() => now).convert(json.noSpaces, 0)
    assertOffsets(actual)
    actual.topic() shouldBe TargetTopic
    actual.kafkaPartition() shouldBe TargetPartition
    actual.timestamp() shouldBe now.toEpochMilli
    actual.keySchema() shouldBe null
    actual.key() shouldBe null
    actual.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.value() shouldBe """{"field1":"value1","field2":2}"""
    assertHeaders(actual)
  }
  test("envelope with value is mapped to a SourceRecord") {
    val json = Json.obj(
      "value" -> Json.obj(
        "field1" -> Json.fromString("value1"),
        "field2" -> Json.fromInt(2),
      ),
    )

    val now    = Instant.now()
    val actual = createConverter(() => now).convert(json.noSpaces, 0)
    assertOffsets(actual)
    actual.topic() shouldBe TargetTopic
    actual.kafkaPartition() shouldBe TargetPartition
    actual.timestamp() shouldBe now.toEpochMilli
    actual.keySchema() shouldBe null
    actual.key() shouldBe null
    actual.valueSchema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    actual.value() shouldBe """{"field1":"value1","field2":2}"""
    actual.headers().asScala.map(h => h.key() -> h.value()).toMap shouldBe Map.empty
  }
  test("input not object raises an error") {
    val json = Json.fromString("not an object")
    assertThrows[RuntimeException](createConverter().convert(json.noSpaces, 0))
  }
  test("base64 encoded key and value are decoded as arrays") {
    val json = Json.obj(
      "key"          -> Json.fromString(Base64.getEncoder.encodeToString("key".getBytes())),
      "keyIsArray"   -> Json.fromBoolean(true),
      "value"        -> Json.fromString(Base64.getEncoder.encodeToString("value".getBytes())),
      "valueIsArray" -> Json.fromBoolean(true),
      "headers"      -> createHeaders(),
      "metadata"     -> createMetadata(),
    )
    val actual = createConverter().convert(json.noSpaces, 0)
    assertOffsets(actual)
    actual.topic() shouldBe TargetTopic
    actual.kafkaPartition() shouldBe Partition
    actual.timestamp() shouldBe Timestamp
    actual.keySchema().`type`() shouldBe Schema.OPTIONAL_BYTES_SCHEMA.`type`()
    actual.key() shouldBe "key".getBytes()
    actual.valueSchema().`type`() shouldBe Schema.OPTIONAL_BYTES_SCHEMA.`type`()
    actual.value() shouldBe "value".getBytes()
    assertHeaders(actual)
  }
  private def createConverter(instantF: () => Instant = () => Instant.now()): SchemalessEnvelopeConverter =
    new SchemalessEnvelopeConverter(Map("partition" -> "abc").asJava,
                                    Topic(TargetTopic),
                                    TargetPartition,
                                    s3Location,
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

  private def createHeaders(): Json =
    //create a json for the 3 headers
    Json.obj(
      "header1" -> Json.fromString(Header1Value),
      "header2" -> Json.fromString(Header2Value),
      "header3" -> Json.fromLong(Header3Value),
    )

  private def createMetadata(): Json =
    Json.obj(
      "timestamp" -> Json.fromLong(Timestamp),
      "topic"     -> Json.fromString(SourceTopic),
      "partition" -> Json.fromInt(Partition),
      "offset"    -> Json.fromLong(Offset),
    )
}
