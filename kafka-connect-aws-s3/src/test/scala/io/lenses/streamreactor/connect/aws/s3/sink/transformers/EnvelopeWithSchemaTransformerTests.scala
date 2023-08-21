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
package io.lenses.streamreactor.connect.aws.s3.sink.transformers

import io.lenses.streamreactor.connect.aws.s3.config.DataStorageSettings
import io.lenses.streamreactor.connect.aws.s3.formats.writer._
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.utils.SampleData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class EnvelopeWithSchemaTransformerTests extends AnyFunSuite with Matchers {
  test("returns an error when the topic does not match") {
    val transformer = EnvelopeWithSchemaTransformer(Topic("different"), DataStorageSettings.enabled)
    val expected = MessageDetail(
      StringSinkData("key"),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    transformer.transform(expected) should be(
      Left(
        new RuntimeException(
          "Invalid state reached. Envelope transformer topic [different] does not match incoming message topic [topic1].",
        ),
      ),
    )
  }

  test("returns the message when the envelope settings is disabled") {
    val transformer = EnvelopeWithSchemaTransformer(Topic("topic1"), DataStorageSettings.disabled)
    val expected = MessageDetail(
      StringSinkData("key"),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    transformer.transform(expected).getOrElse(fail("Should have returned a message")) eq expected should be(true)
  }

  test("full envelope") {
    val expected = MessageDetail(
      StringSinkData("key", Some(Schema.STRING_SCHEMA)),
      StructSinkData(SampleData.Users.head),
      Map(
        "header1" -> StringSinkData("value1", Some(Schema.STRING_SCHEMA)),
        "header2" -> ByteArraySinkData("value2".getBytes(), Some(Schema.BYTES_SCHEMA)),
      ),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    run(DataStorageSettings.enabled, expected)
  }
  test("envelope without metadata") {
    val settings = DataStorageSettings.enabled.copy(metadata = false)
    val expected = MessageDetail(
      StringSinkData("key", Some(Schema.OPTIONAL_STRING_SCHEMA)),
      StructSinkData(SampleData.Users.head),
      Map(
        "header1" -> StringSinkData("value1", Some(Schema.STRING_SCHEMA)),
        "header2" -> ByteArraySinkData("value2".getBytes(), Some(Schema.BYTES_SCHEMA)),
      ),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    run(settings, expected)
  }

  test("envelope without headers") {
    val settings = DataStorageSettings.enabled.copy(headers = false)
    val expected = MessageDetail(
      StringSinkData("key", Some(Schema.OPTIONAL_STRING_SCHEMA)),
      StructSinkData(SampleData.Users.head),
      Map(
        "header1" -> StringSinkData("value1", Some(Schema.STRING_SCHEMA)),
        "header2" -> ByteArraySinkData("value2".getBytes(), Some(Schema.BYTES_SCHEMA)),
      ),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    run(settings, expected)
  }

  test("envelope without key") {
    val settings = DataStorageSettings.enabled.copy(key = false)
    val expected = MessageDetail(
      StringSinkData("key", Some(Schema.OPTIONAL_STRING_SCHEMA)),
      StructSinkData(SampleData.Users.head),
      Map(
        "header1" -> StringSinkData("value1", Some(Schema.STRING_SCHEMA)),
        "header2" -> ByteArraySinkData("value2".getBytes(), Some(Schema.BYTES_SCHEMA)),
      ),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    run(settings, expected)
  }
  test("envelope without value") {
    val settings = DataStorageSettings.enabled.copy(value = false)

    val expected = MessageDetail(
      StringSinkData("key", Some(Schema.OPTIONAL_STRING_SCHEMA)),
      StructSinkData(SampleData.Users.head),
      Map(
        "header1" -> StringSinkData("value1", Some(Schema.STRING_SCHEMA)),
        "header2" -> ByteArraySinkData("value2".getBytes(), Some(Schema.BYTES_SCHEMA)),
      ),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    run(settings, expected)
  }
  test("envelope value is an array") {
    val settings = DataStorageSettings.enabled

    val expected = MessageDetail(
      StringSinkData("key", Some(Schema.OPTIONAL_STRING_SCHEMA)),
      ArraySinkData(SampleData.Users.asJava, Some(SchemaBuilder.array(SampleData.UsersSchema).build())),
      Map(
        "header1" -> StringSinkData("value1", Some(Schema.STRING_SCHEMA)),
        "header2" -> ByteArraySinkData("value2".getBytes(), Some(Schema.BYTES_SCHEMA)),
      ),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    run(settings, expected)
  }
  test("envelope value is a map") {
    val settings = DataStorageSettings.enabled

    val expected = MessageDetail(
      StringSinkData("key", Some(Schema.OPTIONAL_STRING_SCHEMA)),
      MapSinkData(
        Map(
          "key1" -> SampleData.Users.head,
          "key2" -> SampleData.Users.tail.head,
        ).asJava,
        Some(SchemaBuilder.map(Schema.STRING_SCHEMA, SampleData.UsersSchema).build()),
      ),
      Map(
        "header1" -> StringSinkData("value1", Some(Schema.STRING_SCHEMA)),
        "header2" -> ByteArraySinkData("value2".getBytes(), Some(Schema.BYTES_SCHEMA)),
      ),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    run(settings, expected)
  }

  private def run(settings: DataStorageSettings, expected: MessageDetail): Assertion = {
    val transformer = EnvelopeWithSchemaTransformer(Topic("topic1"), settings)
    val actual      = transformer.transform(expected).getOrElse(fail("Should have returned a message"))
    //only the value is changed. this needs to be cleaned up later.
    actual.key eq expected.key should be(true)
    actual.headers eq expected.headers should be(true)
    actual.timestamp shouldBe expected.timestamp
    actual.topic shouldBe expected.topic
    actual.partition shouldBe expected.partition
    actual.offset shouldBe expected.offset

    assert(actual.value) { struct =>
      if (settings.key) {
        val keyS = struct.schema().field("key").schema()
        keyS.isOptional shouldBe true
        keyS.schema() shouldBe EnvelopeWithSchemaTransformer.toOptional(expected.key.schema().get)
        struct.get("key") shouldBe EnvelopeWithSchemaTransformer.toOptionalConnectData(expected.key)
      } else {
        struct.schema().field("key") shouldBe null
      }

      if (settings.value) {
        struct.get("value") shouldBe EnvelopeWithSchemaTransformer.toOptionalConnectData(expected.value)
      } else {
        struct.schema().field("value") shouldBe null
      }
      if (settings.headers) {
        val headersS = struct.schema().field("headers").schema()
        expected.headers.foreach {
          case (k, v) =>
            headersS.field(k).schema() shouldBe EnvelopeWithSchemaTransformer.toOptional(v.schema().get)
            struct.get("headers").asInstanceOf[Struct].get(
              k,
            ) shouldBe EnvelopeWithSchemaTransformer.toOptionalConnectData(v)
        }
      } else {
        struct.schema().field("headers") shouldBe null
      }

      if (settings.metadata) {
        val metadataS = struct.schema().field("metadata").schema()
        metadataS.field("timestamp").schema() shouldBe Schema.INT64_SCHEMA
        metadataS.field("topic").schema() shouldBe Schema.STRING_SCHEMA
        metadataS.field("partition").schema() shouldBe Schema.INT32_SCHEMA
        metadataS.field("offset").schema() shouldBe Schema.INT64_SCHEMA

        val metadata = struct.get("metadata").asInstanceOf[Struct]
        metadata.get("timestamp") shouldBe expected.timestamp.get.toEpochMilli
        metadata.get("topic") shouldBe expected.topic.value
        metadata.get("partition") shouldBe expected.partition
        metadata.get("offset") shouldBe expected.offset.value

      } else {
        struct.schema().field("metadata") shouldBe null
      }
    }
  }
  private def assert(value: SinkData)(fn: Struct => Assertion): Assertion =
    value match {
      case StructSinkData(struct) => fn(struct)
      case _                      => fail("Value should be a struct")
    }
}
