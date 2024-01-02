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
package io.lenses.streamreactor.connect.cloud.common.sink.transformers

import io.lenses.streamreactor.connect.cloud.common.config.DataStorageSettings
import io.lenses.streamreactor.connect.cloud.common.formats.writer._
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.util
import java.util.Collections
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class SchemalessEnvelopeTransformerTests extends AnyFunSuite with Matchers {
  test("returns an error when the topic does not match") {
    val transformer = SchemalessEnvelopeTransformer(Topic("different"), DataStorageSettings.enabled)
    val expected = MessageDetail(
      StringSinkData("key"),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    transformer.transform(expected) match {
      case Left(value) =>
        value.getMessage shouldBe "Invalid state reached. Envelope transformer topic [different] does not match incoming message topic [topic1]."
      case Right(_) => fail("Should have returned an error")
    }
  }

  test("returns the message when the envelope settings is disabled") {
    val transformer = SchemalessEnvelopeTransformer(Topic("topic1"), DataStorageSettings.disabled)
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
    val transformer = SchemalessEnvelopeTransformer(Topic("topic1"), settings)
    val actual      = transformer.transform(expected).getOrElse(fail("Should have returned a message"))
    //only the value is changed. this needs to be cleaned up later.
    actual.key eq expected.key should be(true)
    actual.headers eq expected.headers should be(true)
    actual.timestamp shouldBe expected.timestamp
    actual.topic shouldBe expected.topic
    actual.partition shouldBe expected.partition
    actual.offset shouldBe expected.offset

    assert(actual.value) { map =>
      if (settings.key) {
        map.get("key") shouldBe SchemalessEnvelopeTransformer.convert(expected.key)
      } else {
        map.containsKey("key") shouldBe false
      }

      if (settings.value) {
        map.get("value") shouldBe SchemalessEnvelopeTransformer.convert(expected.value)
      } else {
        map.containsKey("value") shouldBe false
      }
      if (settings.headers) {
        val headersMap = map.get("headers").asInstanceOf[java.util.Map[_, _]]
        expected.headers.foreach {
          case (k, v) =>
            headersMap.get(k) shouldBe SchemalessEnvelopeTransformer.convert(v)
        }
      } else {
        map.containsKey("headers") shouldBe false
      }

      if (settings.metadata) {
        val metadata = map.get("metadata").asInstanceOf[java.util.Map[_, _]]

        metadata.get("timestamp") shouldBe expected.timestamp.get.toEpochMilli
        metadata.get("topic") shouldBe expected.topic.value
        metadata.get("partition") shouldBe expected.partition
        metadata.get("offset") shouldBe expected.offset.value

      } else {
        map.containsKey("metadata") shouldBe false
      }
    }
  }

  test("converting a struct returns a Map") {
    val struct = SampleData.Users.head
    val map    = SchemalessEnvelopeTransformer.convert(StructSinkData(struct))
    map shouldBe a[java.util.Map[_, _]]
    val map2 = map.asInstanceOf[java.util.Map[String, Any]]
    map2.get("name") shouldBe "sam"
    map2.get("title") shouldBe "mr"
    map2.get("salary") shouldBe 100.43
  }

  test("converting a nested struct returns a Map with nested Map") {
    val nestedSchema = SchemaBuilder.struct()
      .field("field", Schema.INT32_SCHEMA)
      .build();
    val parentSchema = SchemaBuilder.struct()
      .field("first", Schema.INT32_SCHEMA)
      .field("second", Schema.STRING_SCHEMA)
      .field("array", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .field("map", SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build())
      .field("nested", nestedSchema)
      .build();
    val structValue = new Struct(parentSchema)
      .put("first", 1)
      .put("second", "foo")
      .put("array", util.Arrays.asList(1, 2, 3))
      .put("map", Collections.singletonMap(1, "value"))
      .put("nested", new Struct(nestedSchema).put("field", 12));

    val map = SchemalessEnvelopeTransformer.convert(StructSinkData(structValue))
    map shouldBe a[java.util.Map[_, _]]
    val map2 = map.asInstanceOf[java.util.Map[String, Any]]
    map2.get("first") shouldBe 1
    map2.get("second") shouldBe "foo"
    map2.get("array") shouldBe util.Arrays.asList(1, 2, 3)
    map2.get("map") shouldBe Collections.singletonMap(1, "value")
    map2.get("nested") shouldBe Collections.singletonMap("field", 12)
  }

  test("key is bytes array adds a keyIsArray field in the envelope") {
    val settings = DataStorageSettings.enabled
    val expected = MessageDetail(
      ByteArraySinkData("key".getBytes(), Some(Schema.OPTIONAL_BYTES_SCHEMA)),
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
    val transformer = SchemalessEnvelopeTransformer(Topic("topic1"), settings)
    val actual      = transformer.transform(expected).getOrElse(fail("Should have returned a message"))
    assert(actual.value) { map =>
      map.get("key") shouldBe "key".getBytes()
      map.get("keyIsArray") shouldBe true
      map.containsKey("valueIsArray") shouldBe false
    }
  }
  test("value is bytes array adds isValueArray field in the envelope") {
    val settings = DataStorageSettings.enabled
    val expected = MessageDetail(
      StringSinkData("key", Some(Schema.OPTIONAL_STRING_SCHEMA)),
      ByteArraySinkData("value".getBytes(), Some(Schema.OPTIONAL_BYTES_SCHEMA)),
      Map(
        "header1" -> StringSinkData("value1", Some(Schema.STRING_SCHEMA)),
        "header2" -> ByteArraySinkData("value2".getBytes(), Some(Schema.BYTES_SCHEMA)),
      ),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    val transformer = SchemalessEnvelopeTransformer(Topic("topic1"), settings)
    val actual      = transformer.transform(expected).getOrElse(fail("Should have returned a message"))
    assert(actual.value) { map =>
      map.get("value") shouldBe "value".getBytes()
      map.get("valueIsArray") shouldBe true
      map.containsKey("keyIsArray") shouldBe false
    }
  }
  private def assert(value: SinkData)(fn: java.util.Map[_, _] => Assertion): Assertion =
    value match {
      case MapSinkData(map, _) => fn(map)
      case _                   => fail("Value should be a struct")
    }
}
