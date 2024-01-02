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
import io.lenses.streamreactor.connect.cloud.common.formats.writer.ByteArraySinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MapSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StringSinkData
import io.lenses.streamreactor.connect.cloud.common.formats.writer.StructSinkData
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.jdk.CollectionConverters.MapHasAsJava

class AddConnectSchemaTransformerTest extends AnyFunSuite with Matchers {
  test("return an error if the topic is not matching the message") {
    val transformer = new AddConnectSchemaTransformer(Topic("different"), DataStorageSettings.enabled)
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
        value.getMessage shouldBe "Invalid state reached. Schema enrichment transformer topic [different] does not match incoming message topic [topic1]."
      case Right(_) => fail("Should have returned an error")
    }
  }
  test("ignores the key transformation if the settings does not store the key") {
    val transformer = new AddConnectSchemaTransformer(Topic("topic1"), DataStorageSettings.enabled.copy(key = false))
    val expected = MessageDetail(
      ByteArraySinkData(Array[Byte](1, 2, 3)),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    transformer.transform(expected).getOrElse(fail("Should have returned a message")) eq expected should be(true)
  }
  test("ignores the value transformation if the settings does not store the value") {
    val transformer = new AddConnectSchemaTransformer(Topic("topic1"), DataStorageSettings.enabled.copy(value = false))
    val expected = MessageDetail(
      StringSinkData("key"),
      ByteArraySinkData(Array[Byte](1, 2, 3)),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    transformer.transform(expected).getOrElse(fail("Should have returned a message")) eq expected should be(true)
  }
  test("ignores the key and value if the envelope is disabled") {
    val transformer = new AddConnectSchemaTransformer(Topic("topic1"), DataStorageSettings.disabled)
    val expected = MessageDetail(
      ByteArraySinkData(Array[Byte](1, 2, 3)),
      ByteArraySinkData(Array[Byte](1, 2, 3)),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    transformer.transform(expected).getOrElse(fail("Should have returned a message")) eq expected shouldBe true
  }
  test("converts the key from a map to a struct") {
    val transformer = new AddConnectSchemaTransformer(Topic("topic1"), DataStorageSettings.enabled)
    val map = Map[String, Any](
      "key1" -> "value1",
      "key2" -> "value2".getBytes(),
      "key3" -> 2,
      "key4" -> 3L,
      "key5" -> 4.0f,
      "key6" -> 5.0,
      "key7" -> true,
      "key8" -> 6.toShort,
      "key9" -> 7.toByte,
    ).asJava
    val expected = MessageDetail(
      MapSinkData(
        map,
        None,
      ),
      StructSinkData(SampleData.Users.head),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    val actual = transformer.transform(expected) match {
      case Left(value)  => fail(s"Should have returned a message: ${value.getMessage}")
      case Right(value) => value
    }
    actual.value eq expected.value shouldBe true
    actual.key match {
      case StructSinkData(struct) =>
        assertStruct(struct)

      case _ => fail("Should have returned a struct")
    }
  }

  test("converts the value from a map to a struct") {
    val transformer = new AddConnectSchemaTransformer(Topic("topic1"), DataStorageSettings.enabled)
    val map = Map[String, Any](
      "key1" -> "value1",
      "key2" -> "value2".getBytes(),
      "key3" -> 2,
      "key4" -> 3L,
      "key5" -> 4.0f,
      "key6" -> 5.0,
      "key7" -> true,
      "key8" -> 6.toShort,
      "key9" -> 7.toByte,
    ).asJava
    val expected = MessageDetail(
      StringSinkData("key"),
      MapSinkData(
        map,
        None,
      ),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    val actual = transformer.transform(expected) match {
      case Left(value)  => fail(s"Should have returned a message: ${value.getMessage}")
      case Right(value) => value
    }
    actual.key eq expected.key shouldBe true
    actual.value match {
      case StructSinkData(struct) =>
        assertStruct(struct)

      case _ => fail("Should have returned a struct")
    }
  }

  test("nested map validation") {
    val transformer = new AddConnectSchemaTransformer(Topic("topic1"), DataStorageSettings.enabled)
    val map = Map[String, Any](
      "key1" -> "value1",
      "key2" -> "value2".getBytes(),
      "key3" -> 2,
      "key4" -> 3L,
      "key5" -> 4.0f,
      "key6" -> 5.0,
      "key7" -> true,
      "key8" -> 6.toShort,
      "key9" -> 7.toByte,
      "key10" -> Map[String, Any](
        "key1" -> "value1",
        "key2" -> "value2".getBytes(),
        "key3" -> 2,
        "key4" -> 3L,
        "key5" -> 4.0f,
        "key6" -> 5.0,
        "key7" -> true,
        "key8" -> 6.toShort,
        "key9" -> 7.toByte,
      ).asJava,
    ).asJava

    val expected = MessageDetail(
      StringSinkData("key"),
      MapSinkData(
        map,
        None,
      ),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    val actual = transformer.transform(expected) match {
      case Left(value)  => fail(s"Should have returned a message: ${value.getMessage}")
      case Right(value) => value
    }
    actual.key eq expected.key shouldBe true
    actual.value match {
      case StructSinkData(struct) =>
        assertStruct(struct)
        assertStruct(struct.getStruct("key10"))
      case _ => fail("Should have returned a struct")
    }
  }

  test("return error if the Key map keys are not string") {
    val transformer = new AddConnectSchemaTransformer(Topic("topic1"), DataStorageSettings.enabled)
    val map = Map[Any, Any](
      1      -> "value1",
      "key2" -> "value2".getBytes(),
      "key3" -> 2,
      "key4" -> 3L,
      "key5" -> 4.0f,
      "key6" -> 5.0,
      "key7" -> true,
      "key8" -> 6.toShort,
      "key9" -> 7.toByte,
    ).asJava

    val expected = MessageDetail(
      StringSinkData("key"),
      MapSinkData(
        map,
        None,
      ),
      Map("header1" -> StringSinkData("value1"), "header2" -> ByteArraySinkData("value2".getBytes())),
      Some(Instant.now()),
      Topic("topic1"),
      0,
      Offset(12),
    )
    transformer.transform(expected) match {
      case Left(value) =>
        value.getMessage shouldBe "Conversion not supported for type [java.lang.Integer]. Input is a Map with non-String keys."
      case Right(_) => fail("Should have returned an error")
    }
  }
  private def assertStruct(struct: Struct): Assertion = {
    //validate struct schema  and values
    struct.schema().field("key1").schema() shouldBe Schema.OPTIONAL_STRING_SCHEMA
    struct.schema().field("key2").schema() shouldBe Schema.OPTIONAL_BYTES_SCHEMA
    struct.schema().field("key3").schema() shouldBe Schema.OPTIONAL_INT32_SCHEMA
    struct.schema().field("key4").schema() shouldBe Schema.OPTIONAL_INT64_SCHEMA
    struct.schema().field("key5").schema() shouldBe Schema.OPTIONAL_FLOAT32_SCHEMA
    struct.schema().field("key6").schema() shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA
    struct.schema().field("key7").schema() shouldBe Schema.OPTIONAL_BOOLEAN_SCHEMA
    struct.schema().field("key8").schema() shouldBe Schema.OPTIONAL_INT16_SCHEMA
    struct.schema().field("key9").schema() shouldBe Schema.OPTIONAL_INT8_SCHEMA

    struct.getString("key1") shouldBe "value1"
    struct.getBytes("key2") shouldBe "value2".getBytes()
    struct.getInt32("key3") shouldBe 2
    struct.getInt64("key4") shouldBe 3L
    struct.getFloat32("key5") shouldBe 4.0f
    struct.getFloat64("key6") shouldBe 5.0
    struct.getBoolean("key7") shouldBe true
    struct.getInt16("key8") shouldBe 6
    struct.getInt8("key9") shouldBe 7
  }
}
