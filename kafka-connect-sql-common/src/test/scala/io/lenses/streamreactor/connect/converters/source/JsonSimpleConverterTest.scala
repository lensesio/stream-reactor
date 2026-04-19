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
package io.lenses.streamreactor.connect.converters.source

import io.lenses.streamreactor.common.converters.MsgKey
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.ConnectException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.Collections
import scala.jdk.CollectionConverters.ListHasAsScala

class JsonSimpleConverterTest extends AnyWordSpec with Matchers {
  val topic       = "the_real_topic"
  val sourceTopic = "source_topic"

  "JsonSimpleConverter" should {

    "convert a flat JSON object to a Struct" in {
      val car       = Car("LaFerrari", "Ferrari", 2015, 963, 0.0001)
      val json      = JacksonJson.toJson(car)
      val converter = new JsonSimpleConverter
      val record    = converter.convert(topic, sourceTopic, "100", json.getBytes)
      record.keySchema() shouldBe MsgKey.schema
      record.key() shouldBe MsgKey.getStruct(sourceTopic, "100")

      record.valueSchema().fields().asScala.count(f => f.name() == "name") shouldBe 1
      record.valueSchema().fields().asScala.count(f => f.name() == "manufacturer") shouldBe 1
      record.valueSchema().fields().asScala.count(f => f.name() == "model") shouldBe 1
      record.valueSchema().fields().asScala.count(f => f.name() == "bhp") shouldBe 1
      record.valueSchema().fields().asScala.count(f => f.name() == "price") shouldBe 1

      val struct = record.value().asInstanceOf[Struct]
      struct.get("name") shouldBe "LaFerrari"
      struct.get("manufacturer") shouldBe "Ferrari"
      struct.get("model") shouldBe 2015
      struct.get("bhp") shouldBe 963
      struct.get("price") shouldBe 0.0001

      record.sourcePartition() shouldBe Collections.singletonMap(Converter.TopicKey, sourceTopic)
      record.sourceOffset() shouldBe null
    }

    "convert an empty JSON array without throwing" in {
      val sv = JsonSimpleConverter.convert(sourceTopic, "[]")
      sv.schema().`type`() shouldBe Schema.Type.ARRAY
      sv.schema().valueSchema() shouldBe Schema.OPTIONAL_STRING_SCHEMA
      sv.schema().isOptional shouldBe true
      sv.value().asInstanceOf[java.util.List[_]] shouldBe empty
    }

    "convert a non-empty JSON array of strings" in {
      val sv    = JsonSimpleConverter.convert(sourceTopic, """["a","b","c"]""")
      val items = sv.value().asInstanceOf[java.util.List[_]].asScala
      sv.schema().`type`() shouldBe Schema.Type.ARRAY
      items shouldBe List("a", "b", "c")
    }

    "convert a non-empty JSON array of objects" in {
      val sv    = JsonSimpleConverter.convert(sourceTopic, """[{"x":1},{"x":2}]""")
      val items = sv.value().asInstanceOf[java.util.List[_]].asScala
      sv.schema().`type`() shouldBe Schema.Type.ARRAY
      items should have size 2
      items.head.asInstanceOf[Struct].get("x") shouldBe 1L
    }

    "convert a JSON object with an empty array field without throwing" in {
      val sv     = JsonSimpleConverter.convert(sourceTopic, """{"name":"bridge","extensions":[]}""")
      val struct = sv.value().asInstanceOf[Struct]
      struct.get("name") shouldBe "bridge"
      struct.getArray("extensions") shouldBe empty
    }

    "convert a JSON object with a nested empty array field without throwing" in {
      val json = """{"level1":{"items":[],"count":0}}"""
      val sv   = JsonSimpleConverter.convert(sourceTopic, json)
      val l1   = sv.value().asInstanceOf[Struct].get("level1").asInstanceOf[Struct]
      l1.getArray("items") shouldBe empty
      l1.get("count") shouldBe 0L
    }

    "convert scalar JSON types correctly via JValue overload" in {
      import org.json4s._
      JsonSimpleConverter.convert(sourceTopic, JBool(true)).value() shouldBe true
      JsonSimpleConverter.convert(sourceTopic, JBool(false)).value() shouldBe false
      JsonSimpleConverter.convert(sourceTopic, JInt(42)).value() shouldBe 42L
      JsonSimpleConverter.convert(sourceTopic, JDouble(3.14)).value() shouldBe 3.14
      JsonSimpleConverter.convert(sourceTopic, JString("hello")).value() shouldBe "hello"
      JsonSimpleConverter.convert(sourceTopic, JNull).value() shouldBe null
    }

    "convert a JSON object containing scalar fields of all types" in {
      val json = """{"flag":true,"count":42,"ratio":3.14,"label":"hello","nothing":null}"""
      val sv   = JsonSimpleConverter.convert(sourceTopic, json)
      val s    = sv.value().asInstanceOf[Struct]
      s.get("flag") shouldBe true
      s.get("count") shouldBe 42L
      s.get("ratio") shouldBe 3.14
      s.get("label") shouldBe "hello"
      s.get("nothing") shouldBe null
    }

    "throw ConnectException on invalid JSON" in {
      val converter = new JsonSimpleConverter
      intercept[ConnectException] {
        converter.convert(topic, sourceTopic, "1", "not json {{{".getBytes)
      }
    }

    "throw ConnectException on null input" in {
      val converter = new JsonSimpleConverter
      intercept[ConnectException] {
        converter.convert(topic, sourceTopic, "1", null)
      }
    }

    "use the source topic as the Kafka record key when no keys are specified" in {
      val json      = """{"id":"abc","value":1}"""
      val converter = new JsonSimpleConverter
      val record    = converter.convert(topic, sourceTopic, "1", json.getBytes)
      record.key() shouldBe MsgKey.getStruct(sourceTopic, "1")
    }

    "extract a field value as the Kafka record key when keys are specified" in {
      val json      = """{"id":"abc","value":1}"""
      val converter = new JsonSimpleConverter
      val record    = converter.convert(topic, sourceTopic, "1", json.getBytes, keys = Seq("id"))
      record.key() shouldBe "abc"
      record.keySchema() shouldBe Schema.STRING_SCHEMA
    }
  }
}
