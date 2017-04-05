/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.influx

import io.confluent.common.config.ConfigException
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.scalatest.{Matchers, WordSpec}

class StructFieldsExtractorTest extends WordSpec with Matchers {
  "StructFieldsExtractor" should {
    "return all the fields and their bytes value" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val min = System.currentTimeMillis()
      val record = StructFieldsExtractor(true, Map.empty, None, Set.empty).get(struct)
      val max = System.currentTimeMillis()
      (min <= record.timestamp && record.timestamp <= max) shouldBe true
      val map = record.fields.toMap
      map("firstName") shouldBe "Alex"
      map("lastName") shouldBe "Smith"
      map("age") shouldBe 30
    }


    "throw an exception if the timestamp field is not present in the struct" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      intercept[ConfigException] {
        StructFieldsExtractor(true, Map.empty, Some("ts"), Set.empty).get(struct)
      }
    }

    "throw an exception if the timestamp field is a string/double/float" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("abc", Schema.STRING_SCHEMA)
        .field("d", Schema.FLOAT64_SCHEMA)
        .field("f", Schema.FLOAT32_SCHEMA).build()

      val struct = new Struct(schema)
        .put("abc", "Alex")
        .put("d", 1231.5)
        .put("f", -5.93.toFloat)

      intercept[ConfigException] {
        StructFieldsExtractor(true, Map.empty, Some("abc"), Set.empty).get(struct)
      }

      intercept[ConfigException] {
        StructFieldsExtractor(true, Map.empty, Some("d"), Set.empty).get(struct)
      }

      intercept[ConfigException] {
        StructFieldsExtractor(true, Map.empty, Some("f"), Set.empty).get(struct)
      }
    }

    "throw an excception if a field is in bytes" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("bibble", Schema.BYTES_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("bibble", Array(1.toByte, 121.toByte, -111.toByte))
        .put("age", 30)

      intercept[RuntimeException] {
        StructFieldsExtractor(true, Map.empty, None, Set.empty).get(struct)
      }
    }

    "return all fields and apply the mapping" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val map: Map[String, Any] = StructFieldsExtractor(includeAllFields = true, Map("lastName" -> "Name", "age" -> "a"), None, ignoredFields = Set.empty).get(struct).fields.toMap
      map("firstName") shouldBe "Alex"
      map("Name") shouldBe "Smith"
      map("a") shouldBe 30

    }

    "return only the specified fields" in {
      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("lastName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct = new Struct(schema)
        .put("firstName", "Alex")
        .put("lastName", "Smith")
        .put("age", 30)

      val map: Map[String, Any] = StructFieldsExtractor(includeAllFields = false, Map("lastName" -> "Name", "age" -> "age"), None, ignoredFields = Set.empty).get(struct).fields.toMap
      map("Name") shouldBe "Smith"
      map("age") shouldBe 30
      map.size shouldBe 2
    }
  }
}
