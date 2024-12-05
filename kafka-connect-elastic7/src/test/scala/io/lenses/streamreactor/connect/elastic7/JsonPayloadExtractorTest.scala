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
package io.lenses.streamreactor.connect.elastic7

import com.fasterxml.jackson.databind.node.TextNode
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues
import org.scalatest.OptionValues

import java.nio.ByteBuffer
import java.util.{ HashMap => JHashMap }

class JsonPayloadExtractorTest extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  test("handle null values") {
    val result = JsonPayloadExtractor.extractJsonNode(null, null)
    result shouldBe Right(None)
  }

  test("handle string JSON with schema") {
    val jsonString = """{"name": "test"}"""
    val schema     = Schema.STRING_SCHEMA
    val result     = JsonPayloadExtractor.extractJsonNode(jsonString, schema)

    result.value.value.get("name").asText() shouldBe "test"
  }

  test("primitive string is read as TextNode") {
    val invalidJson = "invalid json"
    val schema      = Schema.STRING_SCHEMA
    val result      = JsonPayloadExtractor.extractJsonNode(invalidJson, schema)

    result.value.value shouldBe a[TextNode]
    result.value.value.asText() shouldBe invalidJson
  }

  test("handle byte array with schema") {
    val jsonString = """{"name": "test"}"""
    val bytes      = jsonString.getBytes("UTF-8")
    val schema     = Schema.BYTES_SCHEMA
    val result     = JsonPayloadExtractor.extractJsonNode(bytes, schema)

    result.value.value.get("name").asText() shouldBe "test"
  }

  test("handle ByteBuffer with schema") {
    val jsonString = """{"name": "test"}"""
    val byteBuffer = ByteBuffer.wrap(jsonString.getBytes("UTF-8"))
    val schema     = Schema.BYTES_SCHEMA
    val result     = JsonPayloadExtractor.extractJsonNode(byteBuffer, schema)

    result.value.value.get("name").asText() shouldBe "test"
  }

  test("handle Struct with schema") {
    val schema = SchemaBuilder.struct()
      .field("name", Schema.STRING_SCHEMA)
      .field("age", Schema.INT32_SCHEMA)
      .build()

    val struct = new Struct(schema)
      .put("name", "John")
      .put("age", 30)

    val result = JsonPayloadExtractor.extractJsonNode(struct, schema)

    val jsonNode = result.value.value
    jsonNode.get("name").asText() shouldBe "John"
    jsonNode.get("age").asInt() shouldBe 30
  }

  test("handle Map without schema") {
    val javaMap = new JHashMap[String, Any]()
    javaMap.put("name", "test")
    javaMap.put("value", Integer.valueOf(42))

    val result = JsonPayloadExtractor.extractJsonNode(javaMap, null)

    val jsonNode = result.value.value
    jsonNode.get("name").asText() shouldBe "test"
    jsonNode.get("value").asInt() shouldBe 42
  }

  test("handle nested Map without schema") {
    val innerMap = new JHashMap[String, Any]()
    innerMap.put("age", Integer.valueOf(25))
    innerMap.put("city", "New York")

    val outerMap = new JHashMap[String, Any]()
    outerMap.put("name", "test")
    outerMap.put("details", innerMap)

    val result = JsonPayloadExtractor.extractJsonNode(outerMap, null)

    val jsonNode = result.value.value
    jsonNode.get("name").asText() shouldBe "test"
    jsonNode.get("details").get("age").asInt() shouldBe 25
    jsonNode.get("details").get("city").asText() shouldBe "New York"
  }

  test("handle string JSON without schema") {
    val jsonString = """{"name": "test"}"""
    val result     = JsonPayloadExtractor.extractJsonNode(jsonString, null)

    result.value.value.get("name").asText() shouldBe "test"
  }

  test("handle byte array without schema") {
    val jsonString = """{"name": "test"}"""
    val bytes      = jsonString.getBytes("UTF-8")
    val result     = JsonPayloadExtractor.extractJsonNode(bytes, null)

    result.value.value.get("name").asText() shouldBe "test"
  }

  test("handle complex nested JSON") {
    val jsonString = """
      {
        "name": "test",
        "details": {
          "age": 30,
          "address": {
            "street": "123 Main St",
            "city": "Test City"
          },
          "hobbies": ["reading", "coding"]
        }
      }
    """
    val result     = JsonPayloadExtractor.extractJsonNode(jsonString, Schema.STRING_SCHEMA)

    val jsonNode = result.value.value
    jsonNode.get("name").asText() shouldBe "test"
    jsonNode.get("details").get("age").asInt() shouldBe 30
    jsonNode.get("details").get("address").get("street").asText() shouldBe "123 Main St"
    jsonNode.get("details").get("hobbies").get(0).asText() shouldBe "reading"
  }

  test("handle boolean value type") {
    val result = JsonPayloadExtractor.extractJsonNode(true, Schema.BOOLEAN_SCHEMA)

    result.value.value.asBoolean() shouldBe true
  }

  test("handle int value type") {
    val result = JsonPayloadExtractor.extractJsonNode(42, null)

    result.value.value.asInt() shouldBe 42
  }

  test("handle invalid type for schema") {
    val result = JsonPayloadExtractor.extractJsonNode(42, Schema.STRING_SCHEMA)

    result.left.value should include("Expected string but got")
  }

  test("handle invalid struct type") {
    val schema = SchemaBuilder.struct()
      .field("name", Schema.STRING_SCHEMA)
      .build()

    val result = JsonPayloadExtractor.extractJsonNode("not a struct", schema)

    result.left.value should include("Expected Struct but got")
  }

  test("handle invalid bytes type") {
    val result = JsonPayloadExtractor.extractJsonNode("not bytes", Schema.BYTES_SCHEMA)
    result.left.value should include("Expected byte array or ByteBuffer")
  }
}
