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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data._
import scala.jdk.CollectionConverters._
import org.apache.kafka.connect.errors.ConnectException

class PrimaryKeyExtractorTest extends AnyFunSuite with Matchers {
  def parseJson(jsonString: String): JsonNode = {
    val mapper = new ObjectMapper()
    mapper.readTree(jsonString)
  }

  test("extract should retrieve a primitive value from JsonNode") {
    val jsonString = """{"field": "value"}"""
    val jsonNode   = parseJson(jsonString)
    val path       = Vector("field")

    val result = PrimaryKeyExtractor.extract(jsonNode, path)
    result shouldEqual "value"
  }

  test("extract should retrieve a nested primitive value from JsonNode") {
    val jsonString = """{"parent": {"child": 123}}"""
    val jsonNode   = parseJson(jsonString)
    val path       = Vector("parent", "child")

    val result = PrimaryKeyExtractor.extract(jsonNode, path)
    result shouldEqual 123
  }

  test("extract should throw exception if path does not exist in JsonNode") {
    val jsonString = """{"field": "value"}"""
    val jsonNode   = parseJson(jsonString)
    val path       = Vector("nonexistent")

    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(jsonNode, path)
    }
    exception.getMessage should include("Can't find nonexistent field")
  }

  test("extract should throw exception if path leads to non-primitive field in JsonNode") {
    val jsonString = """{"field": {"subfield": "value"}}"""
    val jsonNode   = parseJson(jsonString)
    val path       = Vector("field")

    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(jsonNode, path)
    }
    exception.getMessage should include("The path is not resolving to a primitive field")
  }

  test("extract should throw exception when encountering an array in JsonNode") {
    val jsonString = """{"field": [1, 2, 3]}"""
    val jsonNode   = parseJson(jsonString)
    val path       = Vector("field")

    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(jsonNode, path)
    }
    exception.getMessage should include("The path is involving an array structure")
  }

  test("extract should handle various primitive types in JsonNode") {
    val jsonString =
      """
        |{
        |  "stringField": "text",
        |  "intField": 42,
        |  "floatField": 3.14,
        |  "booleanField": true,
        |  "nullField": null
        |}
        |""".stripMargin
    val jsonNode = parseJson(jsonString)

    PrimaryKeyExtractor.extract(jsonNode, Vector("stringField")) shouldEqual "text"
    PrimaryKeyExtractor.extract(jsonNode, Vector("intField")) shouldEqual 42
    PrimaryKeyExtractor.extract(jsonNode, Vector("floatField")) shouldEqual 3.14
    PrimaryKeyExtractor.extract(jsonNode, Vector("booleanField")) shouldEqual true
    PrimaryKeyExtractor.extract(jsonNode, Vector("nullField")).asInstanceOf[AnyRef] shouldBe null
  }

  test("extract should retrieve a primitive value from Struct") {
    val schema = SchemaBuilder.struct()
      .field("field", Schema.STRING_SCHEMA)
      .build()

    val struct = new Struct(schema)
      .put("field", "value")

    val path = Vector("field")

    val result = PrimaryKeyExtractor.extract(struct, path)
    result shouldEqual "value"
  }

  test("extract should retrieve a nested primitive value from Struct") {
    val nestedSchema = SchemaBuilder.struct()
      .field("child", Schema.INT32_SCHEMA)
      .build()

    val schema = SchemaBuilder.struct()
      .field("parent", nestedSchema)
      .build()

    val nestedStruct = new Struct(nestedSchema)
      .put("child", 123)

    val struct = new Struct(schema)
      .put("parent", nestedStruct)

    val path = Vector("parent", "child")

    val result = PrimaryKeyExtractor.extract(struct, path)
    result shouldEqual 123
  }

  test("extract should throw exception if path does not exist in Struct") {
    val schema = SchemaBuilder.struct()
      .field("field", Schema.STRING_SCHEMA)
      .build()

    val struct = new Struct(schema)
      .put("field", "value")

    val path = Vector("nonexistent")

    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(struct, path)
    }
    exception.getMessage should include("Couldn't find field 'nonexistent'")
  }

  test("extract should throw exception if field in Struct is null") {
    val schema = SchemaBuilder.struct()
      .field("field", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val struct = new Struct(schema)
      .put("field", null)

    val path = Vector("field")

    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(struct, path)
    }
    exception.getMessage should include("Field 'field' is null")
  }

  test("extract should handle various primitive types in Struct") {
    val schema = SchemaBuilder.struct()
      .field("stringField", Schema.STRING_SCHEMA)
      .field("intField", Schema.INT32_SCHEMA)
      .field("floatField", Schema.FLOAT32_SCHEMA)
      .field("booleanField", Schema.BOOLEAN_SCHEMA)
      .build()

    val struct = new Struct(schema)
      .put("stringField", "text")
      .put("intField", 42)
      .put("floatField", 3.14f)
      .put("booleanField", true)

    PrimaryKeyExtractor.extract(struct, Vector("stringField")) shouldEqual "text"
    PrimaryKeyExtractor.extract(struct, Vector("intField")) shouldEqual 42
    PrimaryKeyExtractor.extract(struct, Vector("floatField")) shouldEqual 3.14f
    PrimaryKeyExtractor.extract(struct, Vector("booleanField")) shouldEqual true
  }

  test("extract should handle logical types in Struct (e.g., Date, Time, Timestamp)") {
    val dateSchema      = Date.SCHEMA
    val timeSchema      = Time.SCHEMA
    val timestampSchema = Timestamp.SCHEMA

    val schema = SchemaBuilder.struct()
      .field("dateField", dateSchema)
      .field("timeField", timeSchema)
      .field("timestampField", timestampSchema)
      .build()

    val dateValue      = new java.util.Date(1627843200000L) // Aug 1, 2021
    val timeValue      = new java.util.Date(3600000L)       // 1 hour in milliseconds
    val timestampValue = new java.util.Date(1627843200000L)

    val struct = new Struct(schema)
      .put("dateField", dateValue)
      .put("timeField", timeValue)
      .put("timestampField", timestampValue)

    PrimaryKeyExtractor.extract(struct, Vector("dateField")) shouldEqual dateValue
    PrimaryKeyExtractor.extract(struct, Vector("timeField")) shouldEqual timeValue
    PrimaryKeyExtractor.extract(struct, Vector("timestampField")) shouldEqual timestampValue
  }

  test("extract should throw exception when encountering unsupported schema type in Struct") {
    val schema = SchemaBuilder.struct()
      .field("mapField", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA))
      .build()

    val mapValue = Map("key" -> 123).asJava

    val struct = new Struct(schema)
      .put("mapField", mapValue)

    val path = Vector("mapField")

    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(struct, path)
    }
    exception.getMessage should include("It doesn't resolve to a primitive field")
  }

  test("extract should traverse nested maps in Struct") {
    val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build()

    val schema = SchemaBuilder.struct()
      .field("mapField", mapSchema)
      .build()

    val mapValue = Map("innerKey" -> 123).asJava

    val struct = new Struct(schema)
      .put("mapField", mapValue)

    val path = Vector("mapField", "innerKey")

    val result = PrimaryKeyExtractor.extract(struct, path)
    result shouldEqual 123
  }

  test("extract should throw exception when encountering unsupported schema type") {
    val schema = SchemaBuilder.struct()
      .field("arrayField", SchemaBuilder.array(Schema.INT32_SCHEMA))
      .build()

    val arrayValue = List(1, 2, 3).asJava

    val struct = new Struct(schema)
      .put("arrayField", arrayValue)

    val path = Vector("arrayField")

    val exception = intercept[ConnectException] {
      PrimaryKeyExtractor.extract(struct, path)
    }
    exception.getMessage should include("ARRAY is not a recognized schema")
  }
}
