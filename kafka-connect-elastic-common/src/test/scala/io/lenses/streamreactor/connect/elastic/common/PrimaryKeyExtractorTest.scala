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
package io.lenses.streamreactor.connect.elastic.common

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.ConnectException

import scala.jdk.CollectionConverters._

class PrimaryKeyExtractorTest extends AnyFunSuite with Matchers {

  def parseJson(jsonString: String): JsonNode = {
    val mapper = new ObjectMapper()
    mapper.readTree(jsonString)
  }

  test("extract should retrieve a primitive value from JsonNode") {
    val jsonNode = parseJson("""{"field": "value"}""")
    PrimaryKeyExtractor.extract(jsonNode, Vector("field")) shouldEqual "value"
  }

  test("extract should retrieve a nested primitive value from JsonNode") {
    val jsonNode = parseJson("""{"parent": {"child": 123}}""")
    PrimaryKeyExtractor.extract(jsonNode, Vector("parent", "child")) shouldEqual 123
  }

  test("extract should throw exception if path does not exist in JsonNode") {
    val jsonNode = parseJson("""{"field": "value"}""")
    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(jsonNode, Vector("nonexistent"))
    }
    exception.getMessage should include("Can't find nonexistent field")
  }

  test("extract should throw exception if path leads to non-primitive field in JsonNode") {
    val jsonNode = parseJson("""{"field": {"subfield": "value"}}""")
    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(jsonNode, Vector("field"))
    }
    exception.getMessage should include("The path is not resolving to a primitive field")
  }

  test("extract should throw exception when encountering an array in JsonNode") {
    val jsonNode = parseJson("""{"field": [1, 2, 3]}""")
    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(jsonNode, Vector("field"))
    }
    exception.getMessage should include("The path is involving an array structure")
  }

  test("extract should handle various primitive types in JsonNode") {
    val jsonNode = parseJson(
      """{"stringField": "text", "intField": 42, "floatField": 3.14, "booleanField": true, "nullField": null}""",
    )

    PrimaryKeyExtractor.extract(jsonNode, Vector("stringField")) shouldEqual "text"
    PrimaryKeyExtractor.extract(jsonNode, Vector("intField")) shouldEqual 42
    PrimaryKeyExtractor.extract(jsonNode, Vector("floatField")) shouldEqual 3.14
    PrimaryKeyExtractor.extract(jsonNode, Vector("booleanField")) shouldEqual true
  }

  // A PK resolving to a JSON null used to be silently returned as the Scala `null`, which
  // `pks.mkString` in JsonBulkWriter then stringified to the literal id "null" — a bogus,
  // collision-prone document id, indistinguishable from a genuine extraction failure. Throwing
  // here instead makes it a divertible writer-stage failure like any other bad PK, and matches
  // the Struct overload below, which already throws "Field 'x' is null" for the same condition.
  test("extract should throw exception when a PK field resolves to a JSON null") {
    val jsonNode = parseJson("""{"nullField": null}""")
    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(jsonNode, Vector("nullField"))
    }
    exception.getMessage should include("cannot be used as a primary key value")
  }

  test("extract should throw exception when a PK field resolves to a missing JSON node") {
    val jsonNode = parseJson("""{"otherField": "value"}""")
    // Missing a field on an ObjectNode already throws ("Can't find ... field") before ever
    // reaching a MissingNode — MissingNode is reachable via Jackson's own path traversal
    // (e.g. `node.path("missing")`), not `node.get`, so exercise it directly.
    val missing = jsonNode.path("doesNotExist")
    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(missing, Vector.empty)
    }
    exception.getMessage should include("cannot be used as a primary key value")
  }

  test("extract should retrieve a primitive value from Struct") {
    val schema = SchemaBuilder.struct().field("field", Schema.STRING_SCHEMA).build()
    val struct = new Struct(schema).put("field", "value")
    PrimaryKeyExtractor.extract(struct, Vector("field")) shouldEqual "value"
  }

  test("extract should retrieve a nested primitive value from Struct") {
    val nestedSchema = SchemaBuilder.struct().field("child", Schema.INT32_SCHEMA).build()
    val schema       = SchemaBuilder.struct().field("parent", nestedSchema).build()
    val nestedStruct = new Struct(nestedSchema).put("child", 123)
    val struct       = new Struct(schema).put("parent", nestedStruct)
    PrimaryKeyExtractor.extract(struct, Vector("parent", "child")) shouldEqual 123
  }

  test("extract should throw exception if path does not exist in Struct") {
    val schema = SchemaBuilder.struct().field("field", Schema.STRING_SCHEMA).build()
    val struct = new Struct(schema).put("field", "value")
    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(struct, Vector("nonexistent"))
    }
    exception.getMessage should include("Couldn't find field 'nonexistent'")
  }

  test("extract should throw exception if field in Struct is null") {
    val schema = SchemaBuilder.struct().field("field", Schema.OPTIONAL_STRING_SCHEMA).build()
    val struct = new Struct(schema).put("field", null)
    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(struct, Vector("field"))
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

  test("extract should handle logical types in Struct (Date, Time, Timestamp)") {
    val schema = SchemaBuilder.struct()
      .field("dateField", Date.SCHEMA)
      .field("timeField", Time.SCHEMA)
      .field("timestampField", Timestamp.SCHEMA)
      .build()

    val dateValue      = new java.util.Date(1627843200000L)
    val timeValue      = new java.util.Date(3600000L)
    val timestampValue = new java.util.Date(1627843200000L)

    val struct = new Struct(schema)
      .put("dateField", dateValue)
      .put("timeField", timeValue)
      .put("timestampField", timestampValue)

    PrimaryKeyExtractor.extract(struct, Vector("dateField")) shouldEqual dateValue
    PrimaryKeyExtractor.extract(struct, Vector("timeField")) shouldEqual timeValue
    PrimaryKeyExtractor.extract(struct, Vector("timestampField")) shouldEqual timestampValue
  }

  test("extract should throw exception when encountering unsupported schema type in Struct (MAP)") {
    val schema =
      SchemaBuilder.struct().field("mapField", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA)).build()
    val mapValue = Map("key" -> 123).asJava
    val struct   = new Struct(schema).put("mapField", mapValue)

    val exception = intercept[IllegalArgumentException] {
      PrimaryKeyExtractor.extract(struct, Vector("mapField"))
    }
    exception.getMessage should include("It doesn't resolve to a primitive field")
  }

  test("extract should traverse nested maps in Struct") {
    val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build()
    val schema    = SchemaBuilder.struct().field("mapField", mapSchema).build()
    val mapValue  = Map("innerKey" -> 123).asJava
    val struct    = new Struct(schema).put("mapField", mapValue)
    PrimaryKeyExtractor.extract(struct, Vector("mapField", "innerKey")) shouldEqual 123
  }

  test("extract should throw ConnectException when encountering ARRAY schema type") {
    val schema     = SchemaBuilder.struct().field("arrayField", SchemaBuilder.array(Schema.INT32_SCHEMA)).build()
    val arrayValue = List(1, 2, 3).asJava
    val struct     = new Struct(schema).put("arrayField", arrayValue)

    val exception = intercept[ConnectException] {
      PrimaryKeyExtractor.extract(struct, Vector("arrayField"))
    }
    exception.getMessage should include("ARRAY is not a recognized schema")
  }
}
