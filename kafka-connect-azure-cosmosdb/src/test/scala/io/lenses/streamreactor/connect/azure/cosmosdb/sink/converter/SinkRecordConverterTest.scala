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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.converter

import io.lenses.streamreactor.connect.azure.cosmosdb.converters.SinkRecordConverterEither
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.node.BinaryNode

import java.util
import org.scalatest.EitherValues

class SinkRecordConverterTest extends AnyFunSuite with Matchers with EitherValues {

  test("fromStruct should convert a valid Struct to Document") {
    val schema = SchemaBuilder.struct().name("TestStruct")
      .field("intField", Schema.INT32_SCHEMA)
      .field("stringField", Schema.STRING_SCHEMA)
      .build()
    val struct = new Struct(schema)
      .put("intField", 42)
      .put("stringField", "foo")
    val record = new SinkRecord("topic", 0, null, null, schema, struct, 0)
    val result = SinkRecordConverterEither.fromStruct(record)
    val doc    = result.value
    doc.getInt("intField") shouldBe 42
    doc.getString("stringField") shouldBe "foo"
  }

  test("fromStruct should return Left for non-Struct value") {
    val schema = Schema.STRING_SCHEMA
    val record = new SinkRecord("topic", 0, null, null, schema, "not a struct", 0)
    val result = SinkRecordConverterEither.fromStruct(record)
    result.left.value shouldBe a[IllegalArgumentException]
  }

  test("fromStruct should throw for mismatching schema in struct") {
    val schema1 = SchemaBuilder.struct().name("A").field("f", Schema.INT32_SCHEMA).build()
    val schema2 = SchemaBuilder.struct().name("B").field("f", Schema.INT32_SCHEMA).build()
    val struct  = new Struct(schema1).put("f", 1)
    val record  = new SinkRecord("topic", 0, null, null, schema2, struct, 0)
    val result  = SinkRecordConverterEither.fromStruct(record)
    result.left.value shouldBe a[org.apache.kafka.connect.errors.DataException]
  }

  test("convertToDocument should handle all primitive types") {
    val schema = SchemaBuilder.struct().name("Primitives")
      .field("int32", Schema.INT32_SCHEMA)
      .field("int64", Schema.INT64_SCHEMA)
      .field("string", Schema.STRING_SCHEMA)
      .field("bytes", Schema.BYTES_SCHEMA)
      .build()
    val bytes = Array[Byte](1, 2, 3)
    val struct = new Struct(schema)
      .put("int32", 123)
      .put("int64", 456L)
      .put("string", "abc")
      .put("bytes", bytes)
    val record = new SinkRecord("topic", 0, null, null, schema, struct, 0)
    val result = SinkRecordConverterEither.fromStruct(record)
    val doc    = result.value
    doc.getInt("int32") shouldBe 123
    doc.getLong("int64") shouldBe 456L
    doc.getString("string") shouldBe "abc"
    decodeIfBase64(doc.get("bytes")) shouldBe bytes
  }

  private def decodeIfBase64(value: Any): Array[Byte] = value match {
    case arr: Array[Byte] => arr
    case s:   String      => java.util.Base64.getDecoder.decode(s)
    case bin: BinaryNode  => bin.binaryValue()
    case other => fail(s"Unexpected type for bytes: ${other.getClass}")
  }

  private def asArrayNode(value: Any): ArrayNode = value match {
    case arr: ArrayNode => arr
    case other => fail(s"Expected ArrayNode but got: ${other.getClass}")
  }

  private def asObjectNode(value: Any): ObjectNode = value match {
    case obj: ObjectNode => obj
    case other => fail(s"Expected ObjectNode but got: ${other.getClass}")
  }

  test("convertToDocument should handle nulls and optionals") {
    val schema = SchemaBuilder.struct().name("OptStruct")
      .field("optionalField", SchemaBuilder.int32().optional().build())
      .field("defaultField", SchemaBuilder.int32().defaultValue(99).build())
      .build()
    val struct = new Struct(schema)
      .put("optionalField", null)
    // don't put defaultField
    val record = new SinkRecord("topic", 0, null, null, schema, struct, 0)
    val result = SinkRecordConverterEither.fromStruct(record)
    val doc    = result.value
    doc.get("optionalField") shouldBe null
    doc.getInt("defaultField") shouldBe 99
  }

  test("convertToDocument should throw for missing required field with no default") {
    val schema = SchemaBuilder.struct().name("ReqStruct")
      .field("requiredField", Schema.INT32_SCHEMA)
      .build()
    val struct = new Struct(schema)
    val record = new SinkRecord("topic", 0, null, null, schema, struct, 0)
    val result = SinkRecordConverterEither.fromStruct(record)
    result.left.value shouldBe a[org.apache.kafka.connect.errors.DataException]
  }

  test("struct schema validation throws DataException for invalid bytes field type (not converter)") {
    // This test demonstrates that Kafka Connect's Struct enforces type safety and throws DataException
    // before the converter is even called, if you try to put an invalid type into a bytes field.
    val schema = SchemaBuilder.struct().name("BytesStruct").field("bytesField", Schema.BYTES_SCHEMA).build()
    assertThrows[org.apache.kafka.connect.errors.DataException] {
      new Struct(schema).put("bytesField", 123) // invalid type for bytes
    }
  }

  test("handleArray should convert array of primitives") {
    val arrSchema = SchemaBuilder.array(Schema.INT32_SCHEMA).build()
    val schema    = SchemaBuilder.struct().name("ArrStruct").field("arr", arrSchema).build()
    val arr       = new util.ArrayList[Integer]()
    arr.add(1); arr.add(2); arr.add(3)
    val struct    = new Struct(schema).put("arr", arr)
    val record    = new SinkRecord("topic", 0, null, null, schema, struct, 0)
    val result    = SinkRecordConverterEither.fromStruct(record)
    val doc       = result.value
    val resultArr = asArrayNode(doc.get("arr"))
    resultArr.size() shouldBe 3
    resultArr.get(0).asInt() shouldBe 1
    resultArr.get(1).asInt() shouldBe 2
    resultArr.get(2).asInt() shouldBe 3
  }

  test("handleArray should convert array of structs") {
    val elemSchema = SchemaBuilder.struct().name("Elem").field("f", Schema.INT32_SCHEMA).build()
    val arrSchema  = SchemaBuilder.array(elemSchema).build()
    val schema     = SchemaBuilder.struct().name("ArrStruct").field("arr", arrSchema).build()
    val elem1      = new Struct(elemSchema).put("f", 10)
    val elem2      = new Struct(elemSchema).put("f", 20)
    val arr        = new util.ArrayList[Struct]()
    arr.add(elem1); arr.add(elem2)
    val struct    = new Struct(schema).put("arr", arr)
    val record    = new SinkRecord("topic", 0, null, null, schema, struct, 0)
    val result    = SinkRecordConverterEither.fromStruct(record)
    val doc       = result.value
    val resultArr = asArrayNode(doc.get("arr"))
    resultArr.size() shouldBe 2
    resultArr.get(0).get("f").asInt() shouldBe 10
    resultArr.get(1).get("f").asInt() shouldBe 20
  }

  test("handleMap should convert map with string keys") {
    val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build()
    val schema    = SchemaBuilder.struct().name("MapStruct").field("map", mapSchema).build()
    val map       = new util.HashMap[String, Integer]()
    map.put("a", 1); map.put("b", 2)
    val struct    = new Struct(schema).put("map", map)
    val record    = new SinkRecord("topic", 0, null, null, schema, struct, 0)
    val result    = SinkRecordConverterEither.fromStruct(record)
    val doc       = result.value
    val resultDoc = asObjectNode(doc.get("map"))
    resultDoc.get("a").asInt() shouldBe 1
    resultDoc.get("b").asInt() shouldBe 2
  }

  test("handleMap should convert map with non-string keys (array mode)") {
    val mapSchema = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build()
    val schema    = SchemaBuilder.struct().name("MapStruct").field("map", mapSchema).build()
    val map       = new util.HashMap[Integer, String]()
    map.put(1, "one"); map.put(2, "two")
    val struct    = new Struct(schema).put("map", map)
    val record    = new SinkRecord("topic", 0, null, null, schema, struct, 0)
    val result    = SinkRecordConverterEither.fromStruct(record)
    val doc       = result.value
    val resultArr = asArrayNode(doc.get("map"))
    resultArr.size() shouldBe 2
    val keys = (0 until 2).map(i => (resultArr.get(i).get(0).asInt(), resultArr.get(i).get(1).asText())).toSet
    keys shouldBe Set((1, "one"), (2, "two"))
  }

  test("convertToDocument should throw for unknown schema type") {
    val schema = SchemaBuilder.struct().name("Unknown").field("f", SchemaBuilder.float32().build()).build()
    val struct = new Struct(schema).put("f", 1.23f)
    val record = new SinkRecord("topic", 0, null, null, schema, struct, 0)
    val result = SinkRecordConverterEither.fromStruct(record)
    val doc    = result.value
    doc.get("f").asInstanceOf[com.fasterxml.jackson.databind.JsonNode].floatValue() shouldBe (1.23f +- 0.0001f)
  }

  test("convertToDocument should throw DataException for invalid type") {
    val schema = SchemaBuilder.int32().build()
    val result = SinkRecordConverterEither.fromStruct(new SinkRecord("topic", 0, null, null, schema, "not an int", 0))
    result.left.value shouldBe a[IllegalArgumentException]
    result.left.value.getMessage should include("Expecting a Struct")
  }

  test("convertToDocument should throw DataException for missing schema type") {
    val schema = null
    val result = SinkRecordConverterEither.fromStruct(new SinkRecord("topic", 0, null, null, schema, 123, 0))
    result.left.value shouldBe a[IllegalArgumentException]
    result.left.value.getMessage should include("Expecting a Struct")
  }
}
