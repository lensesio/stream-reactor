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
package io.lenses.streamreactor.connect.http.sink.tpl.substitutions

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import java.util.Base64
import scala.jdk.CollectionConverters._

class HeaderValueConverterTest extends AnyFunSuiteLike with Matchers {

  test("should return empty string for null value") {
    HeaderValueConverter.headerValueToString(null, Schema.STRING_SCHEMA) shouldBe ""
  }

  test("should convert string value") {
    HeaderValueConverter.headerValueToString("foo", Schema.STRING_SCHEMA) shouldBe "foo"
  }

  test("should convert int value") {
    HeaderValueConverter.headerValueToString(42, Schema.INT32_SCHEMA) shouldBe "42"
  }

  test("should convert boolean value") {
    HeaderValueConverter.headerValueToString(true, Schema.BOOLEAN_SCHEMA) shouldBe "true"
  }

  test("should base64 encode byte array") {
    val arr      = Array[Byte](1, 2, 3, 4)
    val expected = Base64.getEncoder.encodeToString(arr)
    HeaderValueConverter.headerValueToString(arr, Schema.BYTES_SCHEMA) shouldBe expected
  }

  test("should serialize struct to JSON") {
    val structSchema = SchemaBuilder.struct().name("TestStruct")
      .field("f1", Schema.STRING_SCHEMA)
      .field("f2", Schema.INT32_SCHEMA)
      .build()
    val struct = new Struct(structSchema)
      .put("f1", "bar")
      .put("f2", 99)
    val json = HeaderValueConverter.headerValueToString(struct, structSchema)
    json should include("f1")
    json should include("bar")
    json should include("f2")
    json should include("99")
  }

  test("should serialize map to JSON") {
    val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build()
    val map       = Map("a" -> 1, "b" -> 2).asJava
    val json      = HeaderValueConverter.headerValueToString(map, mapSchema)
    json should include("a")
    json should include("1")
    json should include("b")
    json should include("2")
  }

  test("should serialize array to JSON") {
    val arrSchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build()
    val arr       = java.util.Arrays.asList("x", "y", "z")
    val json      = HeaderValueConverter.headerValueToString(arr, arrSchema)
    json should include("x")
    json should include("y")
    json should include("z")
  }

  test("should serialize java.util.Map to JSON") {
    val mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build()
    val javaMap   = new java.util.HashMap[String, Int]()
    javaMap.put("foo", 123)
    javaMap.put("bar", 456)
    val json = HeaderValueConverter.headerValueToString(javaMap, mapSchema)
    json should include("foo")
    json should include("123")
    json should include("bar")
    json should include("456")
  }

  test("should fallback to toString for unknown type") {
    val custom       = new Object { override def toString = "custom!" }
    val customSchema = SchemaBuilder.float64().build() // not handled specially
    HeaderValueConverter.headerValueToString(custom, customSchema) shouldBe "custom!"
  }

  test("should fallback to toString if schema is missing") {
    val custom = new Object { override def toString = "custom!" }
    HeaderValueConverter.headerValueToString(custom, null) shouldBe "custom!"
  }

  test("should fallback to toString if value is present but schema is None") {
    val custom = new Object { override def toString = "custom!" }
    HeaderValueConverter.headerValueToString(custom, null) shouldBe "custom!"
  }

  test("should fallback to toString if schema type is None (Some(v), Some(_), None)") {
    // Simulate a schema with a null type (not possible with real Schema, so use a mock)
    val mockSchema = new org.apache.kafka.connect.data.Schema {
      override def `type`: Schema.Type = null
      // All other methods can throw or return null
      override def name():         String                                              = null
      override def version():      Integer                                             = null
      override def doc():          String                                              = null
      override def isOptional:     Boolean                                             = false
      override def defaultValue(): Object                                              = null
      override def parameters():   java.util.Map[String, String]                       = null
      override def fields():       java.util.List[org.apache.kafka.connect.data.Field] = null
      override def field(name: String): org.apache.kafka.connect.data.Field = null
      override def keySchema():   Schema = null
      override def valueSchema(): Schema = null
      override def schema():      Schema = this
    }
    HeaderValueConverter.headerValueToString("foo", mockSchema) shouldBe "foo"
  }
}
