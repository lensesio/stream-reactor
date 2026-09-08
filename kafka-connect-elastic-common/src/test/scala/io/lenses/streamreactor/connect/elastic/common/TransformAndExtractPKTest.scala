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

import io.lenses.kcql.targettype.StaticTargetType
import io.lenses.kcql.targettype.TargetType
import io.lenses.sql.Field
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.header.ConnectHeaders

class TransformAndExtractPKTest extends AnyFunSuite with Matchers {

  // TransformAndExtractPK never reads KcqlValues.targetType — it's only consulted by
  // CreateIndex, which this test file never exercises — so a placeholder is fine. KcqlValues has
  // no default for targetType precisely so a real caller (JsonBulkWriter) cannot forget to supply
  // one; see KcqlValues's scaladoc for why a null default was unsafe.
  private val unusedTargetType: TargetType = new StaticTargetType("unused-in-this-test")

  def createKcqlValues(fields: Seq[(String, String, Vector[String])]): KcqlValues = {
    val fieldObjects = fields.map {
      case (name, alias, parents) => Field(name, alias, parents)
    }
    KcqlValues(
      fields               = fieldObjects,
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq.empty,
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )
  }

  test("should return None and empty Seq when value is null") {
    val result = TransformAndExtractPK.apply(
      kcqlValues    = createKcqlValues(Seq.empty),
      schema        = null,
      value         = null,
      withStructure = false,
      keySchema     = null,
      key           = null,
      headers       = new ConnectHeaders(),
    )
    result shouldEqual (None, Seq.empty)
  }

  test("should handle valid JSON value and extract primary keys") {
    val jsonString = """{"field1": "value1", "field2": 2}"""
    val value      = jsonString
    val schema     = Schema.STRING_SCHEMA

    val keyJsonString = """{"keyField": "keyValue"}"""
    val key           = keyJsonString
    val keySchema     = Schema.STRING_SCHEMA

    val headers = new ConnectHeaders()
    headers.addString("headerKey", "headerValue")

    val kcqlValues = KcqlValues(
      fields = Seq(
        Field(name = "field1", alias = "field1", parents = Vector.empty),
        Field(name = "field2", alias = "field2", parents = Vector.empty),
      ),
      ignoredFields = Seq.empty,
      primaryKeysPath = Seq(
        Vector("_value", "field1"),
        Vector("_key", "keyField"),
        Vector("_header", "headerKey"),
      ),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val result = TransformAndExtractPK.apply(
      kcqlValues    = kcqlValues,
      schema        = schema,
      value         = value,
      withStructure = false,
      keySchema     = keySchema,
      key           = key,
      headers       = headers,
    )

    val (transformedJsonOpt, primaryKeys) = result

    transformedJsonOpt shouldBe defined
    val transformedJson = transformedJsonOpt.get

    transformedJson.get("field1").asText() shouldEqual "value1"
    transformedJson.get("field2").asInt() shouldEqual 2

    primaryKeys should have size 3
    primaryKeys(0) shouldEqual "value1"
    primaryKeys(1) shouldEqual "keyValue"
    primaryKeys(2) shouldEqual "headerValue"
  }

  test("should throw exception when header is missing") {
    val jsonString = """{"field1": "value1"}"""
    val value      = jsonString
    val schema     = Schema.STRING_SCHEMA

    val headers = new ConnectHeaders()

    val kcqlValues = KcqlValues(
      fields = Seq(
        Field(name = "field1", alias = "field1", parents = Vector.empty),
      ),
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_header", "missingHeader")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val exception = intercept[IllegalArgumentException] {
      TransformAndExtractPK.apply(
        kcqlValues    = kcqlValues,
        schema        = schema,
        value         = value,
        withStructure = false,
        keySchema     = null,
        key           = null,
        headers       = headers,
      )
    }

    exception.getMessage should include("Header with key 'missingHeader' not found")
  }

  test("should extract primary key from Struct value") {
    val schema = SchemaBuilder.struct()
      .field("field1", Schema.STRING_SCHEMA)
      .field("field2", Schema.INT32_SCHEMA)
      .build()

    val struct = new Struct(schema)
      .put("field1", "value1")
      .put("field2", 2)

    val value = struct

    val kcqlValues = KcqlValues(
      fields = Seq(
        Field(name = "field1", alias = "field1", parents = Vector.empty),
        Field(name = "field2", alias = "field2", parents = Vector.empty),
      ),
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_value", "field1")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val result = TransformAndExtractPK.apply(
      kcqlValues    = kcqlValues,
      schema        = schema,
      value         = value,
      withStructure = false,
      keySchema     = null,
      key           = null,
      headers       = new ConnectHeaders(),
    )

    val (transformedJsonOpt, primaryKeys) = result

    transformedJsonOpt shouldBe defined
    val transformedJson = transformedJsonOpt.get

    transformedJson.get("field1").asText() shouldEqual "value1"
    transformedJson.get("field2").asInt() shouldEqual 2

    primaryKeys should have size 1
    primaryKeys(0) shouldEqual "value1"
  }

  test("should use the primary key value when the field path is just _key and the key payload is a primitive LONG") {
    val key       = 123L
    val keySchema = Schema.INT64_SCHEMA

    val kcqlValues = KcqlValues(
      fields               = Seq.empty,
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_key")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val schema = SchemaBuilder.struct()
      .field("field1", Schema.STRING_SCHEMA)
      .field("field2", Schema.INT32_SCHEMA)
      .build()

    val struct = new Struct(schema)
      .put("field1", "value1")
      .put("field2", 2)

    val result = TransformAndExtractPK.apply(
      kcqlValues    = kcqlValues,
      schema        = schema,
      value         = struct,
      withStructure = false,
      keySchema     = keySchema,
      key           = key,
      headers       = new ConnectHeaders(),
    )

    val (_, primaryKeys) = result
    primaryKeys should have size 1
    primaryKeys(0) shouldEqual 123L
  }

  test("should use the primary key value when the field path is just _key and the key payload is a primitive STRING") {
    val key       = "keyValue"
    val keySchema = Schema.STRING_SCHEMA

    val kcqlValues = KcqlValues(
      fields               = Seq.empty,
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_key")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val schema = SchemaBuilder.struct()
      .field("field1", Schema.STRING_SCHEMA)
      .field("field2", Schema.INT32_SCHEMA)
      .build()

    val struct = new Struct(schema)
      .put("field1", "value1")
      .put("field2", 2)

    val result = TransformAndExtractPK.apply(
      kcqlValues    = kcqlValues,
      schema        = schema,
      value         = struct,
      withStructure = false,
      keySchema     = keySchema,
      key           = key,
      headers       = new ConnectHeaders(),
    )

    val (_, primaryKeys) = result
    primaryKeys should have size 1
    primaryKeys(0) shouldEqual "keyValue"
  }

  test("fail when the PK path uses _key.a when the key is only a primitive STRING") {
    val key       = "keyValue"
    val keySchema = Schema.STRING_SCHEMA

    val kcqlValues = KcqlValues(
      fields               = Seq.empty,
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_key", "a")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val schema = SchemaBuilder.struct()
      .field("field1", Schema.STRING_SCHEMA)
      .field("field2", Schema.INT32_SCHEMA)
      .build()

    val struct = new Struct(schema)
      .put("field1", "value1")
      .put("field2", 2)

    val exception = the[IllegalArgumentException] thrownBy {
      TransformAndExtractPK.apply(
        kcqlValues    = kcqlValues,
        schema        = schema,
        value         = struct,
        withStructure = false,
        keySchema     = keySchema,
        key           = key,
        headers       = new ConnectHeaders(),
      )
    }

    exception.getMessage should include("Invalid field selection for '_key.a'")
  }

  test("fail when primary key path involves a missing header key") {
    val jsonString = """{"field1": "value1"}"""
    val schema     = Schema.STRING_SCHEMA

    val headers = new ConnectHeaders()
    headers.addString("headerKey", "headerValue")

    val kcqlValues = KcqlValues(
      fields = Seq(
        Field(name = "field1", alias = "field1", parents = Vector.empty),
      ),
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_header", "missingHeader")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val exception = intercept[IllegalArgumentException] {
      TransformAndExtractPK.apply(
        kcqlValues    = kcqlValues,
        schema        = schema,
        value         = jsonString,
        withStructure = false,
        keySchema     = Schema.STRING_SCHEMA,
        key           = """{"keyField": "keyValue"}""",
        headers       = headers,
      )
    }

    exception.getMessage should include("Header with key 'missingHeader' not found")
  }

  test("fail when the header key has a null value") {
    val jsonString = """{"field1": "value1"}"""
    val schema     = Schema.STRING_SCHEMA

    val headers = new ConnectHeaders()
    headers.addString("headerKey", null)

    val kcqlValues = KcqlValues(
      fields = Seq(
        Field(name = "field1", alias = "field1", parents = Vector.empty),
      ),
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_header", "headerKey")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val exception = intercept[IllegalArgumentException] {
      TransformAndExtractPK.apply(
        kcqlValues    = kcqlValues,
        schema        = schema,
        value         = jsonString,
        withStructure = false,
        keySchema     = Schema.STRING_SCHEMA,
        key           = """{"keyField": "keyValue"}""",
        headers       = headers,
      )
    }

    exception.getMessage should include("Header 'headerKey' has a null value")
  }

  test("fail when the primary key path uses a nested header key") {
    val jsonString = """{"field1": "value1"}"""
    val schema     = Schema.STRING_SCHEMA

    val headers = new ConnectHeaders()
    headers.addString("headerKey", "headerValue")

    val kcqlValues = KcqlValues(
      fields = Seq(
        Field(name = "field1", alias = "field1", parents = Vector.empty),
      ),
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_header", "headerKey", "nonexistentField")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val exception = intercept[IllegalArgumentException] {
      TransformAndExtractPK.apply(
        kcqlValues    = kcqlValues,
        schema        = schema,
        value         = jsonString,
        withStructure = false,
        keySchema     = Schema.STRING_SCHEMA,
        key           = """{"keyField": "keyValue"}""",
        headers       = headers,
      )
    }

    exception.getMessage shouldBe "Invalid field selection for '_header.headerKey.nonexistentField'. Headers lookup only supports single-level keys. Nested header keys are not supported."
  }

  test("fail when primary key path uses a key path which does not exist") {
    val jsonString = """{"field1": "value1"}"""
    val schema     = Schema.STRING_SCHEMA

    val headers = new ConnectHeaders()
    headers.addString("headerKey", "headerValue")

    val kcqlValues = KcqlValues(
      fields = Seq(
        Field(name = "field1", alias = "field1", parents = Vector.empty),
      ),
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_key", "nonexistentField")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val exception = intercept[IllegalArgumentException] {
      TransformAndExtractPK.apply(
        kcqlValues    = kcqlValues,
        schema        = schema,
        value         = jsonString,
        withStructure = false,
        keySchema     = Schema.STRING_SCHEMA,
        key           = """{"keyField": "keyValue"}""",
        headers       = headers,
      )
    }

    exception.getMessage shouldBe "Invalid field selection for '_key.nonexistentField'. Can't find nonexistentField field. Field found are:keyField"
  }

  // ---- Parity-contract additions -------------------------------------------------------

  test("header PK with non-string header value raises exact message \"Header '<name>' is not a string\"") {
    val jsonString = """{"field1": "value1"}"""
    val schema     = Schema.STRING_SCHEMA

    val headers = new ConnectHeaders()
    headers.addInt("numericHeader", 42)

    val kcqlValues = KcqlValues(
      fields               = Seq(Field(name = "field1", alias = "field1", parents = Vector.empty)),
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_header", "numericHeader")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val ex = intercept[IllegalArgumentException] {
      TransformAndExtractPK.apply(
        kcqlValues    = kcqlValues,
        schema        = schema,
        value         = jsonString,
        withStructure = false,
        keySchema     = null,
        key           = null,
        headers       = headers,
      )
    }
    ex.getMessage shouldBe "Header 'numericHeader' is not a string"
  }

  test("multi-valued header PK uses LAST occurrence (headers.lastWithName semantics)") {
    val jsonString = """{"field1": "value1"}"""
    val schema     = Schema.STRING_SCHEMA

    val headers = new ConnectHeaders()
    headers.addString("h", "first")
    headers.addString("h", "second")
    headers.addString("h", "last")

    val kcqlValues = KcqlValues(
      fields               = Seq(Field(name = "field1", alias = "field1", parents = Vector.empty)),
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_header", "h")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    val (_, pks) = TransformAndExtractPK.apply(
      kcqlValues    = kcqlValues,
      schema        = schema,
      value         = jsonString,
      withStructure = false,
      keySchema     = null,
      key           = null,
      headers       = headers,
    )
    pks.head shouldBe "last"
  }

  // NullPointerException is NonFatal (only VM errors/InterruptedException are not), so under the
  // writer-stage diversion in JsonBulkWriter.insert this is divertible under
  // bulk.item.errors=drop/dlq like any other extraction failure, not just under fail — see
  // JsonBulkWriterTest for the writer-level assertion.
  test("null headers reference raises NullPointerException (verbatim ES7 behaviour)") {
    val jsonString = """{"field1": "value1"}"""
    val schema     = Schema.STRING_SCHEMA

    val kcqlValues = KcqlValues(
      fields               = Seq(Field(name = "field1", alias = "field1", parents = Vector.empty)),
      ignoredFields        = Seq.empty,
      primaryKeysPath      = Seq(Vector("_header", "h")),
      behaviorOnNullValues = NullValueBehavior.FAIL,
      targetType           = unusedTargetType,
    )

    intercept[NullPointerException] {
      TransformAndExtractPK.apply(
        kcqlValues    = kcqlValues,
        schema        = schema,
        value         = jsonString,
        withStructure = false,
        keySchema     = null,
        key           = null,
        headers       = null,
      )
    }
  }

}
