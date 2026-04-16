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
package io.lenses.streamreactor.connect.cloud.common.formats.writer.schema

import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.funsuite.AnyFunSuiteLike

class CompatibilitySchemaChangeDetectorTest extends AnyFunSuiteLike {

  private val detector = CompatibilitySchemaChangeDetector

  def createSchema(fields: (String, Schema)*): Schema = {
    val builder = SchemaBuilder.struct()
    fields.foreach { case (name, schema) => builder.field(name, schema) }
    builder.build()
  }

  test("detectSchemaChange returns false for identical schemas") {
    val schema1 = createSchema("field1" -> Schema.STRING_SCHEMA)
    val schema2 = createSchema("field1" -> Schema.STRING_SCHEMA)
    assert(!detector.detectSchemaChange(schema1, schema2))
  }

  test("detectSchemaChange returns true for schemas with different fields") {
    val schema1 = createSchema("field1" -> Schema.STRING_SCHEMA)
    val schema2 = createSchema("field2" -> Schema.STRING_SCHEMA)
    assert(detector.detectSchemaChange(schema1, schema2))
  }

  test("detectSchemaChange returns true for schemas with different field types") {
    val schema1 = createSchema("field1" -> Schema.STRING_SCHEMA)
    val schema2 = createSchema("field1" -> Schema.INT32_SCHEMA)
    assert(detector.detectSchemaChange(schema1, schema2))
  }

  test("detectSchemaChange returns false for compatible schemas with additional fields") {
    val schema1 = createSchema("field1" -> Schema.STRING_SCHEMA)
    val schema2 = createSchema("field1" -> Schema.STRING_SCHEMA, "field2" -> Schema.INT32_SCHEMA)
    assert(!detector.detectSchemaChange(schema1, schema2))
  }

  test("detectSchemaChange returns true for incompatible schemas with removed fields") {
    val schema1 = createSchema("field1" -> Schema.STRING_SCHEMA, "field2" -> Schema.INT32_SCHEMA)
    val schema2 = createSchema("field1" -> Schema.STRING_SCHEMA)
    assert(detector.detectSchemaChange(schema1, schema2))
  }

}
