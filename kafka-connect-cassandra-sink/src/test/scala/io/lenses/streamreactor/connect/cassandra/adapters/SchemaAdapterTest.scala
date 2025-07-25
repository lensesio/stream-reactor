/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cassandra.adapters

import com.datastax.oss.common.sink.record.StructDataMetadata
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.`type`.reflect.GenericType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SchemaAdapterTest extends AnyFunSuite with Matchers {

  private val schema: Schema =
    SchemaBuilder.struct()
      .name("Kafka")
      .field("bigint", Schema.INT64_SCHEMA)
      .field("boolean", Schema.BOOLEAN_SCHEMA)
      .field("double", Schema.FLOAT64_SCHEMA)
      .field("float", Schema.FLOAT32_SCHEMA)
      .field("int", Schema.INT32_SCHEMA)
      .field("smallint", Schema.INT16_SCHEMA)
      .field("text", Schema.STRING_SCHEMA)
      .field("tinyint", Schema.INT8_SCHEMA)
      .field("blob", Schema.BYTES_SCHEMA)
      .field("map", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
      .field(
        "mapnested",
        SchemaBuilder.map(
          Schema.STRING_SCHEMA,
          SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.STRING_SCHEMA).build(),
        ).build(),
      )
      .field("list", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
      .field(
        "listnested",
        SchemaBuilder.array(SchemaBuilder.array(Schema.INT32_SCHEMA).build()).build(),
      )
      .build()

  private val metadata = new StructDataMetadata(SchemaAdapter.from(schema))

  private def getFieldType(field: String): GenericType[_] =
    metadata.getFieldType(field, DataTypes.TEXT)

  test("should translate field types") {
    getFieldType("bigint") shouldBe GenericType.LONG
    getFieldType("boolean") shouldBe GenericType.BOOLEAN
    getFieldType("double") shouldBe GenericType.DOUBLE
    getFieldType("float") shouldBe GenericType.FLOAT
    getFieldType("int") shouldBe GenericType.INTEGER
    getFieldType("smallint") shouldBe GenericType.SHORT
    getFieldType("text") shouldBe GenericType.STRING
    getFieldType("tinyint") shouldBe GenericType.BYTE
    getFieldType("blob") shouldBe GenericType.BYTE_BUFFER
    getFieldType("map") shouldBe GenericType.mapOf(GenericType.STRING, GenericType.INTEGER)
    getFieldType("mapnested") shouldBe GenericType.mapOf(
      GenericType.STRING,
      GenericType.mapOf(GenericType.INTEGER, GenericType.STRING),
    )
    getFieldType("list") shouldBe GenericType.listOf(GenericType.INTEGER)
    getFieldType("listnested") shouldBe GenericType.listOf(GenericType.listOf(GenericType.INTEGER))
  }
}
