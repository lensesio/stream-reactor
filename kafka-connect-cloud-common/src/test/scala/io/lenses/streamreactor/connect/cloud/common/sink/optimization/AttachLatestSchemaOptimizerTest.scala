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
package io.lenses.streamreactor.connect.cloud.common.sink.optimization

import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.errors.DataException
import org.apache.kafka.connect.header.ConnectHeaders
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AttachLatestSchemaOptimizerTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var optimizer: AttachLatestSchemaOptimizer = _

  before {
    optimizer = new AttachLatestSchemaOptimizer()
  }
  private def createSchema(name: String, version: Int, fields: (String, Schema)*): Schema = {
    val builder = SchemaBuilder.struct().name(name).version(version)
    fields.foreach {
      case (fieldName, fieldSchema) =>
        builder.field(fieldName, fieldSchema)
    }
    builder.build()
  }
  private def createSchemaWithVersion(name: String, version: Int): Schema =
    createSchema(name, version, "field1" -> Schema.STRING_SCHEMA, "field2" -> Schema.INT32_SCHEMA)

  private def createExpandedSchemaWithVersion(name: String, version: Int): Schema =
    createSchema(name,
                 version,
                 "field1" -> Schema.STRING_SCHEMA,
                 "field2" -> Schema.INT32_SCHEMA,
                 "field3" -> Schema.OPTIONAL_STRING_SCHEMA,
    )

  private def createStruct(schema: Schema, fields: (String, Any)*): Struct = {
    val struct = new Struct(schema)
    fields.foreach {
      case (fieldName, fieldValue) =>
        struct.put(fieldName, fieldValue)
    }
    struct
  }

  private def createSinkRecord(
    topic:       String,
    partition:   Int,
    offset:      Long,
    keySchema:   Schema,
    key:         Any,
    valueSchema: Schema,
    value:       Any,
    timestamp:   Long = 0L,
  ): SinkRecord =
    new SinkRecord(
      topic,
      partition,
      keySchema,
      key,
      valueSchema,
      value,
      offset,
      timestamp,
      TimestampType.CREATE_TIME,
      null,
    )

  "AttachLatestSchemaOptimizer" should "return empty list when input is empty" in {
    val result = optimizer.update(List.empty)
    result shouldBe empty
  }

  it should "handle null schemas for key and value" in {
    val record = createSinkRecord("topic1", 0, 0, null, "key", null, "value")

    val result = optimizer.update(List(record))

    result.size shouldBe 1
    result.head.keySchema() shouldBe null
    result.head.valueSchema() shouldBe null
  }

  it should "not modify records when schemas remain the same" in {
    val schema = createSchemaWithVersion("test", 1)
    val struct = createStruct(schema, "field1" -> "value1", "field2" -> 42)

    val record1 = createSinkRecord("topic1", 0, 0, schema, struct, schema, struct)
    val record2 = createSinkRecord("topic1", 0, 1, schema, struct, schema, struct)

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2
    result.head.keySchema() shouldBe schema
    result.head.valueSchema() shouldBe schema
    result(1).keySchema() shouldBe schema
    result(1).valueSchema() shouldBe schema
  }

  it should "adapt records to use the latest schema version" in {
    val schemaV1 = createSchemaWithVersion("test", 1)
    val schemaV2 = createSchemaWithVersion("test", 2)
    val structV1 = createStruct(schemaV1, "field1" -> "value1", "field2" -> 42)
    val structV2 = createStruct(schemaV2, "field1" -> "value2", "field2" -> 43)

    val record1 = createSinkRecord("topic1", 0, 0, schemaV1, structV1, schemaV1, structV1)
    val record2 = createSinkRecord("topic1", 0, 1, schemaV2, structV2, schemaV2, structV2)
    val record3 = createSinkRecord("topic1", 0, 2, schemaV1, structV1, schemaV1, structV1) // Old schema

    val result = optimizer.update(List(record1, record2, record3))

    result.size shouldBe 3
    // Record 3 should be adapted to use schemaV2
    result(2).keySchema() shouldBe schemaV2
    result(2).valueSchema() shouldBe schemaV2
  }

  it should "handle schema evolution with new fields" in {
    val schemaV1 = createSchemaWithVersion("test", 1)
    val schemaV2 = createExpandedSchemaWithVersion("test", 2)

    val structV1 = createStruct(schemaV1, "field1" -> "value1", "field2" -> 42)
    val structV2 = createStruct(schemaV2, "field1" -> "value2", "field2" -> 43, "field3" -> "new-field")

    val record1 = createSinkRecord("topic1", 0, 0, schemaV1, structV1, schemaV1, structV1)
    val record2 = createSinkRecord("topic1", 0, 1, schemaV2, structV2, schemaV2, structV2)
    val record3 = createSinkRecord("topic1", 0, 2, schemaV1, structV1, schemaV1, structV1) // Old schema

    val result = optimizer.update(List(record1, record2, record3))

    result.size shouldBe 3

    // Record 3 should be adapted to use schemaV2
    result(2).keySchema() shouldBe schemaV2
    result(2).valueSchema() shouldBe schemaV2

    // Check the adapted struct has the new field set to null
    val adaptedKeyStruct   = result(2).key().asInstanceOf[Struct]
    val adaptedValueStruct = result(2).value().asInstanceOf[Struct]

    adaptedKeyStruct.get("field1") shouldBe "value1"
    adaptedKeyStruct.get("field2") shouldBe 42
    adaptedKeyStruct.get("field3") shouldBe null

    adaptedValueStruct.get("field1") shouldBe "value1"
    adaptedValueStruct.get("field2") shouldBe 42
    adaptedValueStruct.get("field3") shouldBe null
  }

  it should "handle array schema evolution" in {
    // Create array schemas
    val elemSchemaV1 = createSchemaWithVersion("elem", 1)
    val elemSchemaV2 = createExpandedSchemaWithVersion("elem", 2)

    val arraySchemaV1 = SchemaBuilder.array(elemSchemaV1).version(1).build()
    val arraySchemaV2 = SchemaBuilder.array(elemSchemaV2).version(2).build()

    // Create arrays
    val elemV1 = createStruct(elemSchemaV1, "field1" -> "value1", "field2" -> 42)
    val elemV2 = createStruct(elemSchemaV2, "field1" -> "value2", "field2" -> 43, "field3" -> "new-field")

    val arrayV1 = java.util.Arrays.asList(elemV1, elemV1)
    val arrayV2 = java.util.Arrays.asList(elemV2, elemV2)

    val record1 = createSinkRecord("topic1", 0, 0, null, null, arraySchemaV1, arrayV1)
    val record2 = createSinkRecord("topic1", 0, 1, null, null, arraySchemaV2, arrayV2)
    val record3 = createSinkRecord("topic1", 0, 2, null, null, arraySchemaV1, arrayV1) // Old schema

    val result = optimizer.update(List(record1, record2, record3))

    result.size shouldBe 3
    result(2).valueSchema() shouldBe arraySchemaV2

    // Check the adapted array elements
    val adaptedArray = result(2).value().asInstanceOf[java.util.List[_]]
    adaptedArray.size() shouldBe 2

    val adaptedElem = adaptedArray.get(0).asInstanceOf[Struct]
    adaptedElem.schema() shouldBe elemSchemaV2
    adaptedElem.get("field1") shouldBe "value1"
    adaptedElem.get("field2") shouldBe 42
    adaptedElem.get("field3") shouldBe null
  }

  it should "handle map schema evolution" in {

    // Create map schemas
    val valueSchemaV1 = createSchemaWithVersion("value", 1)
    val valueSchemaV2 = createExpandedSchemaWithVersion("value", 2)

    val mapSchemaV1 = SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchemaV1).version(1).build()
    val mapSchemaV2 = SchemaBuilder.map(Schema.STRING_SCHEMA, valueSchemaV2).version(2).build()

    val valueV1 = createStruct(valueSchemaV1, "field1" -> "value1", "field2" -> 42)
    val valueV2 = createStruct(valueSchemaV2, "field1" -> "value2", "field2" -> 43, "field3" -> "new-field")

    val mapV1 = new java.util.HashMap[String, Struct]()
    mapV1.put("key1", valueV1)

    val mapV2 = new java.util.HashMap[String, Struct]()
    mapV2.put("key2", valueV2)

    val record1 = createSinkRecord("topic1", 0, 0, null, null, mapSchemaV1, mapV1)
    val record2 = createSinkRecord("topic1", 0, 1, null, null, mapSchemaV2, mapV2)
    val record3 = createSinkRecord("topic1", 0, 2, null, null, mapSchemaV1, mapV1) // Old schema

    val result = optimizer.update(List(record1, record2, record3))

    result.size shouldBe 3
    result(2).valueSchema() shouldBe mapSchemaV2

    // Check the adapted map values
    val adaptedMap = result(2).value().asInstanceOf[java.util.Map[_, _]]
    adaptedMap.size() shouldBe 1

    val adaptedValue = adaptedMap.get("key1").asInstanceOf[Struct]
    adaptedValue.schema() shouldBe valueSchemaV2
    adaptedValue.get("field1") shouldBe "value1"
    adaptedValue.get("field2") shouldBe 42
    adaptedValue.get("field3") shouldBe null
  }

  it should "handle null values with optional schema" in {

    val schemaV1 =
      createSchema("test", 1, "field1" -> Schema.OPTIONAL_STRING_SCHEMA, "field2" -> Schema.OPTIONAL_INT32_SCHEMA)
    val schemaV2 = createSchema(
      "test",
      2,
      "field1" -> Schema.OPTIONAL_STRING_SCHEMA,
      "field2" -> Schema.OPTIONAL_INT32_SCHEMA,
      "field3" -> Schema.OPTIONAL_STRING_SCHEMA,
    )

    val structV1 = new Struct(schemaV1)
    structV1.put("field1", null)
    structV1.put("field2", null)

    val record1 = createSinkRecord("topic1", 0, 0, schemaV1, structV1, schemaV1, structV1)
    val record2 = createSinkRecord("topic1", 0, 1, schemaV2, null, schemaV2, null)

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2
    result(0).valueSchema() shouldBe schemaV2 // Updated to latest schema

    // Null value should remain null
    result(1).value() shouldBe null
  }

  it should "throw DataException for incompatible schemas with required fields" in {

    val schemaV1 = createSchemaWithVersion("test", 1)
    val schemaV2 = createSchema("test",
                                2,
                                "field1" -> Schema.STRING_SCHEMA,
                                "field2" -> Schema.INT32_SCHEMA,
                                "field3" -> Schema.STRING_SCHEMA,
    )

    val structV1 = createStruct(schemaV1, "field1" -> "value1", "field2" -> 42)
    val structV2 = createStruct(schemaV2, "field1" -> "value2", "field2" -> 43, "field3" -> "required")

    val record1 = createSinkRecord("topic1", 0, 0, schemaV1, structV1, schemaV1, structV1)
    val record2 = createSinkRecord("topic1", 0, 1, schemaV2, structV2, schemaV2, structV2)

    assertThrows[DataException] {
      optimizer.update(List(record1, record2))
    }
  }

  it should "handle schemas with default values" in {

    val schemaV1 = createSchemaWithVersion("test", 1)
    val schemaV2 = createSchema(
      "test",
      2,
      "field1" -> Schema.STRING_SCHEMA,
      "field2" -> Schema.INT32_SCHEMA,
      "field3" -> SchemaBuilder.string().defaultValue("default-value").build(),
    )

    val structV1 = createStruct(schemaV1, "field1" -> "value1", "field2" -> 42)
    val structV2 = createStruct(schemaV2, "field1" -> "value2", "field2" -> 43, "field3" -> "custom-value")

    val record1 = createSinkRecord("topic1", 0, 0, schemaV1, structV1, schemaV1, structV1)
    val record2 = createSinkRecord("topic1", 0, 1, schemaV2, structV2, schemaV2, structV2)

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2

    // Check that record1 has been adapted with the default value for field3
    val adaptedValue = result(0).value().asInstanceOf[Struct]
    adaptedValue.get("field3") shouldBe "default-value"
  }

  it should "handle records from different topics independently" in {

    val schemaV1 = createSchemaWithVersion("test", 1)
    val schemaV2 = createExpandedSchemaWithVersion("test", 2)

    val structV1 = createStruct(schemaV1, "field1" -> "value1", "field2" -> 42)
    val structV2 = createStruct(schemaV2, "field1" -> "value2", "field2" -> 43, "field3" -> "new-field")

    val record1Topic1 = createSinkRecord("topic1", 0, 0, schemaV1, structV1, schemaV1, structV1)
    val record2Topic1 = createSinkRecord("topic1", 0, 1, schemaV2, structV2, schemaV2, structV2)

    val record1Topic2 = createSinkRecord("topic2", 0, 0, schemaV1, structV1, schemaV1, structV1)

    val result = optimizer.update(List(record1Topic1, record2Topic1, record1Topic2))

    result.size shouldBe 3

    // First topic records
    result(0).keySchema() shouldBe schemaV2
    result(0).valueSchema() shouldBe schemaV2
    result(1).keySchema() shouldBe schemaV2
    result(1).valueSchema() shouldBe schemaV2

    // Second topic records should not be affected by first topic's schema evolution
    result(2).keySchema() shouldBe schemaV1
    result(2).valueSchema() shouldBe schemaV1
  }

  it should "handle key and value schema updates independently" in {

    val keySchemaV1   = createSchemaWithVersion("keySchema", 1)
    val valueSchemaV1 = createSchemaWithVersion("valueSchema", 1)

    val keySchemaV2   = createExpandedSchemaWithVersion("keySchema", 2)
    val valueSchemaV2 = createExpandedSchemaWithVersion("valueSchema", 2)

    val keyStructV1   = createStruct(keySchemaV1, "field1" -> "key1", "field2" -> 101)
    val valueStructV1 = createStruct(valueSchemaV1, "field1" -> "value1", "field2" -> 201)

    val keyStructV2   = createStruct(keySchemaV2, "field1" -> "key2", "field2" -> 102, "field3" -> "key-new")
    val valueStructV2 = createStruct(valueSchemaV2, "field1" -> "value2", "field2" -> 202, "field3" -> "value-new")

    // Key evolves but value stays at V1
    val record1 = createSinkRecord("topic1", 0, 0, keySchemaV1, keyStructV1, valueSchemaV1, valueStructV1)
    val record2 = createSinkRecord("topic1", 0, 1, keySchemaV2, keyStructV2, valueSchemaV1, valueStructV1)

    // Record with old key schema but latest value schema
    val record3 = createSinkRecord("topic1", 0, 2, keySchemaV1, keyStructV1, valueSchemaV2, valueStructV2)

    val result = optimizer.update(List(record1, record2, record3))

    result.size shouldBe 3

    // Check final schemas are the latest versions
    result(0).keySchema() shouldBe keySchemaV2
    result(0).valueSchema() shouldBe valueSchemaV2

    result(1).keySchema() shouldBe keySchemaV2
    result(1).valueSchema() shouldBe valueSchemaV2

    result(2).keySchema() shouldBe keySchemaV2
    result(2).valueSchema() shouldBe valueSchemaV2
  }

  it should "preserve original record metadata" in {

    val schemaV1 = createSchemaWithVersion("test", 1)
    val schemaV2 = createExpandedSchemaWithVersion("test", 2)

    val structV1 = createStruct(schemaV1, "field1" -> "value1", "field2" -> 42)
    val structV2 = createStruct(schemaV2, "field1" -> "value2", "field2" -> 43, "field3" -> "new-field")

    val timestamp = System.currentTimeMillis()
    val headers   = new ConnectHeaders()
    headers.addString("header1", "value1")

    val record1 = new SinkRecord(
      "topic1",
      0,
      schemaV1,
      structV1,
      schemaV1,
      structV1,
      100L,
      timestamp,
      TimestampType.CREATE_TIME,
      headers,
    )

    val record2 = new SinkRecord(
      "topic1",
      0,
      schemaV2,
      structV2,
      schemaV2,
      structV2,
      101L,
      timestamp + 1000,
      TimestampType.CREATE_TIME,
      headers,
    )

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2

    // First record should have original metadata even with updated schema
    result(0).kafkaOffset() shouldBe 100L
    result(0).timestamp() shouldBe timestamp
    result(0).timestampType() shouldBe TimestampType.CREATE_TIME
    result(0).headers() shouldBe headers
  }

  it should "handle records with primitive types" in {

    val record1 = createSinkRecord("topic1", 0, 0, Schema.STRING_SCHEMA, "key1", Schema.INT32_SCHEMA, 42)
    val record2 = createSinkRecord("topic1", 0, 1, Schema.STRING_SCHEMA, "key2", Schema.INT32_SCHEMA, 43)

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2
    result(0).key() shouldBe "key1"
    result(0).value() shouldBe 42
    result(1).key() shouldBe "key2"
    result(1).value() shouldBe 43
  }

  it should "handle records with no key schema" in {

    val valueSchemaV1 = createSchemaWithVersion("valueSchema", 1)
    val valueSchemaV2 = createExpandedSchemaWithVersion("valueSchema", 2)

    val valueStructV1 = createStruct(valueSchemaV1, "field1" -> "value1", "field2" -> 42)
    val valueStructV2 = createStruct(valueSchemaV2, "field1" -> "value2", "field2" -> 43, "field3" -> "new-field")

    val record1 = createSinkRecord("topic1", 0, 0, null, "key1", valueSchemaV1, valueStructV1)
    val record2 = createSinkRecord("topic1", 0, 1, null, "key2", valueSchemaV2, valueStructV2)

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2
    result(0).keySchema() shouldBe null
    result(0).valueSchema() shouldBe valueSchemaV2
    result(1).keySchema() shouldBe null
    result(1).valueSchema() shouldBe valueSchemaV2
  }

  it should "handle records with no value schema" in {

    val keySchemaV1 = createSchemaWithVersion("keySchema", 1)
    val keySchemaV2 = createExpandedSchemaWithVersion("keySchema", 2)

    val keyStructV1 = createStruct(keySchemaV1, "field1" -> "key1", "field2" -> 101)
    val keyStructV2 = createStruct(keySchemaV2, "field1" -> "key2", "field2" -> 102, "field3" -> "key-field3")

    val record1 = createSinkRecord("topic1", 0, 0, keySchemaV1, keyStructV1, null, "value1")
    val record2 = createSinkRecord("topic1", 0, 1, keySchemaV2, keyStructV2, null, "value2")

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2
    result(0).keySchema() shouldBe keySchemaV2
    result(0).valueSchema() shouldBe null
    result(1).keySchema() shouldBe keySchemaV2
    result(1).valueSchema() shouldBe null
  }
}
