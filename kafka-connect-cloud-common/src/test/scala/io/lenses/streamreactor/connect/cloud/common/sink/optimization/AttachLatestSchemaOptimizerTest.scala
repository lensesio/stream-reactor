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

  /**
   * Test verifying enum to union schema evolution is supported.
   *
   * This test simulates the following Avro schema evolution:
   *
   * Schema V1 (Avro):
   * {{{
   * {
   *   "type": "record",
   *   "name": "Order",
   *   "namespace": "com.example",
   *   "fields": [
   *     { "name": "orderId", "type": "string" },
   *     {
   *       "name": "status",
   *       "type": {
   *         "type": "enum",
   *         "name": "OrderStatus",
   *         "symbols": ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
   *       }
   *     }
   *   ]
   * }
   * }}}
   *
   * Schema V2 (Avro) - backward compatible evolution adding string to union:
   * {{{
   * {
   *   "type": "record",
   *   "name": "Order",
   *   "namespace": "com.example",
   *   "fields": [
   *     { "name": "orderId", "type": "string" },
   *     {
   *       "name": "status",
   *       "type": [
   *         {
   *           "type": "enum",
   *           "name": "OrderStatus",
   *           "symbols": ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
   *         },
   *         "string"
   *       ]
   *     }
   *   ]
   * }
   * }}}
   *
   * In Kafka Connect:
   * - Avro enum -> STRING schema with name "com.example.OrderStatus"
   * - Avro union [enum, string] -> STRUCT with name "io.confluent.connect.avro.Union"
   *
   * The optimizer promotes the enum value to a union struct with the matching branch populated.
   */
  it should "successfully adapt enum field to union field (enum -> [enum, string] evolution)" in {
    // Schema V1: Avro enum is represented as STRING in Kafka Connect
    val enumSchema = SchemaBuilder
      .string()
      .name("com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum", "com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum.PENDING", "PENDING")
      .parameter("io.confluent.connect.avro.Enum.PROCESSING", "PROCESSING")
      .parameter("io.confluent.connect.avro.Enum.SHIPPED", "SHIPPED")
      .parameter("io.confluent.connect.avro.Enum.DELIVERED", "DELIVERED")
      .parameter("io.confluent.connect.avro.Enum.CANCELLED", "CANCELLED")
      .build()

    val orderSchemaV1 = SchemaBuilder
      .struct()
      .name("com.example.Order")
      .version(1)
      .field("orderId", Schema.STRING_SCHEMA)
      .field("status", enumSchema)
      .build()

    // Schema V2: Avro union [enum, string] is represented as STRUCT in Kafka Connect
    val unionEnumBranchSchema = SchemaBuilder
      .string()
      .name("com.example.OrderStatus")
      .optional()
      .parameter("io.confluent.connect.avro.Enum", "com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum.PENDING", "PENDING")
      .parameter("io.confluent.connect.avro.Enum.PROCESSING", "PROCESSING")
      .parameter("io.confluent.connect.avro.Enum.SHIPPED", "SHIPPED")
      .parameter("io.confluent.connect.avro.Enum.DELIVERED", "DELIVERED")
      .parameter("io.confluent.connect.avro.Enum.CANCELLED", "CANCELLED")
      .build()

    val unionSchema = SchemaBuilder
      .struct()
      .name("io.confluent.connect.avro.Union")
      .field("com.example.OrderStatus", unionEnumBranchSchema) // enum branch
      .field("string", Schema.OPTIONAL_STRING_SCHEMA)          // string branch
      .build()

    val orderSchemaV2 = SchemaBuilder
      .struct()
      .name("com.example.Order")
      .version(2)
      .field("orderId", Schema.STRING_SCHEMA)
      .field("status", unionSchema)
      .build()

    // Create record with V1 schema (enum value as string)
    val orderV1 = new Struct(orderSchemaV1)
    orderV1.put("orderId", "order-123")
    orderV1.put("status", "PENDING") // Enum value stored as string

    // Create record with V2 schema (union type as struct)
    val statusUnionV2 = new Struct(unionSchema)
    statusUnionV2.put("com.example.OrderStatus", "PROCESSING") // Using enum branch
    statusUnionV2.put("string", null)

    val orderV2 = new Struct(orderSchemaV2)
    orderV2.put("orderId", "order-456")
    orderV2.put("status", statusUnionV2)

    val record1 = createSinkRecord("orders", 0, 0, null, null, orderSchemaV1, orderV1)
    val record2 = createSinkRecord("orders", 0, 1, null, null, orderSchemaV2, orderV2)

    // The optimizer should successfully adapt record 1 to use V2 schema:
    // 1. Record 2 has schema V2 (version 2), so V2 becomes the "latest" schema
    // 2. Optimizer adapts record 1 (V1) to use V2
    // 3. For the "status" field: promotes STRING ("PENDING") to union STRUCT
    val result: List[SinkRecord] = optimizer.update(List(record1, record2))

    result.size shouldBe 2

    // Verify record 1 was adapted to V2 schema
    result(0).valueSchema() shouldBe orderSchemaV2
    val adaptedOrder1 = result(0).value().asInstanceOf[Struct]
    adaptedOrder1.get("orderId") shouldBe "order-123"

    // The status field should now be a union struct with enum branch populated
    val adaptedStatus1 = adaptedOrder1.get("status").asInstanceOf[Struct]
    adaptedStatus1.get("com.example.OrderStatus") shouldBe "PENDING"
    adaptedStatus1.get("string") shouldBe null

    // Record 2 should remain unchanged
    result(1).valueSchema() shouldBe orderSchemaV2
    val adaptedOrder2 = result(1).value().asInstanceOf[Struct]
    adaptedOrder2.get("orderId") shouldBe "order-456"
    val adaptedStatus2 = adaptedOrder2.get("status").asInstanceOf[Struct]
    adaptedStatus2.get("com.example.OrderStatus") shouldBe "PROCESSING"
    adaptedStatus2.get("string") shouldBe null
  }

  /**
   * Test verifying union to enum schema adaptation (extracting from union).
   *
   * This tests the reverse case where a newer schema has a simpler type
   * than an older schema (union -> enum).
   */
  it should "successfully adapt union field to enum field ([enum, string] -> enum extraction)" in {
    // Schema V1: union [enum, string] represented as STRUCT
    val unionEnumBranchSchema = SchemaBuilder
      .string()
      .name("com.example.OrderStatus")
      .optional()
      .parameter("io.confluent.connect.avro.Enum", "com.example.OrderStatus")
      .build()

    val unionSchema = SchemaBuilder
      .struct()
      .name("io.confluent.connect.avro.Union")
      .field("com.example.OrderStatus", unionEnumBranchSchema)
      .field("string", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val orderSchemaV1 = SchemaBuilder
      .struct()
      .name("com.example.Order")
      .version(1)
      .field("orderId", Schema.STRING_SCHEMA)
      .field("status", unionSchema)
      .build()

    // Schema V2: simple enum (higher version - becomes the target)
    val enumSchema = SchemaBuilder
      .string()
      .name("com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum", "com.example.OrderStatus")
      .build()

    val orderSchemaV2 = SchemaBuilder
      .struct()
      .name("com.example.Order")
      .version(2)
      .field("orderId", Schema.STRING_SCHEMA)
      .field("status", enumSchema)
      .build()

    // Create record with V1 schema (union type)
    val statusUnionV1 = new Struct(unionSchema)
    statusUnionV1.put("com.example.OrderStatus", "PENDING")
    statusUnionV1.put("string", null)

    val orderV1 = new Struct(orderSchemaV1)
    orderV1.put("orderId", "order-123")
    orderV1.put("status", statusUnionV1)

    // Create record with V2 schema (simple enum)
    val orderV2 = new Struct(orderSchemaV2)
    orderV2.put("orderId", "order-456")
    orderV2.put("status", "PROCESSING")

    val record1 = createSinkRecord("orders", 0, 0, null, null, orderSchemaV1, orderV1)
    val record2 = createSinkRecord("orders", 0, 1, null, null, orderSchemaV2, orderV2)

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2

    // Verify record 1 was adapted to V2 schema with extracted enum value
    result(0).valueSchema() shouldBe orderSchemaV2
    val adaptedOrder1 = result(0).value().asInstanceOf[Struct]
    adaptedOrder1.get("orderId") shouldBe "order-123"
    adaptedOrder1.get("status") shouldBe "PENDING" // Extracted from union

    // Record 2 should remain unchanged
    result(1).valueSchema() shouldBe orderSchemaV2
    val adaptedOrder2 = result(1).value().asInstanceOf[Struct]
    adaptedOrder2.get("orderId") shouldBe "order-456"
    adaptedOrder2.get("status") shouldBe "PROCESSING"
  }

  /**
   * Test verifying that name matches take priority over type matches when promoting to union.
   *
   * This is a regression test for a bug where promoteToUnion would incorrectly select
   * the first type-matching branch instead of the name-matching branch. For example,
   * when the union is [string, enum], promoting an enum (STRING type with name) would
   * incorrectly match the plain string branch because it appeared first and had a matching type.
   *
   * The fix ensures name matches are searched across ALL fields first, then falls back
   * to type matches only if no name match exists.
   */
  it should "prioritize name match over type match when promoting enum to union [string, enum]" in {
    // Schema V1: Avro enum is represented as STRING in Kafka Connect with a name
    val enumSchema = SchemaBuilder
      .string()
      .name("com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum", "com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum.PENDING", "PENDING")
      .parameter("io.confluent.connect.avro.Enum.SHIPPED", "SHIPPED")
      .build()

    val orderSchemaV1 = SchemaBuilder
      .struct()
      .name("com.example.Order")
      .version(1)
      .field("orderId", Schema.STRING_SCHEMA)
      .field("status", enumSchema)
      .build()

    // Schema V2: Union where STRING branch appears BEFORE enum branch
    // This order is critical for testing the bug fix!
    val unionEnumBranchSchema = SchemaBuilder
      .string()
      .name("com.example.OrderStatus")
      .optional()
      .parameter("io.confluent.connect.avro.Enum", "com.example.OrderStatus")
      .parameter("io.confluent.connect.avro.Enum.PENDING", "PENDING")
      .parameter("io.confluent.connect.avro.Enum.SHIPPED", "SHIPPED")
      .build()

    val unionSchema = SchemaBuilder
      .struct()
      .name("io.confluent.connect.avro.Union")
      .field("string", Schema.OPTIONAL_STRING_SCHEMA)          // STRING branch FIRST (type matches enum)
      .field("com.example.OrderStatus", unionEnumBranchSchema) // Enum branch SECOND (name matches)
      .build()

    val orderSchemaV2 = SchemaBuilder
      .struct()
      .name("com.example.Order")
      .version(2)
      .field("orderId", Schema.STRING_SCHEMA)
      .field("status", unionSchema)
      .build()

    // Create record with V1 schema (enum value)
    val orderV1 = new Struct(orderSchemaV1)
    orderV1.put("orderId", "order-123")
    orderV1.put("status", "PENDING")

    // Create record with V2 schema (union with enum branch populated)
    val statusUnionV2 = new Struct(unionSchema)
    statusUnionV2.put("string", null)
    statusUnionV2.put("com.example.OrderStatus", "SHIPPED")

    val orderV2 = new Struct(orderSchemaV2)
    orderV2.put("orderId", "order-456")
    orderV2.put("status", statusUnionV2)

    val record1 = createSinkRecord("orders", 0, 0, null, null, orderSchemaV1, orderV1)
    val record2 = createSinkRecord("orders", 0, 1, null, null, orderSchemaV2, orderV2)

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2

    // Verify record 1 was adapted to V2 schema
    result(0).valueSchema() shouldBe orderSchemaV2
    val adaptedOrder1 = result(0).value().asInstanceOf[Struct]
    adaptedOrder1.get("orderId") shouldBe "order-123"

    // CRITICAL: The enum value should be in the ENUM branch (name match),
    // NOT the string branch (type match). Before the fix, this would fail
    // because the string branch appeared first and had matching type (STRING).
    val adaptedStatus1 = adaptedOrder1.get("status").asInstanceOf[Struct]
    adaptedStatus1.get("com.example.OrderStatus") shouldBe "PENDING"
    adaptedStatus1.get("string") shouldBe null

    // Record 2 should remain unchanged
    result(1).valueSchema() shouldBe orderSchemaV2
    val adaptedOrder2 = result(1).value().asInstanceOf[Struct]
    adaptedOrder2.get("orderId") shouldBe "order-456"
    val adaptedStatus2 = adaptedOrder2.get("status").asInstanceOf[Struct]
    adaptedStatus2.get("com.example.OrderStatus") shouldBe "SHIPPED"
    adaptedStatus2.get("string") shouldBe null
  }

  /**
   * Test verifying that nested structs inside unions are recursively adapted.
   *
   * This is a regression test for a bug where promoteToUnion and extractFromUnion
   * would place/return values directly without calling adaptValue recursively.
   * This works for primitives but fails for complex types like nested structs
   * that need schema evolution.
   *
   * Scenario:
   * - Schema V1: field "data" is a simple struct with fields (name, value)
   * - Schema V2: field "data" is a union [struct, null] where struct has evolved (name, value, extra)
   *
   * When promoting the V1 struct to the V2 union, the inner struct should be
   * adapted from the V1 schema to the V2 struct schema (adding the new "extra" field).
   */
  it should "recursively adapt nested struct when promoting to union" in {
    // Schema V1: data is a simple struct
    val nestedStructSchemaV1 = SchemaBuilder
      .struct()
      .name("com.example.Data")
      .version(1)
      .field("name", Schema.STRING_SCHEMA)
      .field("value", Schema.INT32_SCHEMA)
      .build()

    val containerSchemaV1 = SchemaBuilder
      .struct()
      .name("com.example.Container")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("data", nestedStructSchemaV1)
      .build()

    // Schema V2: data is a union [struct, null] where struct has an additional field
    val nestedStructSchemaV2 = SchemaBuilder
      .struct()
      .name("com.example.Data")
      .version(2)
      .optional()
      .field("name", Schema.STRING_SCHEMA)
      .field("value", Schema.INT32_SCHEMA)
      .field("extra", Schema.OPTIONAL_STRING_SCHEMA) // New field in V2
      .build()

    val unionSchema = SchemaBuilder
      .struct()
      .name("io.confluent.connect.avro.Union")
      .field("com.example.Data", nestedStructSchemaV2) // struct branch
      .field("null", Schema.OPTIONAL_STRING_SCHEMA)    // null branch (simplified)
      .build()

    val containerSchemaV2 = SchemaBuilder
      .struct()
      .name("com.example.Container")
      .version(2)
      .field("id", Schema.STRING_SCHEMA)
      .field("data", unionSchema)
      .build()

    // Create record with V1 schema
    val nestedDataV1 = new Struct(nestedStructSchemaV1)
    nestedDataV1.put("name", "test-name")
    nestedDataV1.put("value", 42)

    val containerV1 = new Struct(containerSchemaV1)
    containerV1.put("id", "container-1")
    containerV1.put("data", nestedDataV1)

    // Create record with V2 schema
    val nestedDataV2 = new Struct(nestedStructSchemaV2)
    nestedDataV2.put("name", "test-name-2")
    nestedDataV2.put("value", 100)
    nestedDataV2.put("extra", "extra-value")

    val dataUnionV2 = new Struct(unionSchema)
    dataUnionV2.put("com.example.Data", nestedDataV2)
    dataUnionV2.put("null", null)

    val containerV2 = new Struct(containerSchemaV2)
    containerV2.put("id", "container-2")
    containerV2.put("data", dataUnionV2)

    val record1 = createSinkRecord("containers", 0, 0, null, null, containerSchemaV1, containerV1)
    val record2 = createSinkRecord("containers", 0, 1, null, null, containerSchemaV2, containerV2)

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2

    // Verify record 1 was adapted to V2 schema
    result(0).valueSchema() shouldBe containerSchemaV2
    val adaptedContainer1 = result(0).value().asInstanceOf[Struct]
    adaptedContainer1.get("id") shouldBe "container-1"

    // The data field should now be a union struct
    val adaptedDataUnion = adaptedContainer1.get("data").asInstanceOf[Struct]

    // The nested struct should have been adapted to V2 schema with the new field
    val adaptedNestedData = adaptedDataUnion.get("com.example.Data").asInstanceOf[Struct]
    adaptedNestedData.schema() shouldBe nestedStructSchemaV2
    adaptedNestedData.get("name") shouldBe "test-name"
    adaptedNestedData.get("value") shouldBe 42
    adaptedNestedData.get("extra") shouldBe null // New field should be null

    // null branch should be null
    adaptedDataUnion.get("null") shouldBe null
  }

  /**
   * Test verifying that nested structs inside unions are recursively adapted when extracting.
   *
   * Scenario:
   * - Schema V1: field "data" is a union [struct, null] with struct having (name, value)
   * - Schema V2: field "data" is a simple struct with evolved schema (name, value, extra)
   *
   * When extracting from the V1 union to the V2 simple struct, the inner struct
   * should be adapted from the V1 struct schema to the V2 struct schema.
   */
  it should "recursively adapt nested struct when extracting from union" in {
    // Schema V1: data is a union [struct, null]
    val nestedStructSchemaV1 = SchemaBuilder
      .struct()
      .name("com.example.Data")
      .version(1)
      .optional()
      .field("name", Schema.STRING_SCHEMA)
      .field("value", Schema.INT32_SCHEMA)
      .build()

    val unionSchema = SchemaBuilder
      .struct()
      .name("io.confluent.connect.avro.Union")
      .field("com.example.Data", nestedStructSchemaV1)
      .field("null", Schema.OPTIONAL_STRING_SCHEMA)
      .build()

    val containerSchemaV1 = SchemaBuilder
      .struct()
      .name("com.example.Container")
      .version(1)
      .field("id", Schema.STRING_SCHEMA)
      .field("data", unionSchema)
      .build()

    // Schema V2: data is a simple struct with an additional field
    val nestedStructSchemaV2 = SchemaBuilder
      .struct()
      .name("com.example.Data")
      .version(2)
      .field("name", Schema.STRING_SCHEMA)
      .field("value", Schema.INT32_SCHEMA)
      .field("extra", Schema.OPTIONAL_STRING_SCHEMA) // New field in V2
      .build()

    val containerSchemaV2 = SchemaBuilder
      .struct()
      .name("com.example.Container")
      .version(2)
      .field("id", Schema.STRING_SCHEMA)
      .field("data", nestedStructSchemaV2)
      .build()

    // Create record with V1 schema (union with struct)
    val nestedDataV1 = new Struct(nestedStructSchemaV1)
    nestedDataV1.put("name", "test-name")
    nestedDataV1.put("value", 42)

    val dataUnionV1 = new Struct(unionSchema)
    dataUnionV1.put("com.example.Data", nestedDataV1)
    dataUnionV1.put("null", null)

    val containerV1 = new Struct(containerSchemaV1)
    containerV1.put("id", "container-1")
    containerV1.put("data", dataUnionV1)

    // Create record with V2 schema (simple struct)
    val nestedDataV2 = new Struct(nestedStructSchemaV2)
    nestedDataV2.put("name", "test-name-2")
    nestedDataV2.put("value", 100)
    nestedDataV2.put("extra", "extra-value")

    val containerV2 = new Struct(containerSchemaV2)
    containerV2.put("id", "container-2")
    containerV2.put("data", nestedDataV2)

    val record1 = createSinkRecord("containers", 0, 0, null, null, containerSchemaV1, containerV1)
    val record2 = createSinkRecord("containers", 0, 1, null, null, containerSchemaV2, containerV2)

    val result = optimizer.update(List(record1, record2))

    result.size shouldBe 2

    // Verify record 1 was adapted to V2 schema
    result(0).valueSchema() shouldBe containerSchemaV2
    val adaptedContainer1 = result(0).value().asInstanceOf[Struct]
    adaptedContainer1.get("id") shouldBe "container-1"

    // The data field should have been extracted from the union and adapted to V2 schema
    val adaptedData = adaptedContainer1.get("data").asInstanceOf[Struct]
    adaptedData.schema() shouldBe nestedStructSchemaV2
    adaptedData.get("name") shouldBe "test-name"
    adaptedData.get("value") shouldBe 42
    adaptedData.get("extra") shouldBe null // New field should be null
  }
}
