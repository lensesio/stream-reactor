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
package io.lenses.streamreactor.connect.cloud.common.sink.conversion

import io.lenses.streamreactor.connect.cloud.common.sink.conversion.TimeUtils.dateWithTimeFieldsOnly
import io.lenses.streamreactor.connect.config.AvroDataFactory
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.TimeUtils.daysSinceEpoch
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Date
import scala.jdk.CollectionConverters._

class ToAvroDataConverterTest extends AnyFunSuiteLike with Matchers {

  test("should convert date") {
    val date = Date.from(Instant.now().truncatedTo(ChronoUnit.DAYS))
    val daysSince: Long = daysSinceEpoch(date)
    val converted = ToAvroDataConverter.convertToGenericRecord(DateSinkData(date))
    checkValueAndSchema(converted, daysSince)
  }

  test("should convert time") {
    val asDate: Date = dateWithTimeFieldsOnly(12, 30, 45, 450)
    val converted = ToAvroDataConverter.convertToGenericRecord(TimeSinkData(asDate))
    checkValueAndSchema(converted, asDate.getTime)
  }

  test("should convert timestamp") {
    val date      = Date.from(Instant.now())
    val converted = ToAvroDataConverter.convertToGenericRecord(TimestampSinkData(date))
    checkValueAndSchema(converted, date.getTime)
  }

  test("enum field is preserved") {
    val is            = getClass.getResourceAsStream("/avro/enum.avsc")
    val avroSchema    = new org.apache.avro.Schema.Parser().parse(is)
    val avroData      = AvroDataFactory.create(100)
    val connectSchema = avroData.toConnectSchema(avroSchema)

    val avroSchemaBack = ToAvroDataConverter.convertSchema(connectSchema)
    avroSchemaBack.getField("tenant_cd").schema().getTypes.get(1).getEnumSymbols.asScala.toSet shouldBe Set("one",
                                                                                                            "two",
                                                                                                            "three",
    )
  }

  private def checkValueAndSchema(converted: Any, expectedValue: Long): Any =
    converted match {
      case nonRecordContainer: Long =>
        nonRecordContainer should be(expectedValue)
      case _ => fail("not a non-record container")
    }

  /**
    * Tests that Connect Union structs (created by Confluent's AvroConverter with enhanced schema support)
    * are correctly converted back to Avro values.
    *
    * When enhanced.avro.schema.support=true, complex unions like [null, string, enum] are represented
    * as Connect STRUCTs with name "io.confluent.connect.avro.Union", where each union branch is a field.
    *
    * For example, the value "CLOCK_OUT" in the string branch becomes: Struct{string=CLOCK_OUT}
    */
  test("should extract value from Connect Union struct for string branch") {
    // Create an Avro schema with a union [null, string, enum]
    val avroSchemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "EventMetadata",
        |  "namespace": "com.example.events",
        |  "fields": [
        |    {"name": "id", "type": "string"},
        |    {
        |      "name": "type",
        |      "type": [
        |        "null",
        |        "string",
        |        {
        |          "type": "enum",
        |          "name": "EventType",
        |          "symbols": ["CLOCK_IN", "CLOCK_OUT", "MANUAL_CHANGE", "UNKNOWN"]
        |        }
        |      ],
        |      "default": null
        |    }
        |  ]
        |}
        |""".stripMargin

    val avroSchema = new Schema.Parser().parse(avroSchemaJson)

    // Create a Connect schema that mimics what Confluent's AvroConverter creates
    // with enhanced.avro.schema.support=true
    val unionSchema = SchemaBuilder.struct()
      .name("io.confluent.connect.avro.Union")
      .field("string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
      .field("com.example.events.EventType", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build()

    val connectSchema = SchemaBuilder.struct()
      .name("com.example.events.EventMetadata")
      .field("id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
      .field("type", unionSchema)
      .build()

    // Create a Connect struct with a union value in the "string" branch
    val unionStruct = new Struct(unionSchema)
      .put("string", "CLOCK_OUT")
      .put("com.example.events.EventType", null)

    val connectStruct = new Struct(connectSchema)
      .put("id", "test-id-123")
      .put("type", unionStruct)

    // Convert to Avro using the target schema
    val sinkData = StructSinkData(connectStruct)
    val result   = ToAvroDataConverter.convertToGenericRecordWithSchema(sinkData, avroSchema)

    // Verify the result
    result shouldBe a[GenericRecord]
    val record = result.asInstanceOf[GenericRecord]

    record.get("id").toString should be("test-id-123")
    // The union value should be extracted as a plain string, not a Struct
    record.get("type") should be("CLOCK_OUT")
  }

  test("should extract value from Connect Union struct for enum branch") {
    // Create an Avro schema with a union [null, string, enum]
    val avroSchemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "EventMetadata",
        |  "namespace": "com.example.events",
        |  "fields": [
        |    {"name": "id", "type": "string"},
        |    {
        |      "name": "type",
        |      "type": [
        |        "null",
        |        "string",
        |        {
        |          "type": "enum",
        |          "name": "EventType",
        |          "symbols": ["CLOCK_IN", "CLOCK_OUT", "MANUAL_CHANGE", "UNKNOWN"]
        |        }
        |      ],
        |      "default": null
        |    }
        |  ]
        |}
        |""".stripMargin

    val avroSchema = new Schema.Parser().parse(avroSchemaJson)

    // Create a Connect schema with Union struct
    val unionSchema = SchemaBuilder.struct()
      .name("io.confluent.connect.avro.Union")
      .field("string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
      .field("com.example.events.EventType", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build()

    val connectSchema = SchemaBuilder.struct()
      .name("com.example.events.EventMetadata")
      .field("id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
      .field("type", unionSchema)
      .build()

    // Create a Connect struct with a union value in the enum branch
    val unionStruct = new Struct(unionSchema)
      .put("string", null)
      .put("com.example.events.EventType", "CLOCK_IN")

    val connectStruct = new Struct(connectSchema)
      .put("id", "test-id-456")
      .put("type", unionStruct)

    // Convert to Avro using the target schema
    val sinkData = StructSinkData(connectStruct)
    val result   = ToAvroDataConverter.convertToGenericRecordWithSchema(sinkData, avroSchema)

    // Verify the result
    result shouldBe a[GenericRecord]
    val record = result.asInstanceOf[GenericRecord]

    record.get("id").toString should be("test-id-456")
    // The union value should be extracted and converted to a GenericEnumSymbol
    val enumValue = record.get("type")
    enumValue shouldBe a[GenericData.EnumSymbol]
    enumValue.toString should be("CLOCK_IN")
  }

  test("should handle null value in Connect Union struct") {
    // Create an Avro schema with a union [null, string]
    val avroSchemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "TestRecord",
        |  "fields": [
        |    {"name": "id", "type": "string"},
        |    {"name": "optionalField", "type": ["null", "string"], "default": null}
        |  ]
        |}
        |""".stripMargin

    val avroSchema = new Schema.Parser().parse(avroSchemaJson)

    // Create a Connect schema with Union struct where all branches are null
    val unionSchema = SchemaBuilder.struct()
      .name("io.confluent.connect.avro.Union")
      .field("string", org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA)
      .optional()
      .build()

    val connectSchema = SchemaBuilder.struct()
      .name("TestRecord")
      .field("id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
      .field("optionalField", unionSchema)
      .build()

    // Create a Connect struct with null union value
    val unionStruct = new Struct(unionSchema)
      .put("string", null)

    val connectStruct = new Struct(connectSchema)
      .put("id", "test-id-789")
      .put("optionalField", unionStruct)

    // Convert to Avro
    val sinkData = StructSinkData(connectStruct)
    val result   = ToAvroDataConverter.convertToGenericRecordWithSchema(sinkData, avroSchema)

    // Verify the result
    result shouldBe a[GenericRecord]
    val record = result.asInstanceOf[GenericRecord]

    record.get("id").toString should be("test-id-789")
    record.get("optionalField") should be(null)
  }

}
