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

import io.confluent.connect.avro.AvroData
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.TimeUtils.dateWithTimeFieldsOnly
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.TimeUtils.daysSinceEpoch
import org.apache.avro.Schema
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
    // For primitive SinkData types, the schema parameter is not used
    val converted = ToAvroDataConverter.convertToGenericRecordWithSchema(DateSinkData(date), null)
    checkValueAndSchema(converted, daysSince)
  }

  test("should convert time") {
    val asDate: Date = dateWithTimeFieldsOnly(12, 30, 45, 450)
    // For primitive SinkData types, the schema parameter is not used
    val converted = ToAvroDataConverter.convertToGenericRecordWithSchema(TimeSinkData(asDate), null)
    checkValueAndSchema(converted, asDate.getTime)
  }

  test("should convert timestamp") {
    val date = Date.from(Instant.now())
    // For primitive SinkData types, the schema parameter is not used
    val converted = ToAvroDataConverter.convertToGenericRecordWithSchema(TimestampSinkData(date), null)
    checkValueAndSchema(converted, date.getTime)
  }

  test("enum field is preserved") {
    val is            = getClass.getResourceAsStream("/avro/enum.avsc")
    val avroSchema    = new org.apache.avro.Schema.Parser().parse(is)
    val avroData      = new AvroData(100)
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

  test("should use properly typed default values when field is missing from source struct") {
    // Create an Avro schema with fields that have default values
    val avroSchemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "TestRecord",
        |  "namespace": "test",
        |  "fields": [
        |    {"name": "requiredField", "type": "string"},
        |    {"name": "fieldWithStringDefault", "type": "string", "default": "defaultString"},
        |    {"name": "fieldWithIntDefault", "type": "int", "default": 42},
        |    {"name": "fieldWithNullDefault", "type": ["null", "string"], "default": null},
        |    {"name": "fieldWithBooleanDefault", "type": "boolean", "default": true}
        |  ]
        |}
        |""".stripMargin

    val avroSchema = new Schema.Parser().parse(avroSchemaJson)

    // Create a Connect schema with only the required field (simulating an older schema version)
    val connectSchema = SchemaBuilder.struct()
      .name("test.TestRecord")
      .field("requiredField", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
      .build()

    val sourceStruct = new Struct(connectSchema)
      .put("requiredField", "testValue")

    val sinkData = StructSinkData(sourceStruct)

    // Convert using the Avro schema which has default values
    val result = ToAvroDataConverter.convertToGenericRecordWithSchema(sinkData, avroSchema)

    result shouldBe a[GenericRecord]
    val record = result.asInstanceOf[GenericRecord]

    // Verify the required field is set correctly
    record.get("requiredField").toString should be("testValue")

    // Verify default values are properly typed (not JsonNodes)
    // String default should be a String, not a TextNode
    val stringDefault = record.get("fieldWithStringDefault")
    stringDefault shouldBe a[java.lang.CharSequence]
    stringDefault.toString should be("defaultString")

    // Int default should be an Integer, not an IntNode
    val intDefault = record.get("fieldWithIntDefault")
    intDefault shouldBe a[java.lang.Integer]
    intDefault should be(42)

    // Null default should be null, not NullNode
    val nullDefault = record.get("fieldWithNullDefault")
    nullDefault should be(null)

    // Boolean default should be a Boolean, not BooleanNode
    val booleanDefault = record.get("fieldWithBooleanDefault")
    booleanDefault shouldBe a[java.lang.Boolean]
    booleanDefault.asInstanceOf[java.lang.Boolean].booleanValue() should be(true)
  }

  test("should use properly typed default values for nested struct fields") {
    // Create an Avro schema with a nested struct that has default values
    val avroSchemaJson =
      """
        |{
        |  "type": "record",
        |  "name": "OuterRecord",
        |  "namespace": "test",
        |  "fields": [
        |    {"name": "id", "type": "string"},
        |    {
        |      "name": "nested",
        |      "type": {
        |        "type": "record",
        |        "name": "NestedRecord",
        |        "fields": [
        |          {"name": "existingField", "type": "string"},
        |          {"name": "newFieldWithDefault", "type": "long", "default": 12345}
        |        ]
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    val avroSchema = new Schema.Parser().parse(avroSchemaJson)

    // Create Connect schemas - the nested schema is missing the newFieldWithDefault
    val nestedConnectSchema = SchemaBuilder.struct()
      .name("test.NestedRecord")
      .field("existingField", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
      .build()

    val outerConnectSchema = SchemaBuilder.struct()
      .name("test.OuterRecord")
      .field("id", org.apache.kafka.connect.data.Schema.STRING_SCHEMA)
      .field("nested", nestedConnectSchema)
      .build()

    val nestedStruct = new Struct(nestedConnectSchema)
      .put("existingField", "nestedValue")

    val outerStruct = new Struct(outerConnectSchema)
      .put("id", "outer123")
      .put("nested", nestedStruct)

    val sinkData = StructSinkData(outerStruct)

    val result = ToAvroDataConverter.convertToGenericRecordWithSchema(sinkData, avroSchema)

    result shouldBe a[GenericRecord]
    val record       = result.asInstanceOf[GenericRecord]
    val nestedRecord = record.get("nested").asInstanceOf[GenericRecord]

    record.get("id").toString should be("outer123")
    nestedRecord.get("existingField").toString should be("nestedValue")

    // Verify the default value in nested struct is properly typed
    val longDefault = nestedRecord.get("newFieldWithDefault")
    longDefault shouldBe a[java.lang.Long]
    longDefault should be(12345L)
  }

}
