/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package com.datamountaineer.streamreactor.connect.json
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.errors.DataException
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.math.BigDecimal
import java.math.BigInteger
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.time.temporal.ChronoUnit
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.TimeZone
import java.util.{ Date => JavaDate }
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class SimpleJsonConverterTest extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  private[json] val converter = new SimpleJsonConverter

  private val OLD_ISO_DATE_FORMAT = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    sdf
  }
  private val OLD_TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSSZ")
  "JsonSimpleConverter schema types" should {
    "booleanToJson" in {
      val converted = converter.fromConnectData(Schema.BOOLEAN_SCHEMA, true)
      converted.booleanValue should be(true)
    }
    "byteToJson" in {
      val converted = converter.fromConnectData(Schema.INT8_SCHEMA, 12.toByte)
      converted.intValue should be(12)
    }
    "shortToJson" in {
      val converted = converter.fromConnectData(Schema.INT16_SCHEMA, 12.toShort)
      converted.intValue should be(12)
    }
    "intToJson" in {
      val converted = converter.fromConnectData(Schema.INT32_SCHEMA, 12)
      converted.intValue should be(12)
    }
    "longToJson" in {
      val converted = converter.fromConnectData(Schema.INT64_SCHEMA, 4398046511104L)
      converted.longValue should be(4398046511104L)
    }

    "floatToJson" in {
      val converted = converter.fromConnectData(Schema.FLOAT32_SCHEMA, 12.34f)
      converted.floatValue should be(12.34f)
    }

    "doubleToJson" in {
      val converted = converter.fromConnectData(Schema.FLOAT64_SCHEMA, 12.34)
      converted.doubleValue should be(12.34)
    }

    "bytesToJson" in {
      val converted = converter.fromConnectData(Schema.BYTES_SCHEMA, "test-string".getBytes)
      ByteBuffer.wrap(converted.binaryValue) should be(ByteBuffer.wrap("test-string".getBytes))
    }

    "stringToJson" in {
      val converted = converter.fromConnectData(Schema.STRING_SCHEMA, "test-string")
      converted.textValue should be("test-string")
    }
    "arrayToJson" in {
      val int32Array = SchemaBuilder.array(Schema.INT32_SCHEMA).build
      val converted  = converter.fromConnectData(int32Array, List(1, 2, 3).asJava)
      converted should be(JsonNodeFactory.instance.arrayNode.add(1).add(2).add(3))
    }

    "mapToJsonStringKeys" in {
      val stringIntMap = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build
      val input = Map[String, Integer](
        "key1" -> 12,
        "key2" -> 15,
      )
      val converted = converter.fromConnectData(stringIntMap, input.asJava)
      converted should be(JsonNodeFactory.instance.objectNode.put("key1", 12).put("key2", 15))
    }

    "mapToJsonNonStringKeys" in {
      val intIntMap = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA).build
      val input = Map[Int, Int](
        1 -> 12,
        2 -> 15,
      )
      val converted = converter.fromConnectData(intIntMap, input.asJava)
      converted.isArray should be(true)
      val payload = converted.asInstanceOf[ArrayNode]
      payload should have size 2
      val payloadEntries = payload.asScala.toSet
      payloadEntries should be(
        Set(JsonNodeFactory.instance.arrayNode.add(1).add(12), JsonNodeFactory.instance.arrayNode.add(2).add(15)),
      )
    }
    "structToJson" in {
      val schema = SchemaBuilder.struct.field("field1", Schema.BOOLEAN_SCHEMA).field("field2",
                                                                                     Schema.STRING_SCHEMA,
      ).field("field3", Schema.STRING_SCHEMA).field("field4", Schema.BOOLEAN_SCHEMA).build
      val input =
        new Struct(schema).put("field1", true).put("field2", "string2").put("field3", "string3").put("field4", false)

      val converted = converter.fromConnectData(schema, input)
      converted should be(
        JsonNodeFactory.instance.objectNode.put("field1", true).put("field2", "string2").put("field3", "string3").put(
          "field4",
          false,
        ),
      )
    }

    "decimalToJson" in {
      val expectedDecimal = new BigDecimal(new BigInteger("156"), 2)
      val converted       = converter.fromConnectData(Decimal.schema(2), expectedDecimal)
      converted.decimalValue should be(expectedDecimal)
    }

    "dateToJson" in {
      val date =
        JavaDate.from(LocalDateTime.of(1970, 1, 1, 0, 0).plus(10000, ChronoUnit.DAYS).toInstant(ZoneOffset.UTC))
      val converted = converter.fromConnectData(Date.SCHEMA, date)
      converted.isTextual should be(true)
      converted.textValue should be(OLD_ISO_DATE_FORMAT.format(date))
    }

    "timeToJson" in {
      val time      = JavaDate.from(Instant.ofEpochMilli(14400000))
      val converted = converter.fromConnectData(Time.SCHEMA, time)
      converted.isTextual should be(true)
      converted.textValue should be(OLD_TIME_FORMAT.format(time))
    }

    "timestampToJson" in {
      val date      = JavaDate.from(Instant.ofEpochMilli(4000000000L))
      val converted = converter.fromConnectData(Timestamp.SCHEMA, date)
      converted.isLong should be(true)
      converted.longValue should be(4000000000L)
    }

    "nullSchemaAndPrimitiveToJson" in {
      // This still needs to do conversion of data, null schema means "anything goes"
      val converted = converter.fromConnectData(null, true)
      converted.booleanValue should be(true)
    }
    "nullSchemaAndArrayToJson" in {
      // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
      // types to verify conversion still works.
      val converted = converter.fromConnectData(null, List[Any](1, "string", true).asJava)
      converted should be(JsonNodeFactory.instance.arrayNode.add(1).add("string").add(true))
    }

    "nullSchemaAndMapToJson" in {
      // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
      // types to verify conversion still works.
      val input = Map[String, Any](
        "key1" -> 12,
        "key2" -> "string",
        "key3" -> true,
      )
      val converted = converter.fromConnectData(null, input.asJava)
      converted should be(JsonNodeFactory.instance.objectNode.put("key1", 12).put("key2", "string").put("key3", true))
    }

    "nullSchemaAndMapNonStringKeysToJson" in {
      // This still needs to do conversion of data, null schema means "anything goes". Make sure we mix and match
      // types to verify conversion still works.
      val input = Map[Any, Any](
        "string" -> 12,
        52       -> "string",
        false    -> true,
      )
      val converted = converter.fromConnectData(null, input.asJava)
      converted.isArray should be(true)
      val payload = converted.asInstanceOf[ArrayNode]
      payload.size should be(3)
      val payloadEntries = payload.iterator().asScala.toSet

      payloadEntries should be(
        Set(
          JsonNodeFactory.instance.arrayNode.add("string").add(12),
          JsonNodeFactory.instance.arrayNode.add(52).add("string"),
          JsonNodeFactory.instance.arrayNode.add(false).add(true),
        ),
      )

    }

    "mismatchSchemaJson" in {
      assertThrows[DataException] {
        // If we have mismatching schema info, we should properly convert to a DataException
        converter.fromConnectData(Schema.FLOAT64_SCHEMA, true)
      }
    }

    "noSchemaToJson" in {
      val converted = converter.fromConnectData(null, true)
      converted.isBoolean shouldBe true
      converted.booleanValue() shouldBe true
    }

    "identicalSchemasShouldNotMismatch" in {
      val schema1 = SchemaBuilder.struct.field("myString", Schema.STRING_SCHEMA).build
      val schema2 = SchemaBuilder.struct.field("myString", Schema.STRING_SCHEMA).build
      schema1 should be(schema2)
      val struct1 = new Struct(schema1)
      struct1.put("myString", "testString")
      val converted = converter.fromConnectData(schema2, struct1)
      converted.toString should be("{\"myString\":\"testString\"}")
    }
  }

}
