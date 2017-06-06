/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.cassandra.utils

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.UUID

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datastax.driver.core.{ ColumnDefinitions, Row, TestUtils }
import org.apache.kafka.connect.data.{ Schema, Struct }
import org.apache.kafka.connect.errors.DataException
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{ Matchers, WordSpec }

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 21/04/16.
  * stream-reactor
  */
class TestSchemaConverter extends WordSpec with TestConfig with Matchers with MockitoSugar {

  val uuid = UUID.randomUUID()

  "should handle null when converting a Cassandra row schema to a Connect schema" in {
    val schema = CassandraUtils.convertToConnectSchema(null, "test")
    val schemaFields = schema.fields().asScala
    schemaFields.size shouldBe 0
    schema.name() shouldBe "test"
  }

  "should convert a Cassandra row schema to a Connect schema" in {
    val cols: ColumnDefinitions = TestUtils.getColumnDefs
    val schema = CassandraUtils.convertToConnectSchema(cols.asScala.toList, "test")
    val schemaFields = schema.fields().asScala
    schemaFields.size shouldBe cols.asList().size()
    schema.name() shouldBe "test"
    checkCols(schema)
  }

  "should convert a Cassandra row to a Struct" in {
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)
    val colDefList = CassandraUtils.getStructColumns(row, null)
    val sr: Struct = CassandraUtils.convert(row, "test", colDefList)
    val schema = sr.schema()
    checkCols(schema)
    sr.get("timeuuidCol").toString shouldBe uuid.toString
    sr.get("intCol") shouldBe 0
    sr.get("mapCol") shouldBe "{}"
  }

  "should convert a Cassandra row to a Struct no columns" in {
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)
    val colDefList = null
    val sr: Struct = CassandraUtils.convert(row, "test", colDefList)
    val schema = sr.schema()
    schema.defaultValue() shouldBe null
  }

  "should convert a Cassandra row to a Struct and ignore some" in {
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    val ignoreList = List("intCol", "floatCol")
    val colDefList = CassandraUtils.getStructColumns(row, ignoreList)
    val sr: Struct = CassandraUtils.convert(row, "test", colDefList)

    sr.get("timeuuidCol").toString shouldBe uuid.toString
    sr.get("mapCol") shouldBe "{}"

    try {
      sr.get("intCol")
      fail()
    } catch {
      case _: DataException => // Expected, so continue
    }

    try {
      sr.get("floatCol")
      fail()
    } catch {
      case _: DataException => // Expected, so continue
    }

  }

  def mockRow(row: Row) = {
    when(row.getString("uuid")).thenReturn("string")
    when(row.getInet("inetCol")).thenReturn(InetAddress.getByName("127.0.0.1"))
    when(row.getString("asciiCol")).thenReturn("string")
    when(row.getString("textCol")).thenReturn("string")
    when(row.getString("varcharCol")).thenReturn("string")
    when(row.getBool("booleanCol")).thenReturn(true)
    when(row.getShort("smallintCol")).thenReturn(0.toShort)
    when(row.getInt("intCol")).thenReturn(0)
    when(row.getDecimal("decimalCol")).thenReturn(new java.math.BigDecimal(0))
    when(row.getFloat("floatCol")).thenReturn(0)
    when(row.getLong("counterCol")).thenReturn(0.toLong)
    when(row.getLong("bigintCol")).thenReturn(0.toLong)
    when(row.getLong("varintCol")).thenReturn(0.toLong)
    when(row.getDouble("doubleCol")).thenReturn(0.toDouble)
    when(row.getString("timeuuidCol")).thenReturn("111111")
    when(row.getBytes("blobCol")).thenReturn(ByteBuffer.allocate(10))
    when(row.getSet("setCol", classOf[String])).thenReturn(new java.util.HashSet[String])
    when(row.getList("listCol", classOf[String])).thenReturn(new java.util.ArrayList[String])
    when(row.getMap("mapCol", classOf[String], classOf[String])).thenReturn(new java.util.HashMap[String, String])
    when(row.getDate("dateCol")).thenReturn(com.datastax.driver.core.LocalDate.fromDaysSinceEpoch(1))
    when(row.getTime("timeCol")).thenReturn(0)
    when(row.getTimestamp("timestampCol")).thenReturn(new java.util.Date)
    when(row.getUUID("timeuuidCol")).thenReturn(uuid)
    //when(row.getTupleValue("tupleCol")).thenReturn(new TupleValue("tuple"))
  }

  def checkCols(schema: Schema) = {
    schema.field("uuidCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("inetCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("asciiCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("textCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("varcharCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("booleanCol").schema().`type`() shouldBe Schema.OPTIONAL_BOOLEAN_SCHEMA.`type`()
    schema.field("smallintCol").schema().`type`() shouldBe Schema.INT16_SCHEMA.`type`()
    schema.field("intCol").schema().`type`() shouldBe Schema.OPTIONAL_INT32_SCHEMA.`type`()
    schema.field("decimalCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("floatCol").schema().`type`() shouldBe Schema.OPTIONAL_FLOAT32_SCHEMA.`type`()
    schema.field("counterCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("bigintCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("varintCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("doubleCol").schema().`type`() shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA.`type`()
    schema.field("timeuuidCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("blobCol").schema().`type`() shouldBe Schema.OPTIONAL_BYTES_SCHEMA.`type`()
    schema.field("timeCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("timestampCol").schema().`type`() shouldBe CassandraUtils.OPTIONAL_TIMESTAMP_SCHEMA.`type`()
    schema.field("dateCol").schema().`type`() shouldBe CassandraUtils.OPTIONAL_DATE_SCHEMA.`type`()
  }
}
