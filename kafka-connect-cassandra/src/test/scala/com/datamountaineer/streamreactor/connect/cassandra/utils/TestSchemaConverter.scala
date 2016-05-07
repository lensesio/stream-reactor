//package com.datamountaineer.streamreactor.connect.cassandra.utils
//
//import java.net.InetAddress
//import java.nio.ByteBuffer
//
//import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
//import com.datastax.driver.core.{ColumnDefinitions, Row, TestUtils, TupleValue}
//import org.apache.kafka.connect.data.{Schema, Struct}
//import org.scalatest.mock.MockitoSugar
//import org.scalatest.{Matchers, WordSpec}
//import org.mockito.Mockito._
//
//import scala.collection.JavaConverters._
//
///**
//  * Created by andrew@datamountaineer.com on 21/04/16.
//  * stream-reactor
//  */
//class TestSchemaConverter extends WordSpec with TestConfig with Matchers with MockitoSugar {
//    "should convert a Cassandra row schema to a Connect schema" in {
//      val cols: ColumnDefinitions = TestUtils.getColumnDefs
//      val schema = CassandraUtils.convertToConnectSchema(cols.asScala.toList)
//      val schemaFields = schema.fields().asScala
//      schemaFields.size shouldBe(cols.asList().size())
//      checkCols(schema)
//    }
//
//  "should convert a Cassandra row to a SourceRecord" in {
//    val row = mock[Row]
//    val cols = TestUtils.getColumnDefs
//    when(row.getColumnDefinitions()).thenReturn(cols)
//    mockRow(row)
//    val sr: Struct = CassandraUtils.convert(row)
//    val schema = sr.schema()
//    checkCols(schema)
//    sr.get("timeuuidCol") shouldBe("111111")
//    sr.get("intCol") shouldBe(0)
//    sr.get("mapCol") shouldBe("{}")
//  }
//
//
//  def mockRow(row: Row) = {
//    when(row.getString("uuid")).thenReturn("string")
//    when(row.getInet("inetCol")).thenReturn(InetAddress.getByName("127.0.0.1"))
//    when(row.getString("asciiCol")).thenReturn("string")
//    when(row.getString("textCol")).thenReturn("string")
//    when(row.getString("varcharCol")).thenReturn("string")
//    when(row.getBool("booleanCol")).thenReturn(true)
//    when(row.getShort("smallintCol")).thenReturn(0.toShort)
//    when(row.getInt("intCol")).thenReturn(0)
//    when(row.getDecimal("decimalCol")).thenReturn(new java.math.BigDecimal(0))
//    when(row.getFloat("floatCol")).thenReturn(0)
//    when(row.getLong("counterCol")).thenReturn(0.toLong)
//    when(row.getLong("bigintCol")).thenReturn(0.toLong)
//    when(row.getLong("varintCol")).thenReturn(0.toLong)
//    when(row.getDouble("doubleCol")).thenReturn(0.toDouble)
//    when(row.getString("timeuuidCol")).thenReturn("111111")
//    when(row.getBytes("blobCol")).thenReturn(ByteBuffer.allocate(10))
//    when(row.getSet("setCol", classOf[String])).thenReturn(new java.util.HashSet[String])
//    when(row.getList("listCol", classOf[String])).thenReturn(new java.util.ArrayList[String])
//    when(row.getMap("mapCol", classOf[String], classOf[String])).thenReturn(new java.util.HashMap[String, String])
//    when(row.getDate("dateCol")).thenReturn(com.datastax.driver.core.LocalDate.fromDaysSinceEpoch(1))
//    when(row.getTime("timeCol")).thenReturn(0)
//    when(row.getTimestamp("timestampCol")).thenReturn(new java.util.Date)
//    //when(row.getTupleValue("tupleCol")).thenReturn(new TupleValue("tuple"))
//  }
//
//  def checkCols(schema :Schema) = {
//    schema.field("uuidCol").schema().`type`() shouldBe(Schema.OPTIONAL_STRING_SCHEMA.`type`())
//    schema.field("inetCol").schema().`type`() shouldBe(Schema.OPTIONAL_STRING_SCHEMA.`type`())
//    schema.field("asciiCol").schema().`type`() shouldBe(Schema.OPTIONAL_STRING_SCHEMA.`type`())
//    schema.field("textCol").schema().`type`() shouldBe(Schema.OPTIONAL_STRING_SCHEMA.`type`())
//    schema.field("varcharCol").schema().`type`() shouldBe(Schema.OPTIONAL_STRING_SCHEMA.`type`())
//    schema.field("booleanCol").schema().`type`() shouldBe(Schema.OPTIONAL_BOOLEAN_SCHEMA.`type`())
//    schema.field("smallintCol").schema().`type`() shouldBe(Schema.INT16_SCHEMA.`type`())
//    schema.field("intCol").schema().`type`() shouldBe(Schema.OPTIONAL_INT32_SCHEMA.`type`())
//    schema.field("decimalCol").schema().`type`() shouldBe(Schema.OPTIONAL_STRING_SCHEMA.`type`())
//    schema.field("floatCol").schema().`type`() shouldBe(Schema.OPTIONAL_FLOAT32_SCHEMA.`type`())
//    schema.field("counterCol").schema().`type`() shouldBe(Schema.OPTIONAL_INT64_SCHEMA.`type`())
//    schema.field("bigintCol").schema().`type`() shouldBe(Schema.OPTIONAL_INT64_SCHEMA.`type`())
//    schema.field("varintCol").schema().`type`() shouldBe(Schema.OPTIONAL_INT64_SCHEMA.`type`())
//    schema.field("doubleCol").schema().`type`() shouldBe(Schema.OPTIONAL_INT64_SCHEMA.`type`())
//    schema.field("timeuuidCol").schema().`type`() shouldBe(Schema.OPTIONAL_STRING_SCHEMA.`type`())
//    schema.field("blobCol").schema().`type`() shouldBe(Schema.OPTIONAL_BYTES_SCHEMA.`type`())
//    schema.field("timeCol").schema().`type`() shouldBe(Schema.OPTIONAL_INT64_SCHEMA.`type`())
//    schema.field("timestampCol").schema().`type`() shouldBe(Schema.OPTIONAL_STRING_SCHEMA.`type`())
//    schema.field("dateCol").schema().`type`() shouldBe(Schema.OPTIONAL_STRING_SCHEMA.`type`())
//  }
//}
