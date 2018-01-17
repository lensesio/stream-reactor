package com.datamountaineer.streamreactor.connect.cassandra.source

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.UUID

import com.datamountaineer.streamreactor.connect.cassandra.TestConfig
import com.datamountaineer.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSource, CassandraSettings}
import com.datastax.driver.core.{CodecRegistry, _}
import org.apache.kafka.connect.data.{Decimal, Schema, Struct, Timestamp}
import org.apache.kafka.connect.errors.DataException
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class TestCassandraTypeConverter extends WordSpec
  with TestConfig
  with Matchers
  with MockitoSugar {

  val OPTIONAL_DATE_SCHEMA = org.apache.kafka.connect.data.Date.builder().optional().build()
  val OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build()
  val OPTIONAL_DECIMAL_SCHEMA = Decimal.builder(18).optional().build()
  val uuid = UUID.randomUUID()

  val codecRegistry: CodecRegistry = new CodecRegistry();

  "should handle null when converting a Cassandra row schema to a Connect schema" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val schema = cassandraTypeConverter.convertToConnectSchema(null, "test")
    val schemaFields = schema.fields().asScala
    schemaFields.size shouldBe 0
    schema.name() shouldBe "test"
  }

  "should convert a Cassandra row schema to a Connect schema" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val cols: ColumnDefinitions = TestUtils.getColumnDefs
    val schema = cassandraTypeConverter.convertToConnectSchema(cols.asScala.toList, "test")
    val schemaFields = schema.fields().asScala
    schemaFields.size shouldBe cols.asList().size()
    schema.name() shouldBe "test"
    checkCols(schema)
  }

  "should convert a Cassandra row to a Struct" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)
    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)
    sr.get("timeuuidCol").toString shouldBe uuid.toString
    sr.get("intCol") shouldBe 0
    sr.get("mapCol") shouldBe ('empty)
  }

  "should convert a Cassandra row to a Struct with map in sub struct" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    when(row.getMap("mapCol", classOf[String], classOf[String])).thenReturn(new java.util.HashMap[String,String] {
      put("sub1","sub1value");
    })

    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)

    sr.getMap("mapCol").get("sub1").toString shouldBe "sub1value"
  }

  "should convert a Cassandra row to a Struct with map in json" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(true))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    when(row.getMap("mapCol", classOf[String], classOf[String])).thenReturn(new java.util.HashMap[String,String] {
      put("sub1","sub1value");
    })

    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)

    sr.get("mapCol") shouldBe "{\"sub1\":\"sub1value\"}"
  }

  "should convert a Cassandra row to a Struct with list in sub struct" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    when(row.getList("listCol", classOf[String])).thenReturn(new java.util.ArrayList[String]{
      add("A");
      add("B");
      add("C");
    })

    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)

    sr.getArray("listCol").get(0).toString shouldBe "A"
    sr.getArray("listCol").get(1).toString shouldBe "B"
    sr.getArray("listCol").get(2).toString shouldBe "C"
  }

  "should convert a Cassandra row to a Struct with list in json" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(true))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    when(row.getList("listCol", classOf[String])).thenReturn(new java.util.ArrayList[String]{
      add("A");
      add("B");
      add("C");
    })

    val colDefList = cassandraTypeConverter.getStructColumns(row, Set.empty)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    checkCols(schema)

    sr.get("listCol") shouldBe "[\"A\",\"B\",\"C\"]"
  }

  "should convert a Cassandra row to a Struct no columns" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)
    val colDefList = null
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)
    val schema = sr.schema()
    schema.defaultValue() shouldBe null
  }

  "should convert a Cassandra row to a Struct and ignore some" in {
    val cassandraTypeConverter = new CassandraTypeConverter(codecRegistry = codecRegistry, setting = getSettings(false))
    val row = mock[Row]
    val cols = TestUtils.getColumnDefs
    when(row.getColumnDefinitions).thenReturn(cols)
    mockRow(row)

    val ignoreList = Set("intCol", "floatCol")
    val colDefList = cassandraTypeConverter.getStructColumns(row, ignoreList)
    val sr: Struct = cassandraTypeConverter.convert(row, "test", colDefList, None)

    sr.get("timeuuidCol").toString shouldBe uuid.toString
    sr.get("mapCol") shouldBe ('empty)

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
    schema.field("decimalCol").schema().`type`() shouldBe OPTIONAL_DECIMAL_SCHEMA.`type`()
    schema.field("floatCol").schema().`type`() shouldBe Schema.OPTIONAL_FLOAT32_SCHEMA.`type`()
    schema.field("counterCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("bigintCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("varintCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("doubleCol").schema().`type`() shouldBe Schema.OPTIONAL_FLOAT64_SCHEMA.`type`()
    schema.field("timeuuidCol").schema().`type`() shouldBe Schema.OPTIONAL_STRING_SCHEMA.`type`()
    schema.field("blobCol").schema().`type`() shouldBe Schema.OPTIONAL_BYTES_SCHEMA.`type`()
    schema.field("timeCol").schema().`type`() shouldBe Schema.OPTIONAL_INT64_SCHEMA.`type`()
    schema.field("timestampCol").schema().`type`() shouldBe OPTIONAL_TIMESTAMP_SCHEMA.`type`()
    schema.field("dateCol").schema().`type`() shouldBe OPTIONAL_DATE_SCHEMA.`type`()
  }

  def getSettings(mappingCollectionToJson: Boolean) = {
    val config = Map(
      CassandraConfigConstants.CONTACT_POINTS -> CONTACT_POINT,
      CassandraConfigConstants.KEY_SPACE -> CASSANDRA_SINK_KEYSPACE,
      CassandraConfigConstants.USERNAME -> USERNAME,
      CassandraConfigConstants.PASSWD -> PASSWD,
      CassandraConfigConstants.KCQL -> "INSERT INTO cassandra-source SELECT * FROM orders PK created",
      CassandraConfigConstants.POLL_INTERVAL -> "1000",
      CassandraConfigConstants.MAPPING_COLLECTION_TO_JSON -> mappingCollectionToJson.toString
    )
    val taskConfig = CassandraConfigSource(config);
    CassandraSettings.configureSource(taskConfig).toList.head
  }

}
