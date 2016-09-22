package com.datamountaineer.streamreactor.connect.rethink.sink

import com.datamountaineer.streamreactor.connect.rethink.TestBase
import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSinkSettings, ReThinkSinkConfig}
import com.rethinkdb.RethinkDB
import com.rethinkdb.gen.ast.{Db, TableCreate, TableList}
import com.rethinkdb.model.MapObject
import com.rethinkdb.net.Connection
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/06/16. 
  * stream-reactor-maven
  */
class TestReThinkSinkConverter extends TestBase with MockitoSugar {
  "should convert a SinkRecord to a ReThink MapObject" in {
    val records = getTestRecords
    val rethink = RethinkDB.r
    val pks = Set("string_id", "int_field")
    val mo = ReThinkSinkConverter.convertToReThink(rethink, records.head, pks)
    mo.containsKey("string_id") shouldBe true
    mo.get("string_id") shouldBe "rethink_topic-12-1"
    mo.containsKey("int_field") shouldBe true
    mo.get("int_field") shouldBe 12
    mo.containsKey("long_field") shouldBe true
    mo.get("long_field") shouldBe 12
  }

  "should convert a SinkRecord to a ReThink MapObject using default primary key" in {
    val records = getTestRecords
    val rethink = RethinkDB.r
    val mo = ReThinkSinkConverter.convertToReThink(rethink, records.head, Set.empty[String])
    mo.containsKey("id") shouldBe true
    mo.get("id") shouldBe "rethink_topic-12-1"
    mo.containsKey("string_id") shouldBe true
    mo.get("string_id") shouldBe "rethink_topic-12-1"
    mo.containsKey("int_field") shouldBe true
    mo.get("int_field") shouldBe 12
    mo.containsKey("long_field") shouldBe true
    mo.get("long_field") shouldBe 12
  }

  "should convert a SinkRecord with nested type to a ReThink MapObject using default primary key" in {
    val nested =   SchemaBuilder.struct.name("record")
                      .version(1)
                      .field("nested_string_id", Schema.STRING_SCHEMA)
                      .field("nested_int_field", Schema.INT32_SCHEMA)
                      .field("nested_long_field", Schema.INT64_SCHEMA)
                      .field("nested_string_field", Schema.STRING_SCHEMA)
                      .build

    val nestedStruct = new Struct(nested)
                        .put("nested_string_id", "my_id_val")
                        .put("nested_int_field", 12)
                        .put("nested_long_field", 12L)
                        .put("nested_string_field", "foo")

    val schema = SchemaBuilder.struct.name("record")
                      .version(1)
                      .field("string_id", Schema.STRING_SCHEMA)
                      .field("int_field", Schema.INT32_SCHEMA)
                      .field("long_field", Schema.INT64_SCHEMA)
                      .field("string_field", Schema.STRING_SCHEMA)
                      .field("my_struct", nested)
                      .build

    val struct = new Struct(schema)
                    .put("string_id", "my_id_val")
                    .put("int_field", 12)
                    .put("long_field", 12L)
                    .put("string_field", "foo")
                    .put("my_struct", nestedStruct)

    val record = new SinkRecord(TOPIC, 1, Schema.STRING_SCHEMA, "key", schema, struct, 1)

    val rethink = RethinkDB.r
    val mo = ReThinkSinkConverter.convertToReThink(rethink, record, Set.empty[String])
    mo.containsKey("id") shouldBe true
    mo.get("id") shouldBe "rethink_topic-1-1"
    mo.containsKey("string_id") shouldBe true
    mo.get("string_id") shouldBe "my_id_val"
    mo.containsKey("int_field") shouldBe true
    mo.get("int_field") shouldBe 12
    mo.containsKey("long_field") shouldBe true
    mo.get("long_field") shouldBe 12

    mo.containsKey("my_struct") shouldBe true
    val x = mo.get("my_struct").asInstanceOf[MapObject]
    x.get("nested_string_id") shouldBe "my_id_val"
  }

  "should check and create tables" in {
    val props = getPropsUpsertSelectRetry
    val config = new ReThinkSinkConfig(props = props)
    val settings = ReThinkSinkSettings(config)
    val r = mock[RethinkDB]
    val conn = mock[Connection]
    val db =  mock[Db]
    val tableList = mock[TableList]
    val tableCreate = mock[TableCreate]

    when(r.db(settings.db)).thenReturn(db)
    when(db.tableList()).thenReturn(tableList)
    when(r.db(DB).tableList().run(conn)).thenReturn(List.empty[String].asJava)
    when(r.db(DB).tableCreate(TABLE)).thenReturn(tableCreate)
    when(tableCreate.optArg(any[String], any[String])).thenReturn(tableCreate)

    ReThinkSinkConverter.checkAndCreateTables(r, settings, conn)
  }
}
