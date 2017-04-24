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

package com.datamountaineer.streamreactor.connect.rethink.sink

import java.util

import com.datamountaineer.streamreactor.connect.rethink.TestBase
import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkSinkConfig, ReThinkSinkSettings}
import com.rethinkdb.RethinkDB
import com.rethinkdb.gen.ast.{Db, TableCreate, TableList}
import com.rethinkdb.net.Connection
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.mockito.Matchers.{any, eq => mockEq}
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 27/06/16. 
  * stream-reactor-maven
  */
class TestReThinkSinkConverter extends TestBase with MockitoSugar {
  "SinkRecordConversion" should {
    "convert a SinkRecord to a ReThink MapObject" in {
      val records = getTestRecords

      val pks = Set("string_id", "int_field")
      val mo = SinkRecordConversion.fromStruct(records.head, pks)
      mo.containsKey("string_id") shouldBe true
      mo.get("string_id") shouldBe "rethink_topic-12-1"
      mo.containsKey("int_field") shouldBe true
      mo.get("int_field") shouldBe 12
      mo.containsKey("long_field") shouldBe true
      mo.get("long_field") shouldBe 12
    }

    "convert a SinkRecord to a ReThink MapObject using default primary key" in {
      val records = getTestRecords
      val mo = SinkRecordConversion.fromStruct(records.head, Set.empty[String])
      mo.containsKey("id") shouldBe true
      mo.get("id") shouldBe "rethink_topic-12-1"
      mo.containsKey("string_id") shouldBe true
      mo.get("string_id") shouldBe "rethink_topic-12-1"
      mo.containsKey("int_field") shouldBe true
      mo.get("int_field") shouldBe 12
      mo.containsKey("long_field") shouldBe true
      mo.get("long_field") shouldBe 12
    }

    "convert a SinkRecord with nested type to a ReThink MapObject using default primary key" in {
      val nested = SchemaBuilder.struct.name("record")
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

      val mo = SinkRecordConversion.fromStruct(record, Set.empty[String])
      mo.containsKey("id") shouldBe true
      mo.get("id") shouldBe "rethink_topic-1-1"
      mo.containsKey("string_id") shouldBe true
      mo.get("string_id") shouldBe "my_id_val"
      mo.containsKey("int_field") shouldBe true
      mo.get("int_field") shouldBe 12
      mo.containsKey("long_field") shouldBe true
      mo.get("long_field") shouldBe 12

      mo.containsKey("my_struct") shouldBe true
      val x = mo.get("my_struct").asInstanceOf[util.HashMap[String, Any]]
      x.get("nested_string_id") shouldBe "my_id_val"
    }

    "check and create tables" in {
      val props = getPropsUpsertSelectRetry
      val config = new ReThinkSinkConfig(props = props)
      val settings = ReThinkSinkSettings(config)
      val r = mock[RethinkDB]
      val conn = mock[Connection]
      val db = mock[Db]
      val tableList = mock[TableList]
      val tableCreate = mock[TableCreate]

      when(r.db(settings.database)).thenReturn(db)
      when(db.tableList()).thenReturn(tableList)
      when(r.db(DB).tableList().run(conn)).thenReturn(List.empty[String].asJava)
      when(r.db(DB).tableCreate(TABLE)).thenReturn(tableCreate)
      when(tableCreate.optArg(any[String], any[String])).thenReturn(tableCreate)

      ReThinkHelper.checkAndCreateTables(r, settings, conn)
    }

    "convert from a JValue to a hashmap" in {
      val jsonPayload =
        """
         { "name": "joe",
           "address": {
             "street": "Bulevard",
             "city": "Helsinki"
           },
           "children": [
             {
               "name": "Mary",
               "age": 5,
               "birthdate": "2004-09-04T18:06:22Z"
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }
        """
      val json = parse(jsonPayload)

      implicit val formats = DefaultFormats

      val record = new SinkRecord("topic", 0, null, null, Schema.STRING_SCHEMA, jsonPayload, 0)

      val actual = SinkRecordConversion.fromJson(record, json, Set.empty)
      actual.containsKey("id") shouldBe true
      actual.get("id") shouldBe s"${record.topic()}-${record.kafkaPartition().toString}-${record.kafkaOffset().toString}"

      actual.get("name") shouldBe "joe"
      val address = actual.get("address").asInstanceOf[util.HashMap[String, Any]]
      address.get("street") shouldBe "Bulevard"


      val list = actual.get("children").asInstanceOf[util.ArrayList[util.HashMap[String, Any]]]
      list.size() shouldBe 2

      list.get(0).get("birthdate") shouldBe "2004-09-04T18:06:22Z"
      list.get(1).containsKey("birthdate") shouldBe false
      list.get(1).get("age") shouldBe 3

    }
  }
}
