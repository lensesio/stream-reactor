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

package com.datamountaineer.streamreactor.connect.cassandra.sink
import scala.collection.JavaConversions._
import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.cassandra.config.CassandraSinkSetting
import com.datamountaineer.streamreactor.connect.converters.source.SinkRecordToJson
import com.datamountaineer.streamreactor.connect.errors.NoopErrorPolicy
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.landoop.json.sql.JacksonJson
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{Matchers, WordSpec}


class SinkRecordToJsonTest extends WordSpec with Matchers with ConverterUtil {
  "SinkRecordToDocument" should {
    "convert Kafka Struct to a JSON" in {
      implicit val settings = CassandraSinkSetting(
        "keyspace1",
        s"INSERT INTO topic1 SELECT * FROM topic1;INSERT INTO topic2 SELECT * FROM topic2".split(';').map(Kcql.parse),
        Map.empty,
        Map.empty,
        NoopErrorPolicy(),
        2,
        None)
      val kcqlMap=settings.kcqls.map{k=> k.getSource ->k}.toMap

      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/output$i.json").toURI.getPath).mkString
        val output = Json.fromJson[Output](json)

        val record = new SinkRecord("topic1", 0, null, null, Output.ConnectSchema, output.toStruct(), 0)

        val kcql= kcqlMap(record.topic())
        val actual = Transform(
          kcql.getFields.map(FieldConverter.apply),
          kcql.getIgnoredFields.map(FieldConverter.apply),
          record.valueSchema(),
          record.value(),
          kcql.hasRetainStructure())

        //comparing string representation; we have more specific types given the schema
        actual shouldBe JacksonJson.asJson(json).toString
      }
    }

    "convert String Schema + Json payload to JSON" in {
      implicit val settings = CassandraSinkSetting(
        "keyspace1",
        s"INSERT INTO topic1 SELECT * FROM topic1;INSERT INTO topic2 SELECT * FROM topic2".split(';').map(Kcql.parse),
        Map.empty,
        Map.empty,
        NoopErrorPolicy(),
        2,
        None)

      val kcqlMap=settings.kcqls.map{k=> k.getSource ->k}.toMap

      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/output$i.json").toURI.getPath).mkString

        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        val kcql= kcqlMap(record.topic())
        val actual = Transform(
          kcql.getFields.map(FieldConverter.apply),
          kcql.getIgnoredFields.map(FieldConverter.apply),
          record.valueSchema(),
          record.value(),
          kcql.hasRetainStructure())

        //comparing string representation; we have more specific types given the schema
        actual shouldBe JacksonJson.asJson(json).toString
      }
    }

    "convert Schemaless + Json payload to a Document" in {

      implicit val settings = CassandraSinkSetting(
        "keyspace1",
        s"INSERT INTO topic1 SELECT * FROM topic1;INSERT INTO topic2 SELECT * FROM topic2".split(';').map(Kcql.parse),
        Map.empty,
        Map.empty,
        NoopErrorPolicy(),
        2,
        None)

      val kcqlMap=settings.kcqls.map{k=> k.getSource ->k}.toMap

      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/output$i.json").toURI.getPath).mkString

        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        val kcql= kcqlMap(record.topic())
        val actual = Transform(
          kcql.getFields.map(FieldConverter.apply),
          kcql.getIgnoredFields.map(FieldConverter.apply),
          record.valueSchema(),
          record.value(),
          kcql.hasRetainStructure())

        //comparing string representation; we have more specific types given the schema
        actual shouldBe JacksonJson.asJson(json).toString
      }
    }
  }
}