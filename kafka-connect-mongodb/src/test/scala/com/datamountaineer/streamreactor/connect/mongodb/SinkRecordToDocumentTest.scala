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

package com.datamountaineer.streamreactor.connect.mongodb

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.errors.NoopErrorPolicy
import com.datamountaineer.streamreactor.connect.mongodb.Transaction._
import com.datamountaineer.streamreactor.connect.mongodb.config.MongoSettings
import com.datamountaineer.streamreactor.connect.mongodb.sink.SinkRecordToDocument
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import com.mongodb.AuthenticationMechanism
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SinkRecordToDocumentTest extends AnyWordSpec with Matchers with ConverterUtil {
  "SinkRecordToDocument" should {
    "convert Kafka Struct to a Mongo Document" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        val record = new SinkRecord("topic1", 0, null, null, Transaction.ConnectSchema, tx.toStruct(), 0)

        implicit val settings = MongoSettings(
          "",
          "",
          new Password(""),
          AuthenticationMechanism.SCRAM_SHA_1,
          "database",
          Set.empty,
          Map("topic1" -> Set.empty),
          Map("topic1" -> Map.empty[String, String]),
          Map("topic1" -> Set.empty),
          NoopErrorPolicy())
        val (document, _) = SinkRecordToDocument(record)
        val expected = Document.parse(json)

        //comparing string representation; we have more specific types given the schema
        document.toString shouldBe expected.toString
      }
    }

    "convert String Schema + Json payload to a Mongo Document" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString

        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        implicit val settings = MongoSettings(
          "",
          "",
          new Password(""),
          AuthenticationMechanism.SCRAM_SHA_1,
          "database",
          Set(Kcql.parse("insert into x select * from topic1")),
          Map("topic1" -> Set.empty),
          Map("topic1" -> Map("*" -> "*")),
          Map("topic1" -> Set.empty),
          NoopErrorPolicy())
        val (document, _) = SinkRecordToDocument(record)
        val expected = Document.parse(json)

        //comparing string representation; we have more specific types given the schema
        document.toString() shouldBe expected.toString
      }
    }

    "convert String Schema + Json payload to a Mongo Document with selection" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString

        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)
        val kcql = Kcql.parse("insert into x select * from topic1")

        implicit val settings = MongoSettings(
          connection = "",
          username = "",
          password = new Password(""),
          authenticationMechanism = AuthenticationMechanism.SCRAM_SHA_1,
          database = "database",
          kcql = Set(kcql),
          keyBuilderMap = Map("topic1" -> Set.empty),
          fields = Map("topic1" -> Map("lock_time" -> "colc")),
          ignoredField = Map.empty,
          NoopErrorPolicy()
        )

        val (document, _) = SinkRecordToDocument(record)
        document.size() shouldBe 1
        document.containsKey("colc")
    }
  }

    "convert Schemaless + Json payload to a Mongo Document" in {
      // TODO: This test is exactly the same as the above test "convert String Schema + Json payload to a Mongo Document".
      // This should probably test something different or be deleted.
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString

        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        implicit val settings = MongoSettings(
          "",
          "",
          new Password(""),
          AuthenticationMechanism.SCRAM_SHA_1,
          "database",
          Set(Kcql.parse("insert into x select * from topic1")),
          Map("topic1" -> Set.empty),
          Map("topic1" -> Map("*" -> "*")),
          Map("topic1" -> Set.empty),
          NoopErrorPolicy())
        val (document, _) = SinkRecordToDocument(record)
        val expected = Document.parse(json)

        //comparing string representation; we have more specific types given the schema
        document.toString() shouldBe expected.toString
      }
    }
  }
}
