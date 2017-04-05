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

import com.datamountaineer.streamreactor.connect.errors.NoopErrorPolicy
import com.datamountaineer.streamreactor.connect.mongodb.Transaction._
import com.datamountaineer.streamreactor.connect.mongodb.config.MongoSinkSettings
import com.datamountaineer.streamreactor.connect.mongodb.sink.SinkRecordToDocument
import com.datamountaineer.streamreactor.connect.schemas.ConverterUtil
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document
import org.scalatest.{Matchers, WordSpec}

class SinkRecordToDocumentTest extends WordSpec with Matchers with ConverterUtil {
  "SinkRecordToDocument" should {
    "convert Kafka Struct to a Mongo Document" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        val record = new SinkRecord("topic1", 0, null, null, Transaction.ConnectSchema, tx.toStruct(), 0)

        implicit val settings = MongoSinkSettings(
          "",
          "database",
          Seq.empty,
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

        implicit val settings = MongoSinkSettings(
          "",
          "database",
          Seq.empty,
          Map("topic1" -> Set.empty),
          Map("topic1" -> Map.empty[String, String]),
          Map("topic1" -> Set.empty),
          NoopErrorPolicy())
        val (document, _) = SinkRecordToDocument(record)
        val expected = Document.parse(json)

        //comparing string representation; we have more specific types given the schema
        document.toString() shouldBe expected.toString
      }
    }

    "convert Schemaless + Json payload to a Mongo Document" in {
      for (i <- 1 to 4) {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString


        val record = new SinkRecord("topic1", 0, null, null, Schema.STRING_SCHEMA, json, 0)

        implicit val settings = MongoSinkSettings(
          "",
          "database",
          Seq.empty,
          Map("topic1" -> Set.empty),
          Map("topic1" -> Map.empty[String, String]),
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
