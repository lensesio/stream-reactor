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

package com.datamountaineer.streamreactor.connect.mongodb.sink

import com.datamountaineer.connector.config.{Config, WriteModeEnum}
import com.datamountaineer.streamreactor.connect.errors.{NoopErrorPolicy, ThrowErrorPolicy}
import com.datamountaineer.streamreactor.connect.mongodb.config.MongoSinkSettings
import com.datamountaineer.streamreactor.connect.mongodb.{Json, Transaction}
import com.mongodb.MongoClient
import com.mongodb.client.model.{Filters, InsertOneModel}
import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodProcess, MongodStarter}
import de.flapdoodle.embed.process.runtime.Network
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.bson.Document
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConversions._

class MongoWriterTest extends WordSpec with Matchers with BeforeAndAfterAll {

  val starter = MongodStarter.getDefaultInstance
  val port = 12345
  val mongodConfig = new MongodConfigBuilder()
    .version(Version.Main.PRODUCTION)
    .net(new Net(port, Network.localhostIsIPv6()))
    .build()

  var mongodExecutable: Option[MongodExecutable] = None
  var mongod: Option[MongodProcess] = None
  var mongoClient: Option[MongoClient] = None

  override def beforeAll() = {
    mongodExecutable = Some(starter.prepare(mongodConfig))
    mongod = mongodExecutable.map(_.start())
    mongoClient = Some(new MongoClient("localhost", port))
  }

  override def afterAll() = {
    mongod.foreach(_.stop())
    mongodExecutable.foreach(_.stop())
  }

  "MongoWriter" should {
    "insert records into the target Mongo collection with Schema.String and payload json" in {
      val settings = MongoSinkSettings("localhost",
        "local",
        Seq(Config.parse("INSERT INTO insert_string_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        new SinkRecord("topicA", 0, null, null, Schema.STRING_SCHEMA, json, i)
      }

      runInserts(records, settings)
    }

    "upsert records into the target Mongo collection with Schema.String and payload json" in {
      val settings = MongoSinkSettings("localhost",
        "local",
        Seq(Config.parse("UPSERT INTO upsert_string_json SELECT * FROM topicA PK lock_time")),
        Map("topicA" -> Set("lock_time")),
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
          .replace("\"size\": 807", "\"size\": 1010" + (i - 1))
        new SinkRecord("topicA", 0, null, null, Schema.STRING_SCHEMA, json, i)
      }

      runUpserts(records, settings)
    }



    "insert records into the target Mongo collection with Schema.Struct and payload Struct" in {
      val settings = MongoSinkSettings("localhost",
        "local",
        Seq(Config.parse("INSERT INTO insert_struct SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        new SinkRecord("topicA", 0, null, null, Transaction.ConnectSchema, tx.toStruct(), i)
      }
      runInserts(records, settings)
    }

    "upsert records into the target Mongo collection with Schema.Struct and payload Struct" in {
      val settings = MongoSinkSettings("localhost",
        "local",
        Seq(Config.parse("UPSERT INTO upsert_struct SELECT * FROM topicA PK lock_time")),
        Map("topicA" -> Set("lock_time")),
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        ThrowErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        new SinkRecord("topicA", 0, null, null, Transaction.ConnectSchema, tx.copy(size = 10100 + (i - 1)).toStruct(), i)
      }
      runUpserts(records, settings)
    }

    "insert records into the target Mongo collection with schemaless records and payload as json" in {
      val settings = MongoSinkSettings("localhost",
        "local",
        Seq(Config.parse("INSERT INTO insert_schemaless_json SELECT * FROM topicA")),
        Map.empty,
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        new SinkRecord("topicA", 0, null, null, null, tx.toHashMap, i)
      }
      runInserts(records, settings)
    }

    "upsert records into the target Mongo collection with schemaless records and payload as json" in {
      val settings = MongoSinkSettings("localhost",
        "local",
        Seq(Config.parse("INSERT INTO upsert_schemaless_json SELECT * FROM topicA")),
        Map("topicA" -> Set("lock_time")),
        Map("topicA" -> Map.empty),
        Map("topicA" -> Set.empty),
        NoopErrorPolicy())

      val records = for (i <- 1 to 4) yield {
        val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
        val tx = Json.fromJson[Transaction](json)

        new SinkRecord("topicA", 0, null, null, null, tx.copy(size = 10100 + (i - 1)).toHashMap, i)
      }
      runInserts(records, settings)
    }
  }

  private def runInserts(records: Seq[SinkRecord], settings: MongoSinkSettings) = {
    val mongoWriter = new MongoWriter(settings, mongoClient.get)
    mongoWriter.write(records)

    val databases = MongoIterableFn(mongoClient.get.listDatabaseNames()).toSet
    databases.contains(settings.database) shouldBe true

    val collections = MongoIterableFn(mongoClient.get.getDatabase(settings.database).listCollectionNames())
      .toSet

    val collectionName = settings.kcql.head.getTarget
    collections.contains(collectionName) shouldBe true

    val actualCollection = mongoClient.get
      .getDatabase(settings.database)
      .getCollection(collectionName)

    actualCollection.count() shouldBe 4

    actualCollection.count(Filters.eq("lock_time", 9223372036854775807L)) shouldBe 1
    actualCollection.count(Filters.eq("lock_time", 427856)) shouldBe 1
    actualCollection.count(Filters.eq("lock_time", 7856)) shouldBe 1
    actualCollection.count(Filters.eq("lock_time", 0)) shouldBe 1
  }


  private def runUpserts(records: Seq[SinkRecord], settings: MongoSinkSettings) = {
    require(settings.kcql.size == 1)
    require(settings.kcql.head.getWriteMode == WriteModeEnum.UPSERT)
    val db = mongoClient.get.getDatabase(settings.database)
    db.createCollection(settings.kcql.head.getTarget)
    val collection = db.getCollection(settings.kcql.head.getTarget)
    val inserts = for (i <- 1 to 4) yield {
      val json = scala.io.Source.fromFile(getClass.getResource(s"/transaction$i.json").toURI.getPath).mkString
      val tx = Json.fromJson[Transaction](json)
      val doc = new Document(tx.toHashMap.asInstanceOf[java.util.HashMap[String, AnyRef]])
      new InsertOneModel[Document](doc)
    }
    collection.bulkWrite(inserts)

    val mongoWriter = new MongoWriter(settings, mongoClient.get)
    mongoWriter.write(records)

    val databases = MongoIterableFn(mongoClient.get.listDatabaseNames()).toSet
    databases.contains(settings.database) shouldBe true

    val collections = MongoIterableFn(mongoClient.get.getDatabase(settings.database).listCollectionNames())
      .toSet

    val collectionName = settings.kcql.head.getTarget
    collections.contains(collectionName) shouldBe true

    val actualCollection = mongoClient.get
      .getDatabase(settings.database)
      .getCollection(collectionName)

    actualCollection.count() shouldBe 4

    val keys = Seq(9223372036854775807L, 427856L, 0L, 7856L)
    keys.zipWithIndex.foreach { case (k, index) =>
      var docOption = MongoIterableFn(actualCollection.find(Filters.eq("lock_time", k))).headOption
      docOption.isDefined shouldBe true
      docOption.get.get("size") shouldBe 10100 + index
    }
  }
}
