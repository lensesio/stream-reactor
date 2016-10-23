package com.datamountaineer.streamreactor.connect.mongodb.sink

import com.datamountaineer.connector.config.Config
import com.datamountaineer.streamreactor.connect.errors.NoopErrorPolicy
import com.datamountaineer.streamreactor.connect.mongodb.config.MongoSinkSettings
import com.datamountaineer.streamreactor.connect.mongodb.{Json, Transaction}
import com.mongodb.MongoClient
import com.mongodb.client.model.Filters
import de.flapdoodle.embed.mongo.config.{MongodConfigBuilder, Net}
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodProcess, MongodStarter}
import de.flapdoodle.embed.process.runtime.Network
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}


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
      val settings = MongoSinkSettings(Seq("localhost"),
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

    "insert records into the target Mongo collection with Schema.Struct and payload Struct" in {
      val settings = MongoSinkSettings(Seq("localhost"),
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

    "insert records into the target Mongo collection with schemaless records and payload as json" in {
      val settings = MongoSinkSettings(Seq("localhost"),
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
  }

  private def runInserts(records: Seq[SinkRecord], settings: MongoSinkSettings) = {
    val mongoWriter = new MongoWriter(settings, mongoClient.get)
    mongoWriter.write(records)

    val databases = MongoIterableFn(mongoClient.get.listDatabaseNames()).toSet
    databases.contains(settings.database) shouldBe true

    val collections = MongoIterableFn(mongoClient.get.getDatabase(settings.database).listCollectionNames())
      .toSet

    val collectionName = settings.routes.head.getTarget
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
}
