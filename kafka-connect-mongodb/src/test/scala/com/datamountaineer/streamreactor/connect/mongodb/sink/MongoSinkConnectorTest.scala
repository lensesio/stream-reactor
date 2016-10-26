package com.datamountaineer.streamreactor.connect.mongodb.sink

import com.datamountaineer.streamreactor.connect.mongodb.config.MongoConfig
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class MongoSinkConnectorTest extends WordSpec with Matchers {
  "MongoSinkConnector" should {
    "return one task config when one route is provided" in {
      val map = Map(
        MongoConfig.DATABASE_CONFIG -> "database1",
        MongoConfig.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      connector.taskConfigs(3).length shouldBe 1
    }
    "return one task when multiple routes are provided but maxTasks is 1" in {
      val map = Map(
        MongoConfig.DATABASE_CONFIG -> "database1",
        MongoConfig.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1; INSERT INTO coll2 SELECT * FROM topicA"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      connector.taskConfigs(1).length shouldBe 1
    }

    "return 2 configs when 3 routes are provided and maxTasks is 2" in {
      val map = Map(
        MongoConfig.DATABASE_CONFIG -> "database1",
        MongoConfig.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA;INSERT INTO coll3 SELECT * FROM topicB"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      val tasksConfigs = connector.taskConfigs(2)
      tasksConfigs.length shouldBe 2
      tasksConfigs(0).get(MongoConfig.KCQL_CONFIG) shouldBe "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA"
      tasksConfigs(1).get(MongoConfig.KCQL_CONFIG) shouldBe "INSERT INTO coll3 SELECT * FROM topicB"
    }

    "return 3 configs when 3 routes are provided and maxTasks is 3" in {
      val map = Map(
        MongoConfig.DATABASE_CONFIG -> "database1",
        MongoConfig.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA;INSERT INTO coll3 SELECT * FROM topicB"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      val tasksConfigs = connector.taskConfigs(3)
      tasksConfigs.length shouldBe 3
      tasksConfigs(0).get(MongoConfig.KCQL_CONFIG) shouldBe "INSERT INTO collection1 SELECT * FROM topic1"
      tasksConfigs(1).get(MongoConfig.KCQL_CONFIG) shouldBe "INSERT INTO coll2 SELECT * FROM topicA"
      tasksConfigs(2).get(MongoConfig.KCQL_CONFIG) shouldBe "INSERT INTO coll3 SELECT * FROM topicB"
    }

    "return 2 configs when 4 routes are provided and maxTasks is 2" in {
      val map = Map(
        MongoConfig.DATABASE_CONFIG -> "database1",
        MongoConfig.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA;INSERT INTO coll3 SELECT * FROM topicB;INSERT INTO coll4 SELECT * FROM topicC"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      val tasksConfigs = connector.taskConfigs(2)
      tasksConfigs.length shouldBe 2
      tasksConfigs(0).get(MongoConfig.KCQL_CONFIG) shouldBe "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA"
      tasksConfigs(1).get(MongoConfig.KCQL_CONFIG) shouldBe "INSERT INTO coll3 SELECT * FROM topicB;INSERT INTO coll4 SELECT * FROM topicC"
    }
  }
}
