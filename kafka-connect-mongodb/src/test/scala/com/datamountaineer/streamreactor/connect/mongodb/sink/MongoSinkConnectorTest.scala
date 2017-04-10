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

import com.datamountaineer.streamreactor.connect.mongodb.config.{MongoConfig, MongoSinkConfigConstants}
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class MongoSinkConnectorTest extends WordSpec with Matchers {
  "MongoSinkConnector" should {
    "return one task config when one route is provided" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      connector.taskConfigs(3).length shouldBe 1
    }
    "return one task when multiple routes are provided but maxTasks is 1" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1; INSERT INTO coll2 SELECT * FROM topicA"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      connector.taskConfigs(1).length shouldBe 1
    }

    "return 2 configs when 3 routes are provided and maxTasks is 2" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA;INSERT INTO coll3 SELECT * FROM topicB"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      val tasksConfigs = connector.taskConfigs(2)
      tasksConfigs.length shouldBe 2
      tasksConfigs(0).get(MongoSinkConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA"
      tasksConfigs(1).get(MongoSinkConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO coll3 SELECT * FROM topicB"
    }

    "return 3 configs when 3 routes are provided and maxTasks is 3" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA;INSERT INTO coll3 SELECT * FROM topicB"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      val tasksConfigs = connector.taskConfigs(3)
      tasksConfigs.length shouldBe 3
      tasksConfigs(0).get(MongoSinkConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO collection1 SELECT * FROM topic1"
      tasksConfigs(1).get(MongoSinkConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO coll2 SELECT * FROM topicA"
      tasksConfigs(2).get(MongoSinkConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO coll3 SELECT * FROM topicB"
    }

    "return 2 configs when 4 routes are provided and maxTasks is 2" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA;INSERT INTO coll3 SELECT * FROM topicB;INSERT INTO coll4 SELECT * FROM topicC"
      )

      val connector = new MongoSinkConnector()
      connector.start(map)
      val tasksConfigs = connector.taskConfigs(2)
      tasksConfigs.length shouldBe 2
      tasksConfigs(0).get(MongoSinkConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA"
      tasksConfigs(1).get(MongoSinkConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO coll3 SELECT * FROM topicB;INSERT INTO coll4 SELECT * FROM topicC"
    }
  }
}
