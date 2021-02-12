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

package com.datamountaineer.streamreactor.common.mongodb.sink

import com.datamountaineer.streamreactor.common.mongodb.config.MongoConfigConstants
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.JavaConverters._

class MongoSinkConnectorTest extends AnyWordSpec with Matchers with MockitoSugar {
  "MongoSinkConnector" should {


    "return one task when multiple routes are provided but maxTasks is 1" in {
      val map = Map(
        "topics" -> "topic1, topicA",
        MongoConfigConstants.DATABASE_CONFIG -> "database1",
        MongoConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1; INSERT INTO coll2 SELECT * FROM topicA"
      ).asJava

      val connector = new MongoSinkConnector()
      connector.start(map)
      connector.taskConfigs(1).size() shouldBe 1
    }
  }
}
