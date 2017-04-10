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

package com.datamountaineer.streamreactor.connect.mongodb.config


import com.datamountaineer.streamreactor.connect.errors.ThrowErrorPolicy
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class MongoSinkSettingsTest extends WordSpec with Matchers {
  "MongoSinkSettings" should {
    "default the host if the hosts settings not provided" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1"
      )

      val config = MongoConfig(map)
      val settings = MongoSinkSettings(config)
      settings.database shouldBe "database1"
      settings.connection shouldBe "mongodb://localhost:27017"
      settings.batchSize shouldBe MongoSinkConfigConstants.BATCH_SIZE_CONFIG_DEFAULT
      settings.keyBuilderMap.size shouldBe 0
      settings.kcql.size shouldBe 1
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set.empty)
    }

    "handle two topics" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT a as F1, b as F2 FROM topic2"
      )

      val config = MongoConfig(map)
      val settings = MongoSinkSettings(config)
      settings.database shouldBe "database1"
      settings.connection shouldBe "mongodb://localhost:27017"
      settings.batchSize shouldBe MongoSinkConfigConstants.BATCH_SIZE_CONFIG_DEFAULT
      settings.keyBuilderMap.size shouldBe 0
      settings.kcql.size shouldBe 2
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set.empty, "topic2" -> Set.empty)
    }

    "handle ingore fields" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1 IGNORE a,b,c"
      )

      val config = MongoConfig(map)
      val settings = MongoSinkSettings(config)
      settings.database shouldBe "database1"
      settings.connection shouldBe "mongodb://localhost:27017"
      settings.batchSize shouldBe MongoSinkConfigConstants.BATCH_SIZE_CONFIG_DEFAULT
      settings.keyBuilderMap.size shouldBe 0
      settings.kcql.size shouldBe 1
      settings.keyBuilderMap.size shouldBe 0
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set("a", "b", "c"))
    }

    "handle primary key fields" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1 PK a,b"
      )

      val config = MongoConfig(map)
      val settings = MongoSinkSettings(config)
      settings.database shouldBe "database1"
      settings.connection shouldBe "mongodb://localhost:27017"
      settings.batchSize shouldBe MongoSinkConfigConstants.BATCH_SIZE_CONFIG_DEFAULT
      settings.keyBuilderMap.size shouldBe 0
      settings.kcql.size shouldBe 1
      settings.keyBuilderMap.size shouldBe 0
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set.empty)
    }

    "throw an exception if the kcql is not valid" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO  SELECT * FROM topic1"
      )

      val config = MongoConfig(map)
      intercept[ConfigException] {
        MongoSinkSettings(config)
      }
    }

    "throw a ConfigException if the connection is missing" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "database1",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1"
      )

      intercept[ConfigException] {
        val config = MongoConfig(map)
        MongoSinkSettings(config)
      }
    }

    "throw an exception if the database is an empty string" in {
      val map = Map(
        MongoSinkConfigConstants.DATABASE_CONFIG -> "",
        MongoSinkConfigConstants.CONNECTION_CONFIG -> "mongodb://localhost:27017",
        MongoSinkConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1"
      )

      val config = MongoConfig(map)
      intercept[ConfigException] {
        MongoSinkSettings(config)
      }
    }
  }
}
