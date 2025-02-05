/*
 * Copyright 2017-2025 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.azure.cosmosdb.config

import io.lenses.streamreactor.common.errors.ThrowErrorPolicy
import org.apache.kafka.common.config.ConfigException
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CosmosDbSinkSettingsTest extends AnyWordSpec with Matchers {
  private val connection = "https://accountName.documents.azure.com:443/"

  "CosmosDbSinkSettings" should {
    "throw an exception if master key is missing" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "dbs/database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1",
      )

      intercept[ConfigException] {
        CosmosDbConfig(map)
      }
    }

    "throw an exception if connection is missing" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "dbs/database1",
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1",
      )

      intercept[ConfigException] {
        CosmosDbConfig(map)
      }
    }

    "handle two topics" in {

      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "dbs/database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT a as F1, b as F2 FROM topic2",
      )

      val config   = CosmosDbConfig(map)
      val settings = CosmosDbSinkSettings(config)
      settings.database shouldBe "dbs/database1"
      settings.endpoint shouldBe connection
      settings.kcql.size shouldBe 2
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set.empty, "topic2" -> Set.empty)
    }

    "handle ingore fields" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "db/database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1 IGNORE a,b,c",
      )

      val config   = CosmosDbConfig(map)
      val settings = CosmosDbSinkSettings(config)
      settings.database shouldBe "db/database1"
      settings.endpoint shouldBe connection
      settings.kcql.size shouldBe 1
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set("a", "b", "c"))
    }

    "handle primary key fields" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "dbs/database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1 PK a,b",
      )

      val config   = CosmosDbConfig(map)
      val settings = CosmosDbSinkSettings(config)
      settings.kcql.size shouldBe 1
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set.empty)
    }

    "throw an exception if the consistency level is invalid" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG    -> "dbs/database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG  -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG  -> "secret",
        CosmosDbConfigConstants.CONSISTENCY_CONFIG -> "invalid",
        CosmosDbConfigConstants.KCQL_CONFIG        -> "INSERT INTO collection1 SELECT * FROM topic1 PK a,b",
      )

      val config = CosmosDbConfig(map)
      intercept[ConfigException] {
        CosmosDbSinkSettings(config)
      }
    }

    "throw an exception if the kcql is not valid" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO  SELECT * FROM topic1",
      )

      val config = CosmosDbConfig(map)
      intercept[IllegalArgumentException] {
        CosmosDbSinkSettings(config)
      }
    }

    "throw an exception if the database is an empty string" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1",
      )

      val config = CosmosDbConfig(map)
      intercept[ConfigException] {
        CosmosDbSinkSettings(config)
      }
    }
  }
}
