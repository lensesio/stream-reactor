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

package com.datamountaineer.streamreactor.connect.azure.documentdb.config

import com.datamountaineer.streamreactor.connect.errors.ThrowErrorPolicy
import org.apache.kafka.common.config.ConfigException
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class DocumentDbSinkSettingsTest extends WordSpec with Matchers {
  private val connection = "https://accountName.documents.azure.com:443/"

  "DocumentDbSinkSettings" should {
    "throw an exception if master key is missing" in {
      val map = Map(
        DocumentDbConfigConstants.DATABASE_CONFIG -> "dbs/database1",
        DocumentDbConfigConstants.CONNECTION_CONFIG -> connection,
        DocumentDbConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1"
      )

      intercept[ConfigException] {
        DocumentDbConfig(map)
      }
    }

    "throw an exception if connection is missing" in {
      val map = Map(
        DocumentDbConfigConstants.DATABASE_CONFIG -> "dbs/database1",
        DocumentDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1"
      )

      intercept[ConfigException] {
        DocumentDbConfig(map)
      }
    }

    "handle two topics" in {

      val map = Map(
        DocumentDbConfigConstants.DATABASE_CONFIG -> "dbs/database1",
        DocumentDbConfigConstants.CONNECTION_CONFIG -> connection,
        DocumentDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT a as F1, b as F2 FROM topic2"
      )

      val config = DocumentDbConfig(map)
      val settings = DocumentDbSinkSettings(config)
      settings.database shouldBe "dbs/database1"
      settings.endpoint shouldBe connection
      settings.keyBuilderMap.size shouldBe 0
      settings.kcql.size shouldBe 2
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set.empty, "topic2" -> Set.empty)
    }

    "handle ingore fields" in {
      val map = Map(
        DocumentDbConfigConstants.DATABASE_CONFIG -> "db/database1",
        DocumentDbConfigConstants.CONNECTION_CONFIG -> connection,
        DocumentDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1 IGNORE a,b,c"
      )

      val config = DocumentDbConfig(map)
      val settings = DocumentDbSinkSettings(config)
      settings.database shouldBe "db/database1"
      settings.endpoint shouldBe connection
      settings.keyBuilderMap.size shouldBe 0
      settings.kcql.size shouldBe 1
      settings.keyBuilderMap.size shouldBe 0
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set("a", "b", "c"))
    }

    "handle primary key fields" in {
      val map = Map(
        DocumentDbConfigConstants.DATABASE_CONFIG -> "dbs/database1",
        DocumentDbConfigConstants.CONNECTION_CONFIG -> connection,
        DocumentDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1 PK a,b"
      )

      val config = DocumentDbConfig(map)
      val settings = DocumentDbSinkSettings(config)
      settings.keyBuilderMap.size shouldBe 0
      settings.kcql.size shouldBe 1
      settings.keyBuilderMap.size shouldBe 0
      settings.errorPolicy shouldBe ThrowErrorPolicy()
      settings.ignoredField shouldBe Map("topic1" -> Set.empty)
    }

    "throw an exception if the consistency level is invalid" in {
      val map = Map(
        DocumentDbConfigConstants.DATABASE_CONFIG -> "dbs/database1",
        DocumentDbConfigConstants.CONNECTION_CONFIG -> connection,
        DocumentDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfigConstants.CONSISTENCY_CONFIG -> "invalid",
        DocumentDbConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1 PK a,b"
      )

      val config = DocumentDbConfig(map)
      intercept[ConfigException] {
        DocumentDbSinkSettings(config)
      }
    }

    "throw an exception if the kcql is not valid" in {
      val map = Map(
        DocumentDbConfigConstants.DATABASE_CONFIG -> "database1",
        DocumentDbConfigConstants.CONNECTION_CONFIG -> connection,
        DocumentDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfigConstants.KCQL_CONFIG -> "INSERT INTO  SELECT * FROM topic1"
      )

      val config = DocumentDbConfig(map)
      intercept[ConfigException] {
        DocumentDbSinkSettings(config)
      }
    }


    "throw an exception if the database is an empty string" in {
      val map = Map(
        DocumentDbConfigConstants.DATABASE_CONFIG -> "",
        DocumentDbConfigConstants.CONNECTION_CONFIG -> connection,
        DocumentDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        DocumentDbConfigConstants.KCQL_CONFIG -> "INSERT INTO collection1 SELECT * FROM topic1"
      )

      val config = DocumentDbConfig(map)
      intercept[ConfigException] {
        DocumentDbSinkSettings(config)
      }
    }
  }
}
