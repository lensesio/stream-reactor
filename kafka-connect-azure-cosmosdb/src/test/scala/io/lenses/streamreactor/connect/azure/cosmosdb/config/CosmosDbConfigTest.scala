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

import org.apache.kafka.common.config.ConfigException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.EitherValues

class CosmosDbConfigTest extends AnyFunSuite with Matchers with EitherValues {

  test("validate required connection configuration is present") {
    val props = Map(
      "connect.cosmosdb.endpoint"   -> "https://example.documents.azure.com:443/",
      "connect.cosmosdb.connection" -> "AccountEndpoint=https://example.documents.azure.com:443/;AccountKey=exampleKey;",
      "connect.cosmosdb.master.key" -> "exampleMasterKey",
      "connect.cosmosdb.db"         -> "exampleDatabase",
      "connect.cosmosdb.kcql"       -> "INSERT INTO target SELECT * FROM source",
    )
    val config = CosmosDbConfig(props).value
    config.getDatabase shouldEqual "exampleDatabase"
  }

  test("use default values for optional configurations when not provided") {
    val props = Map(
      "connect.cosmosdb.endpoint"   -> "https://example.documents.azure.com:443/",
      "connect.cosmosdb.connection" -> "AccountEndpoint=https://example.documents.azure.com:443/;AccountKey=exampleKey;",
      "connect.cosmosdb.master.key" -> "exampleMasterKey",
      "connect.cosmosdb.db"         -> "exampleDatabase",
      "connect.cosmosdb.kcql"       -> "INSERT INTO target SELECT * FROM source",
    )
    val config = CosmosDbConfig(props).value
    config.getBoolean("connect.cosmosdb.db.create") shouldEqual CosmosDbConfigConstants.CREATE_DATABASE_DEFAULT
    config.getString("connect.cosmosdb.consistency.level") shouldEqual CosmosDbConfigConstants.CONSISTENCY_DEFAULT
    config.getBoolean("connect.cosmosdb.bulk.enabled") shouldEqual CosmosDbConfigConstants.BULK_DEFAULT
  }

  test("throw exception when required configuration is missing") {
    // Missing 'connect.cosmosdb.endpoint'
    val props = Map(
      "connect.cosmosdb.master.key" -> "exampleMasterKey",
      "connect.cosmosdb.db"         -> "exampleDatabase",
      "connect.cosmosdb.kcql"       -> "INSERT INTO target SELECT * FROM source",
    )
    CosmosDbConfig(props).left.value shouldBe a[ConfigException]
  }

  test("validate custom consistency level configuration") {
    val props = Map(
      "connect.cosmosdb.endpoint"          -> "https://example.documents.azure.com:443/",
      "connect.cosmosdb.connection"        -> "AccountEndpoint=https://example.documents.azure.com:443/;AccountKey=exampleKey;",
      "connect.cosmosdb.master.key"        -> "exampleMasterKey",
      "connect.cosmosdb.db"                -> "exampleDatabase",
      "connect.cosmosdb.consistency.level" -> "Eventual",
      "connect.cosmosdb.kcql"              -> "INSERT INTO target SELECT * FROM source",
    )
    val config = CosmosDbConfig(props).value
    config.getString("connect.cosmosdb.consistency.level") shouldEqual "Eventual"
  }

  test("validate boolean configuration for bulk mode") {
    val props = Map(
      "connect.cosmosdb.endpoint"     -> "https://example.documents.azure.com:443/",
      "connect.cosmosdb.connection"   -> "AccountEndpoint=https://example.documents.azure.com:443/;AccountKey=exampleKey;",
      "connect.cosmosdb.master.key"   -> "exampleMasterKey",
      "connect.cosmosdb.db"           -> "exampleDatabase",
      "connect.cosmosdb.bulk.enabled" -> "true",
      "connect.cosmosdb.kcql"         -> "INSERT INTO target SELECT * FROM source",
    )
    val config = CosmosDbConfig(props).value
    config.getBoolean("connect.cosmosdb.bulk.enabled") shouldEqual true
  }

  test("validate collection throughput configuration and default") {
    val propsWithOverride = Map(
      "connect.cosmosdb.endpoint"              -> "https://example.documents.azure.com:443/",
      "connect.cosmosdb.connection"            -> "AccountEndpoint=https://example.documents.azure.com:443/;AccountKey=exampleKey;",
      "connect.cosmosdb.master.key"            -> "exampleMasterKey",
      "connect.cosmosdb.db"                    -> "exampleDatabase",
      "connect.cosmosdb.kcql"                  -> "INSERT INTO target SELECT * FROM source",
      "connect.cosmosdb.collection.throughput" -> "1234",
    )
    val configWithOverride = CosmosDbConfig(propsWithOverride).value
    configWithOverride.getInt("connect.cosmosdb.collection.throughput") shouldEqual 1234

    val propsDefault = Map(
      "connect.cosmosdb.endpoint"   -> "https://example.documents.azure.com:443/",
      "connect.cosmosdb.connection" -> "AccountEndpoint=https://example.documents.azure.com:443/;AccountKey=exampleKey;",
      "connect.cosmosdb.master.key" -> "exampleMasterKey",
      "connect.cosmosdb.db"         -> "exampleDatabase",
      "connect.cosmosdb.kcql"       -> "INSERT INTO target SELECT * FROM source",
    )
    val configDefault = CosmosDbConfig(propsDefault).value
    configDefault.getInt(
      "connect.cosmosdb.collection.throughput",
    ) shouldEqual CosmosDbConfigConstants.COLLECTION_THROUGHPUT_DEFAULT
  }
}
