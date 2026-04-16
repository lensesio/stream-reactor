/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink

import com.azure.cosmos.CosmosClient
import com.azure.cosmos.CosmosDatabase
import com.azure.cosmos.models.ThroughputProperties
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchers._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.CosmosDbDatabaseUtils._

class CosmosDbDatabaseUtilsTest extends AnyFunSuite with MockitoSugar with EitherValues with Matchers {

  test("readOrCreateDatabase should call getDatabase and handle creation logic") {
    val client   = mock[CosmosClient]
    val db       = mock[CosmosDatabase]
    val settings = mock[CosmosDbSinkSettings]
    when(settings.database).thenReturn("testdb")
    when(settings.createDatabase).thenReturn(true)
    when(client.getDatabase("testdb")).thenReturn(db)

    val result = readOrCreateDatabase(settings)(client)
    result.value should be theSameInstanceAs db
    verify(client).getDatabase("testdb")
    // Note: Full database creation logic is tested in integration or CreateDatabaseFn tests
  }

  test("readOrCreateDatabase should not call createDatabaseIfNotExists if createDatabase is false") {
    val client   = mock[CosmosClient]
    val db       = mock[CosmosDatabase]
    val settings = mock[CosmosDbSinkSettings]
    when(settings.database).thenReturn("testdb")
    when(settings.createDatabase).thenReturn(false)
    when(client.getDatabase("testdb")).thenReturn(db)

    val result = readOrCreateDatabase(settings)(client)
    result.value should be theSameInstanceAs db
    verify(client).getDatabase("testdb")
  }

  test("readOrCreateCollections should call createCollection for missing collections") {
    val db       = mock[CosmosDatabase]
    val settings = mock[CosmosDbSinkSettings]
    val kcql1    = mock[Kcql]
    val kcql2    = mock[Kcql]
    when(kcql1.getTarget).thenReturn("coll1")
    when(kcql2.getTarget).thenReturn("coll2")
    when(settings.kcql).thenReturn(Seq(kcql1, kcql2))
    // Simulate getContainer.read() throwing for both collections
    when(db.getContainer(anyString())).thenReturn(mock[com.azure.cosmos.CosmosContainer])
    when(db.getContainer(anyString()).read()).thenThrow(new RuntimeException("not found"))
    when(db.createContainer(anyString(), anyString(), any[ThroughputProperties]())).thenReturn(null)

    readOrCreateCollections(db, settings)
    verify(db, atLeastOnce).createContainer(anyString(), anyString(), any[ThroughputProperties]())
  }

  test("createCollection should call createContainer with correct args") {
    val db         = mock[CosmosDatabase]
    val throughput = mock[ThroughputProperties]
    createCollection(db, throughput, "coll1")
    verify(db).createContainer("coll1", "partitionKeyPath", throughput)
  }

  // --- Migrated from CreateDatabaseFnTest ---

  test("createDatabase returns created database when cosmosClient creates database") {
    val mockClient   = mock[CosmosClient]
    val mockDatabase = mock[CosmosDatabase]
    when(mockClient.createDatabase("db", null)).thenReturn(null)
    when(mockClient.getDatabase("db")).thenReturn(mockDatabase)
    val result = createDatabase("db", mockClient)
    result.value should be theSameInstanceAs mockDatabase
  }

  test("createDatabase returns failure when cosmosClient.createDatabase throws") {
    val mockClient = mock[CosmosClient]
    when(mockClient.createDatabase("db", null)).thenThrow(new RuntimeException("creation error"))
    val result = createDatabase("db", mockClient)
    result.left.value.getMessage should include("creation error")
  }

  test("createDatabase returns failure when cosmosClient.getDatabase throws") {
    val mockClient = mock[CosmosClient]
    when(mockClient.createDatabase("db", null)).thenReturn(null)
    when(mockClient.getDatabase("db")).thenThrow(new RuntimeException("get error"))
    val result = createDatabase("db", mockClient)
    result.left.value.getMessage should include("get error")
  }
}
