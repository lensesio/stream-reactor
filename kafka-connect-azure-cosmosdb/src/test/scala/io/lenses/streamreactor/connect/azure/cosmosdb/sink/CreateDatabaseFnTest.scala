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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink

import com.azure.cosmos.CosmosClient
import com.azure.cosmos.CosmosDatabase
import org.mockito.MockitoSugar
import org.scalatest.TryValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Success

class CreateDatabaseFnTest extends AnyFunSuite with Matchers with MockitoSugar with TryValues {

  test("returns created database when cosmosClient creates datavase") {
    implicit val mockClient: CosmosClient   = mock[CosmosClient]
    val mockDatabase:        CosmosDatabase = mock[CosmosDatabase]

    when(mockClient.createDatabase("db", null)).thenReturn(null)
    when(mockClient.getDatabase("db")).thenReturn(mockDatabase)

    val result = CreateDatabaseFn("db")
    result shouldBe Success(mockDatabase)
  }

  test("returns failure when cosmosClient.createDatabase throws") {
    implicit val mockClient: CosmosClient = mock[CosmosClient]

    when(mockClient.createDatabase("db", null)).thenThrow(new RuntimeException("creation error"))

    val result = CreateDatabaseFn("db")
    result.failure.exception.getMessage should include("creation error")
  }

  test("returns failure when cosmosClient.getDatabase throws") {
    implicit val mockClient: CosmosClient = mock[CosmosClient]

    when(mockClient.createDatabase("db", null)).thenReturn(null)
    when(mockClient.getDatabase("db")).thenThrow(new RuntimeException("get error"))

    val result = CreateDatabaseFn("db")
    result.failure.exception.getMessage should include("get error")
  }

}
