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

import com.azure.cosmos.ConsistencyLevel
import com.azure.cosmos.CosmosClient
import com.azure.cosmos.CosmosContainer
import com.azure.cosmos.CosmosDatabase
import com.azure.cosmos.implementation.Document
import com.azure.cosmos.models.CosmosItemRequestOptions
import org.mockito.ArgumentMatchers.{ eq => mockEq }
import org.mockito.MockitoSugar

trait CosmosDbTestUtils extends MockitoSugar {

  def documentNoIdMatcher(expectedDoc: Document): Document => Boolean = { argument: Document =>
    argument.getPropertyBag.remove("id")
    argument != null && argument.toString == expectedDoc.toString
  }
  def documentMatcher(expectedDoc: Document): Document => Boolean = { argument: Document =>
    argument != null && argument.toString == expectedDoc.toString
  }
  def requestOptionsMatcher(expectedConsistencyLevel: ConsistencyLevel): CosmosItemRequestOptions => Boolean = {
    argument: CosmosItemRequestOptions =>
      argument.getConsistencyLevel == expectedConsistencyLevel
  }
  def mockCollectionsAndDatabase: (CosmosContainer, CosmosContainer, CosmosClient) = {
    val coll1Resource = mock[CosmosContainer]
    val coll2Resource = mock[CosmosContainer]

    val dbResource: CosmosDatabase = mock[CosmosDatabase]
    when(dbResource.getContainer(mockEq("coll1")))
      .thenReturn(coll1Resource)
    when(dbResource.getContainer(mockEq("coll2")))
      .thenReturn(coll2Resource)

    val documentClient = mock[CosmosClient]
    when(documentClient.getDatabase(mockEq("database1")))
      .thenReturn(dbResource)
    (coll1Resource, coll2Resource, documentClient)
  }
}
