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

import cats.implicits.catsSyntaxEitherId
import com.azure.cosmos.CosmosClient
import com.azure.cosmos.CosmosContainer
import com.azure.cosmos.CosmosDatabase
import com.azure.cosmos.models.CosmosContainerResponse
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfigConstants
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.connect.errors.ConnectException
//import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.{ eq => mockEq }
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

class CosmosDbSinkConnectorTest extends AnyWordSpec with Matchers with MockitoSugar with CosmosDbTestUtils {
  private val connection = "https://accountName.documents.azure.com:443/"

  // Test subclass to inject mock CosmosClient
  class TestableCosmosDbSinkConnector(mockClient: CosmosClient) extends CosmosDbSinkConnector {
    override protected def createCosmosClient(settings: CosmosDbSinkSettings): Either[ConnectException, CosmosClient] =
      mockClient.asRight
  }

  "CosmosDbSinkConnector" should {
    "return one task config when one route is provided" in {
      val map = Map(
        "topics"                                  -> "topic1",
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1",
      )

      val collResource = mock[CosmosContainer]

      val dbResource: CosmosDatabase = mock[CosmosDatabase]
      when(dbResource.getContainer(mockEq("collection1")))
        .thenReturn(collResource)

      val documentClient = mock[CosmosClient]
      when(documentClient.getDatabase(mockEq("database1")))
        .thenReturn(dbResource)

      val connector = new TestableCosmosDbSinkConnector(documentClient)
      connector.start(map.asJava)
      connector.taskConfigs(3).asScala.length shouldBe 1
    }

    "return one task when multiple routes are provided but maxTasks is 1" in {
      val map = Map(
        "topics"                                  -> "topic1, topicA",
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1; INSERT INTO coll2 SELECT * FROM topicA",
      )

      val cosmosContainerResponse1 = mock[CosmosContainerResponse]
      val coll1Resource            = mock[CosmosContainer]
      when(coll1Resource.read()).thenReturn(cosmosContainerResponse1)

      val cosmosContainerResponse2 = mock[CosmosContainerResponse]
      val coll2Resource            = mock[CosmosContainer]
      when(coll2Resource.read()).thenReturn(cosmosContainerResponse2)

      val dbResource: CosmosDatabase = mock[CosmosDatabase]
      when(dbResource.getContainer(mockEq("collection1")))
        .thenReturn(coll1Resource)
      when(dbResource.getContainer(mockEq("coll2")))
        .thenReturn(coll2Resource)

      val documentClient = mock[CosmosClient]
      when(documentClient.getDatabase(mockEq("database1")))
        .thenReturn(dbResource)

      val connector = new TestableCosmosDbSinkConnector(documentClient)

      connector.start(map.asJava)
      connector.taskConfigs(1).asScala.length shouldBe 1
    }

    "return 2 configs when 3 routes are provided and maxTasks is 2" in {
      val map = Map(
        "topics"                                  -> "topic1, topicA, topicB",
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA;INSERT INTO coll3 SELECT * FROM topicB",
      )

      val cosmosContainerResponse1 = mock[CosmosContainerResponse]
      val coll1Resource            = mock[CosmosContainer]
      when(coll1Resource.read()).thenReturn(cosmosContainerResponse1)

      val cosmosContainerResponse2 = mock[CosmosContainerResponse]
      val coll2Resource            = mock[CosmosContainer]
      when(coll2Resource.read()).thenReturn(cosmosContainerResponse2)

      val cosmosContainerResponse3 = mock[CosmosContainerResponse]
      val coll3Resource            = mock[CosmosContainer]
      when(coll3Resource.read()).thenReturn(cosmosContainerResponse3)

      val dbResource: CosmosDatabase = mock[CosmosDatabase]
      when(dbResource.getContainer(mockEq("collection1")))
        .thenReturn(coll1Resource)
      when(dbResource.getContainer(mockEq("coll2")))
        .thenReturn(coll2Resource)
      when(dbResource.getContainer(mockEq("coll3")))
        .thenReturn(coll3Resource)

      val documentClient = mock[CosmosClient]
      when(documentClient.getDatabase(mockEq("database1")))
        .thenReturn(dbResource)

      val connector = new TestableCosmosDbSinkConnector(documentClient)

      connector.start(map.asJava)
      val tasksConfigs = connector.taskConfigs(2).asScala
      tasksConfigs.length shouldBe 2
      tasksConfigs(0).get(
        CosmosDbConfigConstants.KCQL_CONFIG,
      ) shouldBe "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA"
      tasksConfigs(1).get(CosmosDbConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO coll3 SELECT * FROM topicB"
    }

    "return 3 configs when 3 routes are provided and maxTasks is 3" in {
      val map = Map(
        "topics"                                  -> "topic1, topicA, topicB",
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA;INSERT INTO coll3 SELECT * FROM topicB",
      )

      val cosmosContainerResponse1 = mock[CosmosContainerResponse]
      val coll1Resource            = mock[CosmosContainer]
      when(coll1Resource.read()).thenReturn(cosmosContainerResponse1)

      val cosmosContainerResponse2 = mock[CosmosContainerResponse]
      val coll2Resource            = mock[CosmosContainer]
      when(coll2Resource.read()).thenReturn(cosmosContainerResponse2)

      val cosmosContainerResponse3 = mock[CosmosContainerResponse]
      val coll3Resource            = mock[CosmosContainer]
      when(coll3Resource.read()).thenReturn(cosmosContainerResponse3)

      val dbResource: CosmosDatabase = mock[CosmosDatabase]
      when(dbResource.getContainer(mockEq("collection1")))
        .thenReturn(coll1Resource)
      when(dbResource.getContainer(mockEq("coll2")))
        .thenReturn(coll2Resource)
      when(dbResource.getContainer(mockEq("coll3")))
        .thenReturn(coll3Resource)

      val documentClient = mock[CosmosClient]
      when(documentClient.getDatabase(mockEq("database1")))
        .thenReturn(dbResource)

      val connector = new TestableCosmosDbSinkConnector(documentClient)

      connector.start(map.asJava)
      val tasksConfigs = connector.taskConfigs(3).asScala
      tasksConfigs.length shouldBe 3
      tasksConfigs(0).get(CosmosDbConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO collection1 SELECT * FROM topic1"
      tasksConfigs(1).get(CosmosDbConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO coll2 SELECT * FROM topicA"
      tasksConfigs(2).get(CosmosDbConfigConstants.KCQL_CONFIG) shouldBe "INSERT INTO coll3 SELECT * FROM topicB"
    }

    "return 2 configs when 4 routes are provided and maxTasks is 2" in {
      val map = Map(
        "topics"                                  -> "topic1, topicA, topicB, topicC",
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA;INSERT INTO coll3 SELECT * FROM topicB;INSERT INTO coll4 SELECT * FROM topicC",
      )

      val cosmosContainerResponse1 = mock[CosmosContainerResponse]
      val coll1Resource            = mock[CosmosContainer]
      when(coll1Resource.read()).thenReturn(cosmosContainerResponse1)

      val cosmosContainerResponse2 = mock[CosmosContainerResponse]
      val coll2Resource            = mock[CosmosContainer]
      when(coll2Resource.read()).thenReturn(cosmosContainerResponse2)

      val cosmosContainerResponse3 = mock[CosmosContainerResponse]
      val coll3Resource            = mock[CosmosContainer]
      when(coll3Resource.read()).thenReturn(cosmosContainerResponse3)

      val cosmosContainerResponse4 = mock[CosmosContainerResponse]
      val coll4Resource            = mock[CosmosContainer]
      when(coll3Resource.read()).thenReturn(cosmosContainerResponse4)

      val dbResource: CosmosDatabase = mock[CosmosDatabase]
      when(dbResource.getContainer(mockEq("collection1")))
        .thenReturn(coll1Resource)
      when(dbResource.getContainer(mockEq("coll2")))
        .thenReturn(coll2Resource)
      when(dbResource.getContainer(mockEq("coll3")))
        .thenReturn(coll3Resource)
      when(dbResource.getContainer(mockEq("coll4")))
        .thenReturn(coll4Resource)

      val documentClient = mock[CosmosClient]
      when(documentClient.getDatabase(mockEq("database1")))
        .thenReturn(dbResource)

      val connector = new TestableCosmosDbSinkConnector(documentClient)

      connector.start(map.asJava)
      val tasksConfigs = connector.taskConfigs(2).asScala
      tasksConfigs.length shouldBe 2
      tasksConfigs(0).get(
        CosmosDbConfigConstants.KCQL_CONFIG,
      ) shouldBe "INSERT INTO collection1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topicA"
      tasksConfigs(1).get(
        CosmosDbConfigConstants.KCQL_CONFIG,
      ) shouldBe "INSERT INTO coll3 SELECT * FROM topicB;INSERT INTO coll4 SELECT * FROM topicC"
    }

  }
}
