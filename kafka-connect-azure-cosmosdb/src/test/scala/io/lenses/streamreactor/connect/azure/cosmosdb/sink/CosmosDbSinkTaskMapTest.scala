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

import com.azure.cosmos.ConsistencyLevel
import com.azure.cosmos.CosmosClient
import com.azure.cosmos.CosmosContainer
import com.azure.cosmos.implementation.Document
import com.azure.cosmos.models.CosmosItemResponse
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.`type`.MapType
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfig
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfigConstants
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer.CosmosDbWriterManager
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.ResourceLoader.readResource

class CosmosDbSinkTaskMapTest
    extends AnyWordSpec
    with Matchers
    with MockitoSugar
    with MatchingArgument
    with CosmosDbTestUtils {
  private val connection = "https://accountName.documents.azure.com:443/"
  private val sinkName   = "mySinkName"

  val mapper = new ObjectMapper()
  val `type`: MapType =
    mapper.getTypeFactory.constructMapType(classOf[java.util.HashMap[String, Any]], classOf[String], classOf[Any])

  "CosmosDbSinkTask" should {
    "handle util.Map INSERTS with default consistency level" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG   -> "database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG -> "secret",
        CosmosDbConfigConstants.KCQL_CONFIG       -> "INSERT INTO coll1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topic2",
      )

      val (coll1Resource: CosmosContainer, coll2Resource: CosmosContainer, documentClient: CosmosClient) =
        mockCollectionsAndDatabase

      val json1 = readResource(s"/transaction1.json")
      val map1: java.util.Map[String, Any] = mapper.readValue[java.util.Map[String, Any]](json1, `type`)

      val json2 = readResource(s"/transaction2.json")
      val map2: java.util.Map[String, Any] = mapper.readValue[java.util.Map[String, Any]](json2, `type`)

      val sinkRecord1 = new SinkRecord("topic1", 0, null, "key1", null, map1, 1000)
      val sinkRecord2 = new SinkRecord("topic2", 0, null, "key2", null, map2, 1000)

      val doc1 = new Document(json1)
      val r1   = mock[CosmosItemResponse[Document]]
      when(r1.getItem).thenReturn(doc1)

      when(
        coll1Resource
          .createItem(
            argThat(documentNoIdMatcher(doc1)),
            argThat(requestOptionsMatcher(ConsistencyLevel.SESSION)),
          ),
      )
        .thenReturn(r1)

      val doc2 = new Document(json2)
      val r2   = mock[CosmosItemResponse[Document]]
      when(r2.getItem).thenReturn(doc2)

      when(
        coll2Resource
          .createItem(
            argThat(documentNoIdMatcher(doc2)),
            argThat(requestOptionsMatcher(ConsistencyLevel.SESSION)),
          ),
      )
        .thenReturn(r2)

      val config         = CosmosDbConfig(map)
      val settings       = CosmosDbSinkSettings(config)
      val kcqlMap        = settings.kcql.map(c => c.getSource -> c).toMap
      val batchPolicyMap = settings.kcql.map(c => c.getSource -> config.commitPolicy(c)).toMap

      val writer = new CosmosDbWriterManager(sinkName, kcqlMap, batchPolicyMap, settings, documentClient)
      writer.write(Seq(sinkRecord1, sinkRecord2))

      verify(coll1Resource)
        .createItem(
          argThat(documentNoIdMatcher(doc1)),
          argThat(requestOptionsMatcher(ConsistencyLevel.SESSION)),
        )

      verify(coll2Resource)
        .createItem(
          argThat(documentNoIdMatcher(doc2)),
          argThat(requestOptionsMatcher(ConsistencyLevel.SESSION)),
        )
    }

    "handle util.Map INSERTS with Eventual consistency level" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG    -> "database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG  -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG  -> "secret",
        CosmosDbConfigConstants.CONSISTENCY_CONFIG -> ConsistencyLevel.EVENTUAL.toString,
        CosmosDbConfigConstants.KCQL_CONFIG        -> "INSERT INTO coll1 SELECT * FROM topic1;INSERT INTO coll2 SELECT * FROM topic2",
      )

      val (coll1Resource: CosmosContainer, coll2Resource: CosmosContainer, documentClient: CosmosClient) =
        mockCollectionsAndDatabase

      val json1 = readResource(s"/transaction1.json")
      val map1: java.util.Map[String, Any] = mapper.readValue[java.util.Map[String, Any]](json1, `type`)

      val json2 = readResource(s"/transaction2.json")
      val map2: java.util.Map[String, Any] = mapper.readValue[java.util.Map[String, Any]](json2, `type`)

      val sinkRecord1 = new SinkRecord("topic1", 0, null, "key1", null, map1, 1000)
      val sinkRecord2 = new SinkRecord("topic2", 0, null, "key2", null, map2, 1000)

      val doc1 = new Document(json1)
      val r1   = mock[CosmosItemResponse[Document]]
      when(r1.getItem).thenReturn(doc1)

      when(
        coll1Resource
          .createItem(
            argThat(documentNoIdMatcher(doc1)),
            argThat(requestOptionsMatcher(ConsistencyLevel.EVENTUAL)),
          ),
      )
        .thenReturn(r1)

      val doc2 = new Document(json2)
      val r2   = mock[CosmosItemResponse[Document]]
      when(r2.getItem).thenReturn(doc2)

      when(
        coll2Resource
          .createItem(
            argThat(documentNoIdMatcher(doc2)),
            argThat(requestOptionsMatcher(ConsistencyLevel.EVENTUAL)),
          ),
      )
        .thenReturn(r2)

      val config         = CosmosDbConfig(map)
      val settings       = CosmosDbSinkSettings(config)
      val kcqlMap        = settings.kcql.map(c => c.getSource -> c).toMap
      val batchPolicyMap = settings.kcql.map(c => c.getSource -> config.commitPolicy(c)).toMap

      val writer = new CosmosDbWriterManager(sinkName, kcqlMap, batchPolicyMap, settings, documentClient)
      writer.write(Seq(sinkRecord1, sinkRecord2))

      verify(coll1Resource)
        .createItem(
          argThat(documentNoIdMatcher(doc1)),
          argThat(requestOptionsMatcher(ConsistencyLevel.EVENTUAL)),
        )

      verify(coll2Resource)
        .createItem(
          argThat(documentNoIdMatcher(doc2)),
          argThat(requestOptionsMatcher(ConsistencyLevel.EVENTUAL)),
        )
    }

    "handle util.Map UPSERT with Eventual consistency level" in {
      val map = Map(
        CosmosDbConfigConstants.DATABASE_CONFIG    -> "database1",
        CosmosDbConfigConstants.CONNECTION_CONFIG  -> connection,
        CosmosDbConfigConstants.MASTER_KEY_CONFIG  -> "secret",
        CosmosDbConfigConstants.CONSISTENCY_CONFIG -> ConsistencyLevel.EVENTUAL.toString,
        CosmosDbConfigConstants.KCQL_CONFIG        -> "UPSERT INTO coll1 SELECT * FROM topic1 PK time",
      )

      val (coll1Resource: CosmosContainer, _: CosmosContainer, documentClient: CosmosClient) =
        mockCollectionsAndDatabase

      val json1 = readResource("/transaction1.json")
      val map1: java.util.Map[String, Any] = mapper.readValue[java.util.Map[String, Any]](json1, `type`)

      val json2 = ResourceLoader.readResource("/transaction2.json")
      val map2: java.util.Map[String, Any] = mapper.readValue[java.util.Map[String, Any]](json2, `type`)

      val sinkRecord1 = new SinkRecord("topic1", 0, null, "key1", null, map1, 1000)
      val sinkRecord2 = new SinkRecord("topic1", 0, null, "key2", null, map2, 1000)

      val doc1 = new Document(json1)
      doc1.setId("key1")

      val r1 = mock[CosmosItemResponse[Document]]
      when(r1.getItem).thenReturn(doc1)

      when(
        coll1Resource
          .upsertItem(
            argThat(documentMatcher(doc1)),
            argThat(requestOptionsMatcher(ConsistencyLevel.EVENTUAL)),
          ),
      )
        .thenReturn(r1)

      val doc2 = new Document(json2)
      doc2.setId("key2")

      val r2 = mock[CosmosItemResponse[Document]]
      when(r2.getItem).thenReturn(doc2)

      when(
        coll1Resource
          .upsertItem(
            argThat(documentMatcher(doc2)),
            argThat(requestOptionsMatcher(ConsistencyLevel.EVENTUAL)),
          ),
      )
        .thenReturn(r2)

      val config         = CosmosDbConfig(map)
      val settings       = CosmosDbSinkSettings(config)
      val kcqlMap        = settings.kcql.map(c => c.getSource -> c).toMap
      val batchPolicyMap = settings.kcql.map(c => c.getSource -> config.commitPolicy(c)).toMap

      val writer = new CosmosDbWriterManager(sinkName, kcqlMap, batchPolicyMap, settings, documentClient)
      writer.write(Seq(sinkRecord1, sinkRecord2))

      verify(coll1Resource)
        .upsertItem(
          argThat(documentMatcher(doc1)),
          argThat(requestOptionsMatcher(ConsistencyLevel.EVENTUAL)),
        )

      verify(coll1Resource)
        .upsertItem(
          argThat(documentMatcher(doc2)),
          argThat(requestOptionsMatcher(ConsistencyLevel.EVENTUAL)),
        )
    }

  }
}
