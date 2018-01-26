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

package com.datamountaineer.streamreactor.connect.elastic5

import java.nio.file.Paths
import java.util.UUID

import com.datamountaineer.streamreactor.connect.elastic5.config.ElasticSettings
import com.datamountaineer.streamreactor.connect.elastic5.config.{ClientType, ElasticConfig, ElasticConfigConstants}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.ElasticsearchClientUri
import com.sksamuel.elastic4s.embedded.LocalNode
import com.sksamuel.elastic4s.http.HttpClient
import org.apache.kafka.connect.sink.SinkTaskContext
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.IndexNotFoundException
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import scala.reflect.io.File


class TestElasticWriter extends TestElasticBase with MockitoSugar {
  "A ElasticWriter should insert into Elastic Search a number of records" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticConfig(getElasticSinkConfigProps)

    val localNode = LocalNode(ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(new TcpKElasticClient(client), settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search(INDEX)
    }.await
    res.totalHits shouldBe testRecords.size
    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }

  "A ElasticWriter should update a number of records in Elastic Search" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticConfig(getElasticSinkUpdateConfigProps)

    val localNode = LocalNode(ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(new TcpKElasticClient(client), settings = settings)
    //First run writes records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search(INDEX)
    }.await
    res.totalHits shouldBe testRecords.size

    val testUpdateRecords = getUpdateTestRecord

    //Second run just updates
    writer.write(testUpdateRecords)

    Thread.sleep(2000)
    //check counts
    val updateRes = client.execute {
      search(INDEX)
    }.await
    updateRes.totalHits shouldBe testRecords.size

    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }

  "A ElasticWriter should update a number of records in Elastic Search with index suffix defined" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticConfig(getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(autoCreate = true))

    val localNode = LocalNode(ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(new TcpKElasticClient(client), settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search(INDEX_WITH_DATE)
    }.await
    res.totalHits shouldBe testRecords.size
    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }

  "It should fail writing to a non-existent index when auto creation is disabled" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticConfig(getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(autoCreate = false))

    val essettings = Settings
      .builder()
      .put("cluster.name", ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT)
      .put("path.home", TMP.toString)
      .put("path.data", Paths.get(TMP.toString()).resolve("data").toString)
      .put("path.repo", Paths.get(TMP.toString()).resolve("repo").toString)
      .put("action.auto_create_index", "false")
      .build()
    val client = LocalNode(essettings).elastic4sclient(true)

    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(new TcpKElasticClient(client), settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    intercept[IndexNotFoundException] {
      client.execute {
        search(INDEX_WITH_DATE)
      }.await
    }
    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }

  "A writer should be using TCP by default" in {
    //get config
    val config = new ElasticConfig(getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(autoCreate = false))
    val settings = ElasticSettings(config)
    settings.clientType.toString shouldBe ClientType.TCP.toString
  }

  "A writer should be using HTTP is set" in {
    //get config
    val config = new ElasticConfig(getElasticSinkConfigPropsHTTPClient(autoCreate = false))
    val settings = ElasticSettings(config)
    settings.clientType.toString shouldBe ClientType.HTTP.toString
  }

  "A ElasticWriter should insert into Elastic Search a number of records with the HTTP Client" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticConfig(getElasticSinkConfigPropsHTTPClient(autoCreate = true))

    val localNode = LocalNode(ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    val httpClient = HttpClient(ElasticsearchClientUri(s"elasticsearch://${localNode.ipAndPort}"))
    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(new HttpKElasticClient(httpClient), settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search(INDEX)
    }.await
    res.totalHits shouldBe testRecords.size
    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }

  "A ElasticWriter should insert into with PK Elastic Search a number of records" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticConfig(getElasticSinkConfigPropsPk)

    val localNode = LocalNode(ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(new TcpKElasticClient(client), settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search(INDEX)
    }.await
    res.totalHits shouldBe testRecords.size

    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val resUpdate = client.execute {
      search(INDEX)
    }.await
    resUpdate.totalHits shouldBe testRecords.size

    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }

  "A ElasticWriter should insert into without PK Elastic Search a number of records" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticConfig(getElasticSinkConfigProps)

    val localNode = LocalNode(ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(new TcpKElasticClient(client), settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search(INDEX)
    }.await
    res.totalHits shouldBe testRecords.size

    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val resUpdate = client.execute {
      search(INDEX)
    }.await
    resUpdate.totalHits shouldBe testRecords.size

    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }
}
