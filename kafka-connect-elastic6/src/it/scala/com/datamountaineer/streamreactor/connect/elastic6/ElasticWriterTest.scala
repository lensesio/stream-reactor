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

package com.datamountaineer.streamreactor.connect.elastic6

import com.datamountaineer.streamreactor.connect.elastic6.CreateLocalNodeClientUtil.{createLocalNode, createLocalNodeClient}
import com.datamountaineer.streamreactor.connect.elastic6.config.{ElasticConfig, ElasticSettings}
import com.sksamuel.elastic4s.http.ElasticClient
import com.sksamuel.elastic4s.http.ElasticDsl._
import org.elasticsearch.common.settings.Settings
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.testcontainers.elasticsearch.ElasticsearchContainer

import java.nio.file.Paths
import java.util.UUID
import scala.reflect.io.File

class ElasticWriterTest extends ITBase with MockitoSugar with BeforeAndAfterEach {

  class TestContext {

    val TemporaryLocalNodeDir = createTmpDir()
    val RandomClusterName     = UUID.randomUUID().toString()
    val TestRecords           = getTestRecords()

    val DefaultSettings = Settings
      .builder()
      .put("cluster.name", RandomClusterName)
      .put("path.home", TemporaryLocalNodeDir.toString)
      .put("path.data", Paths.get(TemporaryLocalNodeDir.toString()).resolve("data").toString)
      .put("path.repo", Paths.get(TemporaryLocalNodeDir.toString()).resolve("repo").toString)
      .build()

    private def createTmpDir(): File = {
      val dirFile = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
      dirFile.createDirectory()
      dirFile
    }

    def writeTestRecords(props: java.util.Map[String, String]) = {

      val localNode = createLocalNode()

      val client: ElasticClient = createLocalNodeClient(localNode)

      val writer = new ElasticJsonWriter(new HttpKElasticClient(client), ElasticSettings(ElasticConfig(props)))

      writer.write(TestRecords)
      (localNode, client, writer)
    }
  }

  "A ElasticWriter should insert into Elastic Search a number of records" in new TestContext {

    val (node: ElasticsearchContainer, client: ElasticClient, writer: ElasticJsonWriter) = writeTestRecords(
      getElasticSinkConfigProps(RandomClusterName),
    )

    Thread.sleep(2000)

    val res = client.execute {
      search(INDEX)
    }.await
    res.result.totalHits shouldBe TestRecords.size

    writer.close()
    client.close()
    node.stop()
    TemporaryLocalNodeDir.deleteRecursively()

  }

  "A ElasticWriter should update a number of records in Elastic Search" in new TestContext {
    val (node: ElasticsearchContainer, client: ElasticClient, writer: ElasticJsonWriter) = writeTestRecords(
      getElasticSinkUpdateConfigProps(RandomClusterName),
    )

    Thread.sleep(2000)

    val res = client.execute {
      search(INDEX)
    }.await
    res.result.totalHits shouldBe TestRecords.size

    val testUpdateRecords = getUpdateTestRecord

    //Second run just updates
    writer.write(testUpdateRecords)

    Thread.sleep(2000)

    val updateRes = client.execute {
      search(INDEX)
    }.await
    updateRes.result.totalHits shouldBe TestRecords.size

    writer.close()
    client.close()
    node.stop()
    TemporaryLocalNodeDir.deleteRecursively()
  }

  "A ElasticWriter should update a number of records in Elastic Search with index suffix defined" in new TestContext {

    val (node: ElasticsearchContainer, client: ElasticClient, writer: ElasticJsonWriter) = writeTestRecords(
      getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(autoCreate = true),
    )

    Thread.sleep(2000)

    val res = client.execute {
      search(INDEX_WITH_DATE)
    }.await
    res.result.totalHits shouldBe TestRecords.size

    writer.close()
    client.close()
    node.stop()
    TemporaryLocalNodeDir.deleteRecursively()

  }

  "It should fail writing to a non-existent index when auto creation is disabled" ignore new TestContext {

    val (node: ElasticsearchContainer, client: ElasticClient, writer: ElasticJsonWriter) = writeTestRecords(
      getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(autoCreate = false, RandomClusterName),
    )

    Thread.sleep(2000)

    val searchResponse = client.execute {
      search(INDEX_WITH_DATE)
    }.await
    searchResponse.isError should be(true)
    searchResponse.error.`type` should be("index_not_found_exception")

    writer.close()
    client.close()
    node.close()
    TemporaryLocalNodeDir.deleteRecursively()

  }

  "A ElasticWriter should insert into Elastic Search a number of records with the HTTP Client" in new TestContext {

    val (node: ElasticsearchContainer, client: ElasticClient, writer: ElasticJsonWriter) = writeTestRecords(
      getElasticSinkConfigPropsHTTPClient(autoCreate = true),
    )

    Thread.sleep(2000)

    val res = client.execute {
      search(INDEX)
    }.await
    res.result.totalHits shouldBe TestRecords.size

    writer.close()
    client.close()
    node.close()
    TemporaryLocalNodeDir.deleteRecursively()
  }

  "A ElasticWriter should insert into with PK Elastic Search a number of records" in new TestContext {

    val (node: ElasticsearchContainer, client: ElasticClient, writer: ElasticJsonWriter) = writeTestRecords(
      getElasticSinkConfigPropsPk(RandomClusterName),
    )

    Thread.sleep(2000)

    val res = client.execute {
      search(INDEX)
    }.await
    res.result.totalHits shouldBe TestRecords.size

    writer.write(TestRecords)

    Thread.sleep(2000)

    val resUpdate = client.execute {
      search(INDEX)
    }.await
    resUpdate.result.totalHits shouldBe TestRecords.size

    writer.close()
    client.close()
    node.close()
    TemporaryLocalNodeDir.deleteRecursively()
  }

  "A ElasticWriter should insert into without PK Elastic Search a number of records" in new TestContext {

    val (node: ElasticsearchContainer, client: ElasticClient, writer: ElasticJsonWriter) = writeTestRecords(
      getElasticSinkConfigProps(RandomClusterName),
    )

    Thread.sleep(2000)

    val res = client.execute {
      search(INDEX)
    }.await
    res.result.totalHits shouldBe TestRecords.size

    writer.write(TestRecords)

    Thread.sleep(2000)

    val resUpdate = client.execute {
      search(INDEX)
    }.await
    resUpdate.result.totalHits shouldBe TestRecords.size

    writer.close()
    client.close()
    node.close()
    TemporaryLocalNodeDir.deleteRecursively()
  }
}
