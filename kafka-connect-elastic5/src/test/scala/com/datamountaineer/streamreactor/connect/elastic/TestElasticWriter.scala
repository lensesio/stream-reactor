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

package com.datamountaineer.streamreactor.connect.elastic

import java.nio.file.Paths
import java.util.UUID

import com.datamountaineer.streamreactor.connect.elastic.config.{ElasticSettings, ElasticSinkConfig, ElasticSinkConfigConstants}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.embedded.LocalNode
import org.apache.kafka.connect.sink.SinkTaskContext
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.IndexNotFoundException
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar

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
    val config = new ElasticSinkConfig(getElasticSinkConfigProps)

    val localNode = LocalNode(ElasticSinkConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(client = client, settings = settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search in INDEX
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
    val config = new ElasticSinkConfig(getElasticSinkUpdateConfigProps)

    val localNode = LocalNode(ElasticSinkConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(client = client, settings = settings)
    //First run writes records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search in INDEX
    }.await
    res.totalHits shouldBe testRecords.size

    val testUpdateRecords = getUpdateTestRecord

    //Second run just updates
    writer.write(testUpdateRecords)

    Thread.sleep(2000)
    //check counts
    val updateRes = client.execute {
      search in INDEX
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
    val config = new ElasticSinkConfig(getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(autoCreate = true))

    val localNode = LocalNode(ElasticSinkConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(client = client, settings = settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search in INDEX_WITH_DATE
    }.await
    res.totalHits shouldBe testRecords.size
    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }

  "it should fail writing to a non-existent index when auto creation is disabled" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticSinkConfig(getElasticSinkConfigPropsWithDateSuffixAndIndexAutoCreation(autoCreate = false))

    val essettings = Settings
      .builder()
      .put("cluster.name", ElasticSinkConfigConstants.ES_CLUSTER_NAME_DEFAULT)
      .put("path.home", TMP.toString)
      .put("path.data", Paths.get(TMP.toString()).resolve("data").toString)
      .put("path.repo", Paths.get(TMP.toString()).resolve("repo").toString)
      .put("action.auto_create_index", "false")
      .build()
    val client = LocalNode(essettings).elastic4sclient(true)

    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(client = client, settings = settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    intercept[IndexNotFoundException] {
      val res = client.execute {
        search in INDEX_WITH_DATE
      }.await
    }
    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }
}
