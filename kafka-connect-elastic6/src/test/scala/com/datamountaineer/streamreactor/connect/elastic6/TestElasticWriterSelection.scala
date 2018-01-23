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

import java.util.UUID

import com.datamountaineer.streamreactor.connect.elastic6.config.ElasticSettings
import com.datamountaineer.streamreactor.connect.elastic6.config.{ElasticConfig, ElasticConfigConstants}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.embedded.LocalNode
import org.apache.kafka.connect.sink.SinkTaskContext
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar

import scala.reflect.io.File


class TestElasticWriterSelection extends TestElasticBase with MockitoSugar {
  "A ElasticWriter should insert into Elastic Search a number of records" in {

    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticConfig(getElasticSinkConfigPropsSelection)

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

  "A ElasticWriter should insert into Elastic Search a number of records when nested fields are selected" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecordsNested
    //get config
    val config = new ElasticConfig( getBaseElasticSinkConfigProps(s"INSERT INTO $INDEX SELECT id, nested.string_field FROM $TOPIC"))

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

  "A ElasticWriter should update records in Elastic Search" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config = new ElasticConfig(getElasticSinkUpdateConfigPropsSelection)

    val localNode = LocalNode(ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(new TcpKElasticClient(client), settings)
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

  "A ElasticWriter should update records in Elastic Search with PK nested field" in {
    val TMP = File(System.getProperty("java.io.tmpdir") + "/elastic-" + UUID.randomUUID())
    TMP.createDirectory()
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecordsNested
    //get config
    val config = new ElasticConfig(getBaseElasticSinkConfigProps(s"UPSERT INTO $INDEX SELECT nested.id, string_field FROM $TOPIC PK nested.id"))

    val localNode = LocalNode(ElasticConfigConstants.ES_CLUSTER_NAME_DEFAULT, TMP.toString)
    val client = localNode.elastic4sclient(true)
    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(new TcpKElasticClient(client), settings)
    //First run writes records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute {
      search(INDEX)
    }.await
    res.totalHits shouldBe testRecords.size

    val testUpdateRecords = getUpdateTestRecordNested

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
}