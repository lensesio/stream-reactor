/*
 * *
 *   * Copyright 2016 Datamountaineer.
 *   *
 *   * Licensed under the Apache License, Version 2.0 (the "License");
 *   * you may not use this file except in compliance with the License.
 *   * You may obtain a copy of the License at
 *   *
 *   * http://www.apache.org/licenses/LICENSE-2.0
 *   *
 *   * Unless required by applicable law or agreed to in writing, software
 *   * distributed under the License is distributed on an "AS IS" BASIS,
 *   * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   * See the License for the specific language governing permissions and
 *   * limitations under the License.
 *   *
 */

package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.streamreactor.connect.elastic.config.{ElasticSettings, ElasticSinkConfig}
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.apache.kafka.connect.sink.SinkTaskContext
import org.elasticsearch.common.settings.Settings
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar


class TestElasticWriterSelection extends TestElasticBase with MockitoSugar {
  "A ElasticWriter should insert into Elastic Search a number of records" in {
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config  = new ElasticSinkConfig(getElasticSinkConfigPropsSelection)

    val essettings = Settings
      .settingsBuilder().put(ElasticSinkConfig.ES_CLUSTER_NAME, ElasticSinkConfig.ES_CLUSTER_NAME_DEFAULT)
      .put("path.home", TMP.toString).build()
    val client = ElasticClient.local(essettings)

    //get writer

    val settings = ElasticSettings(config)
    val writer = new ElasticJsonWriter(client = client, settings = settings)
    //write records to elastic
    writer.write(testRecords)

    Thread.sleep(2000)
    //check counts
    val res = client.execute { search in INDEX }.await
    res.totalHits shouldBe testRecords.size
    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }
}