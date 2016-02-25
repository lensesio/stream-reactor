package com.datamountaineer.streamreactor.connect.elastic

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.apache.kafka.connect.sink.SinkTaskContext
import org.elasticsearch.common.settings.Settings
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar


class TestElasticWriter extends TestElasticBase with MockitoSugar {
  test("A ElasticWriter should insert into Elastic Search a number of records") {
    //mock the context to return our assignment when called
    val context = mock[SinkTaskContext]
    when(context.assignment()).thenReturn(getAssignment)
    //get test records
    val testRecords = getTestRecords
    //get config
    val config  = new ElasticSinkConfig(getElasticSinkConfigProps)

    val essettings = Settings
      .settingsBuilder().put(ElasticSinkConfig.ES_CLUSTER_NAME, ElasticSinkConfig.ES_CLUSTER_NAME_DEFAULT)
      .put("path.home", TMP.toString).build()
    val client = ElasticClient.local(essettings)

    //get writer
    val writer = new ElasticJsonWriter(client = client, context = context)
    //write records to elastic
    writer.insert(testRecords).await

    Thread.sleep(2000)
    //check counts
    val res = client.execute { search in TOPIC }.await
    res.totalHits shouldBe testRecords.size
    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }
}