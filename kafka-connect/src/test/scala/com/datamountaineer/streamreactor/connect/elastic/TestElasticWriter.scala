package com.datamountaineer.streamreactor.connect.elastic

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.apache.kafka.connect.sink.SinkTaskContext
import org.elasticsearch.common.settings.Settings
import org.junit.rules.TemporaryFolder
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
    //get writer
    val temp = new TemporaryFolder()
    val essettings = Settings
      .settingsBuilder().put("cluster.name", "elasticsearch")
      .put("path.home", temp).build()
    val client = ElasticClient.local(essettings)
    val writer = new ElasticJsonWriter(client = client, context = context)
    //write records to elastic
    writer.insert(testRecords).await

//    val res = client.execute { count from TOPIC }.await
//    res.getCount shouldBe(testRecords.size)
//    //close writer
    writer.close()
  }

}