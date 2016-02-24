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
    val config  = new ElasticSinkConfig(getElasticSinkConfigProps(TMP.toString))
    //get writer
    val writer = ElasticWriter(config = config, context = context)
    //write records to elastic
    writer.insert(testRecords).await

    //allow Elastic search to write behind the scenes, waiting to the future on the write is not enough,
    //this just says elastics has it.
    Thread.sleep(2000)
    val essettings = Settings
      .settingsBuilder().put(ElasticConstants.CLUSTER_NAME, ElasticConstants.DEFAULT_CLUSTER_NAME)
      .put(ElasticConstants.PATH_HOME, TMP.toString).build()
    val client = ElasticClient.local(essettings)

    //check counts
    val res = client.execute { search in TOPIC }.await
    res.totalHits shouldBe testRecords.size
    //close writer
    writer.close()
    client.close()
    TMP.deleteRecursively()
  }
}