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