package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.streamreactor.connect.elastic.config.ElasticSinkConfig

import scala.collection.JavaConverters._

class TestElasticsSinkConnector extends TestElasticBase {
  test("Should start a Elastic Search Connector") {
    //get config
    val config = getElasticSinkConfigProps
    //get connector
    val connector = new ElasticSinkConnector()
    //start with config
    connector.start(config)
    //check config
    val taskConfigs = connector.taskConfigs(10)
    taskConfigs.asScala.head.get(ElasticSinkConfig.URL) shouldBe ELASTIC_SEARCH_HOSTNAMES
    taskConfigs.size() shouldBe 10
    //check connector
    connector.taskClass() shouldBe classOf[ElasticSinkTask]
    connector.stop()
  }
}