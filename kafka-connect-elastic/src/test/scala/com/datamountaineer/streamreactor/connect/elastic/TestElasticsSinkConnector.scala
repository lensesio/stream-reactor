package com.datamountaineer.streamreactor.connect.elastic

import scala.collection.JavaConverters._

class TestElasticsSinkConnector extends TestElasticBase {
  test("Should start a Elastic Search Connector") {
    //get config
    val config = getElasticSinkConfigProps(TMP.toString)
    //get connector
    val connector = new ElasticSinkConnector()
    //start with config
    connector.start(config)
    //check config
    val taskConfigs = connector.taskConfigs(10)
    taskConfigs.asScala.head.get(ElasticSinkConfig.HOST_NAME) shouldBe ELASTIC_SEARCH_HOSTNAMES
    taskConfigs.size() shouldBe 10
    //check connector
    connector.taskClass() shouldBe classOf[ElasticSinkTask]
    connector.version() shouldBe ""
    connector.stop()
  }
}