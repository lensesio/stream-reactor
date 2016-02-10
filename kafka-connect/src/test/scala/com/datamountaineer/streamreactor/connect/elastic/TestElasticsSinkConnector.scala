package com.datamountaineer.streamreactor.connect.elastic

import scala.collection.JavaConverters._

class TestElasticsSinkConnector extends TestElasticBase {
  test("Should start a Elastic Search Connector") {
    val config = getElasticSinkConfigProps
    val connector = new ElasticSinkConnector()
    connector.start(config)
    val taskConfigs = connector.taskConfigs(10)
    taskConfigs.asScala.head.get(ElasticSinkConfig.CLIENT_MODE_LOCAL) shouldBe "true"
    taskConfigs.asScala.head.get(ElasticSinkConfig.HOST_NAME) shouldBe ELASTIC_SEARCH_HOSTNAMES
    taskConfigs.size() shouldBe 10
    connector.taskClass() shouldBe classOf[ElasticSinkTask]
    connector.version() shouldBe ""
    connector.stop()
  }
}