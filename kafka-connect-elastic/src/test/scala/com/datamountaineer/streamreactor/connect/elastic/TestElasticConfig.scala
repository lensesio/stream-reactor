package com.datamountaineer.streamreactor.connect.elastic

class TestElasticConfig extends TestElasticBase {
  test("A ElasticConfig should return the client mode and hostnames") {
    val config = new ElasticSinkConfig(getElasticSinkConfigProps)
    config.getString(ElasticSinkConfig.URL) shouldBe ELASTIC_SEARCH_HOSTNAMES
    config.getString(ElasticSinkConfig.ES_CLUSTER_NAME) shouldBe ElasticSinkConfig.ES_CLUSTER_NAME_DEFAULT
  }
}