package com.datamountaineer.streamreactor.connect.elastic

class TestElasticConfig extends TestElasticBase {
  test("A ElasticConfig should return the client mode and hostnames") {
    val config = new ElasticSinkConfig(getElasticSinkConfigProps)
    config.getBoolean(ElasticSinkConfig.CLIENT_MODE_LOCAL) shouldBe true
    config.getString(ElasticSinkConfig.HOST_NAME) shouldBe ELASTIC_SEARCH_HOSTNAMES
  }
}