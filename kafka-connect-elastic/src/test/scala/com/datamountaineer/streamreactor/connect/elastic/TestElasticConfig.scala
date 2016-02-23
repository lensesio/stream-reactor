package com.datamountaineer.streamreactor.connect.elastic

class TestElasticConfig extends TestElasticBase {
  test("A ElasticConfig should return the client mode and hostnames") {
    val config = new ElasticSinkConfig(getElasticSinkConfigProps("test"))
    config.getString(ElasticSinkConfig.HOST_NAME) shouldBe ELASTIC_SEARCH_HOSTNAMES
    config.getString(ElasticSinkConfig.ES_PATH_HOME) shouldBe "test"
  }
}