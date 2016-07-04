package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.streamreactor.connect.elastic.config.ElasticSinkConfig

class TestElasticConfig extends TestElasticBase {
  "A ElasticConfig should return the client mode and hostnames" in {
    val config = new ElasticSinkConfig(getElasticSinkConfigProps)
    config.getString(ElasticSinkConfig.URL) shouldBe ELASTIC_SEARCH_HOSTNAMES
    config.getString(ElasticSinkConfig.ES_CLUSTER_NAME) shouldBe ElasticSinkConfig.ES_CLUSTER_NAME_DEFAULT
    config.getString(ElasticSinkConfig.EXPORT_ROUTE_QUERY) shouldBe QUERY
  }
}