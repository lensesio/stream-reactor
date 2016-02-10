package com.datamountaineer.streamreactor.connect.elastic

import com.datamountaineer.streamreactor.connect.TestBase
import scala.collection.JavaConverters._

trait TestElasticBase extends TestBase {
  val ELASTIC_SEARCH_HOSTNAMES="localhost:9300"

  def getElasticSinkConfigProps = {
    Map (
      ElasticSinkConfig.CLIENT_MODE_LOCAL->true.toString,
      ElasticSinkConfig.HOST_NAME->ELASTIC_SEARCH_HOSTNAMES
    ).asJava
  }
}