package com.datamountaineer.streamreactor.connect.elastic

import java.util

import io.confluent.common.config.ConfigDef.{Importance, Type}
import io.confluent.common.config.{ConfigDef, AbstractConfig}

object ElasticSinkConfig {
  val URL = "url"
  val URL_DOC = "Url including port for Elastic Search cluster node."
  val URL_DEFAULT = "localhost:9300"
  val ES_CLUSTER_NAME = "cluster.name"
  val ES_CLUSTER_NAME_DEFAULT = "elasticsearch"
  val ES_CLUSTER_NAME_DOC = "Name of the elastic search cluster, used in local mode for setting the connection"
  val URL_PREFIX = "url.prefix"
  val URL_PREFIX_DOC = "URL connection string prefix"
  val URL_PREFIX_DEFAULT = "elasticsearch"

  val config: ConfigDef = new ConfigDef()
    .define(URL, Type.STRING, URL_DEFAULT, Importance.HIGH, URL_DOC)
    .define(ES_CLUSTER_NAME, Type.STRING, ES_CLUSTER_NAME_DEFAULT, Importance.HIGH, ES_CLUSTER_NAME_DOC)
    .define(URL_PREFIX, Type.STRING, URL_PREFIX_DEFAULT, Importance.LOW, URL_PREFIX_DOC)
}


/**
  * <h1>ElasticSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  * */
class ElasticSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(ElasticSinkConfig.config, props) {
}