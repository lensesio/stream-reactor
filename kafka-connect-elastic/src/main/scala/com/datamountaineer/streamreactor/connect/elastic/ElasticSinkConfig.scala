package com.datamountaineer.streamreactor.connect.elastic

import java.util

import io.confluent.common.config.ConfigDef.{Importance, Type}
import io.confluent.common.config.{ConfigDef, AbstractConfig}

object ElasticSinkConfig {
  val HOST_NAME = "host_name"
  val HOST_NAME_DOC = "Host names in Elastic Search cluster."
  val HOST_NAME_DEFAULT = "localhost"
  val ES_PATH_HOME="path.home for elastic4s client in local mode"
  val ES_PATH_HOME_DEFAULT="/tmp/elastic"
  val ES_PATH_HOME_DOC="path.home for elastic4s client in local mode"
  val ES_CLUSTER_NAME="elastic_cluster_name"
  val ES_CLUSTER_NAME_DEFAULT=ElasticConstants.DEFAULT_CLUSTER_NAME
  val ES_CLUSTER_NAME_DOC="Name of the elastic search cluster, used in local mode for setting the connection"

  val config: ConfigDef = new ConfigDef()
    .define(HOST_NAME, Type.STRING, HOST_NAME_DEFAULT, Importance.HIGH, HOST_NAME_DOC)
    .define(ES_PATH_HOME, Type.STRING, ES_PATH_HOME_DEFAULT, Importance.LOW, ES_PATH_HOME_DOC)
    .define(ES_CLUSTER_NAME, Type.STRING, ES_CLUSTER_NAME_DEFAULT, Importance.LOW, ES_CLUSTER_NAME_DOC)
}


/**
  * <h1>ElasticSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  * */
class ElasticSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(ElasticSinkConfig.config, props) {
}