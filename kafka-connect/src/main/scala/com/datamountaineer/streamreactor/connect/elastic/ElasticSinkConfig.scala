package com.datamountaineer.streamreactor.connect.elastic

import java.util

import io.confluent.common.config.ConfigDef.{Importance, Type}
import io.confluent.common.config.{ConfigDef, AbstractConfig}

object ElasticSinkConfig {
  val HOST_NAME = "host_name"
  val HOST_NAME_DOC = "Host names in Elastic Search cluster."
  val HOST_NAME_DEFAULT = "localhost"
  val CLIENT_MODE_LOCAL="client_mode"
  val CLIENT_MODE_LOCAL_DEFAULT="false"
  val CLIENT_MODE_LOCAL_DOC="Mode for the elastic4s client"

  val config: ConfigDef = new ConfigDef()
    .define(HOST_NAME, Type.STRING, HOST_NAME_DEFAULT, Importance.HIGH, HOST_NAME_DOC)
    .define(CLIENT_MODE_LOCAL, Type.BOOLEAN, CLIENT_MODE_LOCAL_DEFAULT,Importance.HIGH, CLIENT_MODE_LOCAL_DOC)
}


/**
  * <h1>ElasticSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  * */
class ElasticSinkConfig(props: util.Map[String, String])
  extends AbstractConfig(ElasticSinkConfig.config, props) {
}