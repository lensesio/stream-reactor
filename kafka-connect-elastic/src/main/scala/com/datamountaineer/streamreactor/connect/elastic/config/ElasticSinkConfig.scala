/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.elastic.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}


object ElasticSinkConfig {
  val URL = "connect.elastic.url"
  val URL_DOC = "Url including port for Elastic Search cluster node."
  val URL_DEFAULT = "localhost:9300"
  val ES_CLUSTER_NAME = "connect.elastic.cluster.name"
  val ES_CLUSTER_NAME_DEFAULT = "elasticsearch"
  val ES_CLUSTER_NAME_DOC = "Name of the elastic search cluster, used in local mode for setting the connection"
  val URL_PREFIX = "connect.elastic.url.prefix"
  val URL_PREFIX_DOC = "URL connection string prefix"
  val URL_PREFIX_DEFAULT = "elasticsearch"
  val EXPORT_ROUTE_QUERY = "connect.elastic.sink.kcql"
  val EXPORT_ROUTE_QUERY_DOC =  "KCQL expression describing field selection and routes."

  val config: ConfigDef = new ConfigDef()
    .define(URL, Type.STRING, URL_DEFAULT, Importance.HIGH, URL_DOC, "Connection", 1, ConfigDef.Width.MEDIUM, URL)
    .define(ES_CLUSTER_NAME, Type.STRING, ES_CLUSTER_NAME_DEFAULT, Importance.HIGH, ES_CLUSTER_NAME_DOC,
      "Connection", 2, ConfigDef.Width.MEDIUM, ES_CLUSTER_NAME)
    .define(URL_PREFIX, Type.STRING, URL_PREFIX_DEFAULT, Importance.LOW, URL_PREFIX_DOC, "Connection", 3,
      ConfigDef.Width.MEDIUM, URL_PREFIX)
    .define(EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH, EXPORT_ROUTE_QUERY_DOC, "Target", 1,
      ConfigDef.Width.LONG, EXPORT_ROUTE_QUERY)
}

/**
  * <h1>ElasticSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  * */
case class ElasticSinkConfig(props: util.Map[String, String]) extends AbstractConfig(ElasticSinkConfig.config, props)
