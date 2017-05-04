/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.elastic.config

import java.util

import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}


object ElasticSinkConfig {

  val config: ConfigDef = new ConfigDef()
    .define(ElasticSinkConfigConstants.URL, Type.STRING, ElasticSinkConfigConstants.URL_DEFAULT, Importance.HIGH,
      ElasticSinkConfigConstants.URL_DOC, "Connection", 1, ConfigDef.Width.MEDIUM, ElasticSinkConfigConstants.URL)
    .define(ElasticSinkConfigConstants.ES_CLUSTER_NAME, Type.STRING, ElasticSinkConfigConstants.ES_CLUSTER_NAME_DEFAULT,
      Importance.HIGH, ElasticSinkConfigConstants.ES_CLUSTER_NAME_DOC, "Connection", 2, ConfigDef.Width.MEDIUM,
      ElasticSinkConfigConstants.ES_CLUSTER_NAME)
    .define(ElasticSinkConfigConstants.URL_PREFIX, Type.STRING, ElasticSinkConfigConstants.URL_PREFIX_DEFAULT,
      Importance.LOW, ElasticSinkConfigConstants.URL_PREFIX_DOC, "Connection", 3, ConfigDef.Width.MEDIUM,
      ElasticSinkConfigConstants.URL_PREFIX)
    .define(ElasticSinkConfigConstants.EXPORT_ROUTE_QUERY, Type.STRING, Importance.HIGH,
      ElasticSinkConfigConstants.EXPORT_ROUTE_QUERY_DOC, "Target", 1, ConfigDef.Width.LONG,
      ElasticSinkConfigConstants.EXPORT_ROUTE_QUERY)
    .define(ElasticSinkConfigConstants.INDEX_NAME_SUFFIX, Type.STRING, ElasticSinkConfigConstants.INDEX_NAME_SUFFIX_DEFAULT,
        Importance.LOW, ElasticSinkConfigConstants.INDEX_NAME_SUFFIX_DOC, "Target", 2, ConfigDef.Width.MEDIUM, ElasticSinkConfigConstants.INDEX_NAME_SUFFIX)
    .define(ElasticSinkConfigConstants.AUTO_CREATE_INDEX, Type.BOOLEAN, ElasticSinkConfigConstants.AUTO_CREATE_INDEX_DEFAULT,
        Importance.LOW, ElasticSinkConfigConstants.AUTO_CREATE_INDEX_DOC, "Target", 3, ConfigDef.Width.MEDIUM, ElasticSinkConfigConstants.AUTO_CREATE_INDEX)
    .define(ElasticSinkConfigConstants.DOCUMENT_TYPE, Type.STRING, ElasticSinkConfigConstants.DOCUMENT_TYPE_DEFAULT, Importance.LOW, ElasticSinkConfigConstants.DOCUMENT_TYPE_DOC, "Target", 4, ConfigDef.Width.MEDIUM, ElasticSinkConfigConstants.DOCUMENT_TYPE)
}

/**
  * <h1>ElasticSinkConfig</h1>
  *
  * Holds config, extends AbstractConfig.
  * */
case class ElasticSinkConfig(props: util.Map[String, String]) extends AbstractConfig(ElasticSinkConfig.config, props)
