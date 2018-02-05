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

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst._

object ElasticConfigConstants {
  
  val CONNECTOR_PREFIX = s"connect.elastic"

  val URL = s"$CONNECTOR_PREFIX.$URI_SUFFIX"
  val URL_DOC = "Url including port for Elastic Search cluster node."
  val URL_DEFAULT = "localhost:9300"
  val ES_CLUSTER_NAME = s"$CONNECTOR_PREFIX.${
    CLUSTER_NAME_SUFFIX}"
  val ES_CLUSTER_NAME_DEFAULT = "elasticsearch"
  val ES_CLUSTER_NAME_DOC = "Name of the elastic search cluster, used in local mode for setting the connection"
  val URL_PREFIX = s"$URL.prefix"
  val URL_PREFIX_DOC = "URL connection string prefix"
  val URL_PREFIX_DEFAULT = "elasticsearch"
  val KCQL = s"$CONNECTOR_PREFIX.kcql"
  val KCQL_DOC = "KCQL expression describing field selection and routes."

  val WRITE_TIMEOUT_CONFIG = s"$CONNECTOR_PREFIX.$WRITE_TIMEOUT_SUFFIX"
  val WRITE_TIMEOUT_DOC = "The time to wait in millis. Default is 5 minutes."
  val WRITE_TIMEOUT_DISPLAY = "Write timeout"
  val WRITE_TIMEOUT_DEFAULT = 300000

  val THROW_ON_ERROR_CONFIG = s"$CONNECTOR_PREFIX.error.throw"
  val THROW_ON_ERROR_DOC = "Throws the exception on write. Default is 'true'"
  val THROW_ON_ERROR_DISPLAY = "Throw on errors"
  val THROW_ON_ERROR_DEFAULT = true

  val ERROR_RETRY_INTERVAL = s"${CONNECTOR_PREFIX}.${RETRY_INTERVAL_PROP_SUFFIX}"
  val ERROR_RETRY_INTERVAL_DOC = "The time in milliseconds between retries."
  val ERROR_RETRY_INTERVAL_DEFAULT = "60000"

  /*
  val INDEX_NAME_SUFFIX = s"${CONNECTOR_PREFIX}.index.suffix"
  val INDEX_NAME_SUFFIX_DOC = "Suffix to append to the index name. Supports date time notation inside curly brackets. E.g. 'abc_{YYYY-MM-dd}_def'"
  val INDEX_NAME_SUFFIX_DEFAULT: String = null

  val AUTO_CREATE_INDEX = s"${CONNECTOR_PREFIX}.index.auto.create"
  val AUTO_CREATE_INDEX_DOC = "The flag enables/disables auto creating the ElasticSearch index. Boolean value required. Defaults to TRUE."
  val AUTO_CREATE_INDEX_DEFAULT = true

  val DOCUMENT_TYPE = s"${CONNECTOR_PREFIX}.document.type"
  val DOCUMENT_TYPE_DOC = "Sets the ElasticSearch document type. See https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-type-field.html for more info."
  val DOCUMENT_TYPE_DEFAULT: String = null
  */

  val PROGRESS_COUNTER_ENABLED = s"$PROGRESS_ENABLED_CONST"
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"
}
