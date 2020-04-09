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

package com.datamountaineer.streamreactor.connect.elastic6.config

import com.datamountaineer.streamreactor.connect.config.base.const.TraitConfigConst._

object ElasticConfigConstants {

  val CONNECTOR_PREFIX = "connect.elastic"

  val PROTOCOL = s"${CONNECTOR_PREFIX}.protocol"
  val PROTOCOL_DOC = "URL protocol (http, https)"
  val PROTOCOL_DEFAULT = "http"

  val HOSTS = s"${CONNECTOR_PREFIX}.${CONNECTION_HOSTS_SUFFIX}"
  val HOSTS_DOC = "List of hostnames for Elastic Search cluster node, not including protocol or port."
  val HOSTS_DEFAULT = "localhost"

  val ES_PORT = s"${CONNECTOR_PREFIX}.${CONNECTION_PORT_SUFFIX}"
  val ES_PORT_DOC = "Port on which Elastic Search node listens on"
  val ES_PORT_DEFAULT = "9300"

  val ES_PREFIX = s"${CONNECTOR_PREFIX}.tableprefix"
  val ES_PREFIX_DOC = "Table prefix (optional)"
  val ES_PREFIX_DEFAULT = ""

  val ES_CLUSTER_NAME = s"${CONNECTOR_PREFIX}.${CLUSTER_NAME_SUFFIX}"
  val ES_CLUSTER_NAME_DOC = "Name of the elastic search cluster, used in local mode for setting the connection"
  val ES_CLUSTER_NAME_DEFAULT = "elasticsearch"

  val KCQL = s"${CONNECTOR_PREFIX}.${KCQL_PROP_SUFFIX}"
  val KCQL_DOC = "KCQL expression describing field selection and routes."

  val WRITE_TIMEOUT_CONFIG = s"${CONNECTOR_PREFIX}.${WRITE_TIMEOUT_SUFFIX}"
  val WRITE_TIMEOUT_DOC = "The time to wait in millis. Default is 5 minutes."
  val WRITE_TIMEOUT_DISPLAY = "Write timeout"
  val WRITE_TIMEOUT_DEFAULT = 300000

  val CLIENT_HTTP_BASIC_AUTH_USERNAME = s"$CONNECTOR_PREFIX.use.http.username"
  val CLIENT_HTTP_BASIC_AUTH_USERNAME_DEFAULT = ""
  val CLIENT_HTTP_BASIC_AUTH_USERNAME_DOC = "Username if HTTP Basic Auth required default is null."
  val CLIENT_HTTP_BASIC_AUTH_PASSWORD = s"$CONNECTOR_PREFIX.use.http.password"
  val CLIENT_HTTP_BASIC_AUTH_PASSWORD_DEFAULT = ""
  val CLIENT_HTTP_BASIC_AUTH_PASSWORD_DOC = "Password if HTTP Basic Auth required default is null."

  val NBR_OF_RETRIES_CONFIG = s"${CONNECTOR_PREFIX}.${MAX_RETRIES_PROP_SUFFIX}"
  val NBR_OF_RETRIES_DOC = "The maximum number of times to try the write again."
  val NBR_OF_RETIRES_DEFAULT = 20

  val ERROR_POLICY_CONFIG = s"${CONNECTOR_PREFIX}.${ERROR_POLICY_PROP_SUFFIX}"
  val ERROR_POLICY_DOC: String =
    """Specifies the action to be taken if an error occurs while inserting the data
      |There are two available options:
      |NOOP - the error is swallowed
      |THROW - the error is allowed to propagate.
      |RETRY - The exception causes the Connect framework to retry the message. The number of retries is based on
      |The error will be logged automatically""".stripMargin
  val ERROR_POLICY_DEFAULT = "THROW"

  val BATCH_SIZE_CONFIG = s"$CONNECTOR_PREFIX.$BATCH_SIZE_PROP_SUFFIX"
  val BATCH_SIZE_DOC = "How many records to process at one time. As records are pulled from Kafka it can be 100k+ which will not be feasible to throw at Elastic search at once"
  val BATCH_SIZE_DISPLAY = "Batch size"
  val BATCH_SIZE_DEFAULT = 4000

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

  val PROGRESS_COUNTER_ENABLED = PROGRESS_ENABLED_CONST
  val PROGRESS_COUNTER_ENABLED_DOC = "Enables the output for how many records have been processed"
  val PROGRESS_COUNTER_ENABLED_DEFAULT = false
  val PROGRESS_COUNTER_ENABLED_DISPLAY = "Enable progress counter"

  val PK_JOINER_SEPARATOR = s"$CONNECTOR_PREFIX.pk.separator"
  val PK_JOINER_SEPARATOR_DOC = "Separator used when have more that one field in PK"
  val PK_JOINER_SEPARATOR_DEFAULT = "-"
  val PK_JOINER_SEPARATOR_DISPLAY = "PK joiner separator"
}
