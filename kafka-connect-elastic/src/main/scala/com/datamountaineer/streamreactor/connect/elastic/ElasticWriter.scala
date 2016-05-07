/**
  * Copyright 2015 Datamountaineer.
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
  **/

package com.datamountaineer.streamreactor.connect.elastic

import com.sksamuel.elastic4s.{ElasticClient, ElasticsearchClientUri}
import org.apache.kafka.connect.sink.SinkTaskContext
import org.elasticsearch.common.settings.Settings

object  ElasticWriter {
  /**
    * Construct a JSONWriter.
    *
    * @param config An elasticSinkConfig to extract settings from.
    * @param context A sink context.
    * @return An ElasticJsonWriter to write records from Kafka to ElasticSearch.
    * */
  def apply(config: ElasticSinkConfig, context: SinkTaskContext) : ElasticJsonWriter = {
    val hostNames = config.getString(ElasticSinkConfig.URL)
    val esClusterName = config.getString(ElasticSinkConfig.ES_CLUSTER_NAME)
    val esPrefix = config.getString(ElasticSinkConfig.URL_PREFIX)
    val essettings = Settings
              .settingsBuilder()
              .put(s"${ElasticSinkConfig.ES_CLUSTER_NAME}", esClusterName)
              .build()
    val uri = ElasticsearchClientUri(s"$esPrefix://$hostNames")
    val client = ElasticClient.transport(essettings, uri)
    new ElasticJsonWriter(client = client, context = context)
  }
}
