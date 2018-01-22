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

package com.datamountaineer.streamreactor.connect.elastic6

import com.datamountaineer.streamreactor.connect.elastic6.config.{ElasticConfig, ElasticConfigConstants, ElasticSettings}
import com.sksamuel.elastic4s.ElasticsearchClientUri
import org.elasticsearch.common.settings.Settings


object ElasticWriter {
  /**
    * Construct a JSONWriter.
    *
    * @param config An elasticSinkConfig to extract settings from.
    * @return An ElasticJsonWriter to write records from Kafka to ElasticSearch.
    **/
  def apply(config: ElasticConfig): ElasticJsonWriter = {
    val hostNames = config.getString(ElasticConfigConstants.URL)
    val esClusterName = config.getString(ElasticConfigConstants.ES_CLUSTER_NAME)
    val esPrefix = config.getString(ElasticConfigConstants.URL_PREFIX)
    val essettings: Settings = Settings
      .builder()
      .put("cluster.name", esClusterName)
      .build()
    val uri: ElasticsearchClientUri = ElasticsearchClientUri(s"$esPrefix://$hostNames")

    val settings = ElasticSettings(config)


    new ElasticJsonWriter(KElasticClient.getClient(settings, essettings, uri), settings)
  }
}
