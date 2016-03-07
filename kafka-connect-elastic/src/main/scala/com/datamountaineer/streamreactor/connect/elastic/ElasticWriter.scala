package com.datamountaineer.streamreactor.connect.elastic

import com.sksamuel.elastic4s.{ElasticsearchClientUri, ElasticClient}
import org.apache.kafka.connect.sink.SinkTaskContext
import org.elasticsearch.common.settings.Settings

object  ElasticWriter {
  def apply(config: ElasticSinkConfig, context: SinkTaskContext) = {
    val hostNames = config.getString(ElasticSinkConfig.URL)
    val esClusterName = config.getString(ElasticSinkConfig.ES_CLUSTER_NAME)
    val esPrefix = config.getString(ElasticSinkConfig.URL_PREFIX)
    val essettings = Settings
              .settingsBuilder()
              .put(s"${ElasticSinkConfig.ES_CLUSTER_NAME}", esClusterName)
              .build()
    val uri = ElasticsearchClientUri(s"${esPrefix}://$hostNames")
    val client = ElasticClient.transport(essettings, uri)
    new ElasticJsonWriter(client = client, context = context)
  }
}
