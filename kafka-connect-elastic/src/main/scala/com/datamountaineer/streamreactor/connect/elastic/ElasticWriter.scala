package com.datamountaineer.streamreactor.connect.elastic

import com.sksamuel.elastic4s.{ElasticsearchClientUri, ElasticClient}
import org.apache.kafka.connect.sink.SinkTaskContext
import org.elasticsearch.common.settings.Settings

object  ElasticWriter {
  def apply(config: ElasticSinkConfig, context: SinkTaskContext) = {
    val hostNames = config.getString(ElasticSinkConfig.HOST_NAME)
    val esHomePath = config.getString(ElasticSinkConfig.ES_PATH_HOME)
    val esClusterName = config.getString(ElasticSinkConfig.ES_CLUSTER_NAME)
    val split = hostNames.split(",")
    val client = (split.contains(s"${ElasticConstants.LOCAL_HOST}:" +
      s"${ElasticConstants.PORT}") || split.contains(s"${ElasticConstants.LOCAL_IP}:${ElasticConstants.PORT}"))
    match {
      case true => {
        val essettings = Settings
          .settingsBuilder().put(s"${ElasticConstants.CLUSTER_NAME}", esClusterName)
          .put(s"${ElasticConstants.PATH_HOME}", esHomePath).build()
        ElasticClient.local(essettings)
      }
      case false => {
        val uri = ElasticsearchClientUri(s"${ElasticConstants.URL_PREFIX}://$hostNames")
        ElasticClient.remote(uri)
      }
    }
    new ElasticJsonWriter(client = client, context = context)
  }
}
