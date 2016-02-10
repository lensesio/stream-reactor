package com.datamountaineer.streamreactor.connect.elastic

import com.sksamuel.elastic4s.{ElasticsearchClientUri, ElasticClient}
import org.apache.kafka.connect.sink.SinkTaskContext

object  ElasticWriter {
  def apply(config: ElasticSinkConfig, context: SinkTaskContext) = {
    val localMode = config.getBoolean(ElasticSinkConfig.CLIENT_MODE_LOCAL)
    val hostNames = config.getString(ElasticSinkConfig.HOST_NAME)

    //set up es client
    val client = localMode match {
      case true => ElasticClient.local
      case false =>
        val uri = ElasticsearchClientUri(s"elasticsearch://$hostNames")
        ElasticClient.remote(uri)
    }
    new ElasticJsonWriter(client = client, context = context)
  }
}
