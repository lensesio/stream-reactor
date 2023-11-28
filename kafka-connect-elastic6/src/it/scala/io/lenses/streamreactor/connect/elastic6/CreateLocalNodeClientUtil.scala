package io.lenses.streamreactor.connect.elastic6

import com.sksamuel.elastic4s.http.ElasticClient
import com.sksamuel.elastic4s.http.ElasticProperties
import org.testcontainers.elasticsearch.ElasticsearchContainer

object CreateLocalNodeClientUtil {

  private val url = "docker.elastic.co/elasticsearch/elasticsearch:6.8.21"

  def createLocalNode() = {
    val container = new ElasticsearchContainer(url)
    //container.withReuse(true)
    container.start()
    container
  }

  def createLocalNodeClient(localNode: ElasticsearchContainer) = {
    val esProps = ElasticProperties(s"http://${localNode.getHttpHostAddress}")
    ElasticClient(esProps)
  }
}
