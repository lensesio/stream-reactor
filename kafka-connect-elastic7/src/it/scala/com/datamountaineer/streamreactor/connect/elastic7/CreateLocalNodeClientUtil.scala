package io.lenses.streamreactor.connect.elastic7

import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticProperties
import org.testcontainers.elasticsearch.ElasticsearchContainer

object CreateLocalNodeClientUtil {

  private val url = "docker.elastic.co/elasticsearch/elasticsearch:7.2.0"

  def createLocalNode() = {
    val container = new ElasticsearchContainer(url)
    //container.withReuse(true)
    container.start()
    container
  }

  def createLocalNodeClient(localNode: ElasticsearchContainer) = {
    val esProps = ElasticProperties(s"http://${localNode.getHttpHostAddress}")
    ElasticClient(JavaClient(esProps))
  }
}
