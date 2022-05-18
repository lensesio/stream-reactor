package io.lenses.streamreactor.connect.testcontainers

import io.lenses.streamreactor.connect.testcontainers.ElasticsearchContainer.{defaultNetworkAlias, defaultTag}
import org.testcontainers.elasticsearch.{ElasticsearchContainer => JavaElasticsearchContainer}
import org.testcontainers.utility.DockerImageName

class ElasticsearchContainer(
  dockerImage:      DockerImageName,
  dockerTag:        String = defaultTag,
  val networkAlias: String = defaultNetworkAlias,
) extends SingleContainer[JavaElasticsearchContainer] {

  val port: Int = 9200

  override val container: JavaElasticsearchContainer =
    new JavaElasticsearchContainer(dockerImage.withTag(dockerTag))
  container.withNetworkAliases(networkAlias)

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    def httpHostAddress: String = container.getHttpHostAddress
  }
}

object ElasticsearchContainer {
  private val dockerImage         = DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
  private val defaultTag          = "6.8.8"
  private val defaultNetworkAlias = "elastic"

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
  ): ElasticsearchContainer =
    new ElasticsearchContainer(dockerImage, dockerTag, networkAlias)
}
