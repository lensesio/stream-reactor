package io.lenses.streamreactor.connect.testcontainers

import cats.effect.IO
import cats.effect.kernel.Resource
import com.mongodb.MongoClient
import io.lenses.streamreactor.connect.testcontainers.MongoDBContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.MongoDBContainer.defaultTag
import org.testcontainers.containers.{MongoDBContainer => JavaMongoDBContainer}
import org.testcontainers.utility.DockerImageName

class MongoDBContainer(
  dockerImage:      DockerImageName,
  dockerTag:        String = defaultTag,
  val networkAlias: String = defaultNetworkAlias,
) extends SingleContainer[JavaMongoDBContainer] {

  val port: Int = 27017

  override val container: JavaMongoDBContainer =
    new JavaMongoDBContainer(dockerImage.withTag(dockerTag))
  container.withNetworkAliases(networkAlias)

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    val mongoClient = Resource.fromAutoCloseable(IO(new MongoClient(container.getHost, container.getMappedPort(port))))
  }
}

object MongoDBContainer {
  private val dockerImage         = DockerImageName.parse("mongo")
  private val defaultTag          = "4.0.10"
  private val defaultNetworkAlias = "mongo"

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
  ): MongoDBContainer =
    new MongoDBContainer(dockerImage, dockerTag, networkAlias)
}
