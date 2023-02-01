package io.lenses.streamreactor.connect.testcontainers

import io.lenses.streamreactor.connect.testcontainers.RedisContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.RedisContainer.defaultPort
import io.lenses.streamreactor.connect.testcontainers.RedisContainer.defaultTag
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName
import redis.clients.jedis.Jedis

class RedisContainer(
  dockerImage:      DockerImageName,
  dockerTag:        String = defaultTag,
  val networkAlias: String = defaultNetworkAlias,
  val port:         Int    = defaultPort,
) extends GenericContainer[RedisContainer](dockerImage.withTag(dockerTag)) {

  withNetworkAliases(networkAlias)
  withExposedPorts(port)

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    val jedisClient = new Jedis(getHost, getMappedPort(port))
  }
}

object RedisContainer {
  private val dockerImage         = DockerImageName.parse("redis")
  private val defaultTag          = "6-alpine"
  private val defaultNetworkAlias = "redis"
  private val defaultPort         = 6379

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
    port:         Int    = defaultPort,
  ): RedisContainer =
    new RedisContainer(dockerImage, dockerTag, networkAlias, port)
}
