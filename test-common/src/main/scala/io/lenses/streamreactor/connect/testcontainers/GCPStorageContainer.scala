package io.lenses.streamreactor.connect.testcontainers

import com.typesafe.scalalogging.Logger
import io.lenses.streamreactor.connect.testcontainers.GCPStorageContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.GCPStorageContainer.defaultPort
import io.lenses.streamreactor.connect.testcontainers.GCPStorageContainer.defaultTag
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.output.Slf4jLogConsumer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.net.URI

class GCPStorageContainer(
  dockerImage:  DockerImageName,
  dockerTag:    String = defaultTag,
  networkAlias: String = defaultNetworkAlias,
  port:         Int    = defaultPort,
) extends GenericContainer[GCPStorageContainer](dockerImage.withTag(dockerTag)) {

  protected lazy val overrideLogger: Logger =
    Logger(LoggerFactory.getLogger(getClass.getName))

  withNetworkAliases(networkAlias)
  withExposedPorts(port)
  waitingFor(Wait.forListeningPort())
    .withLogConsumer(new Slf4jLogConsumer(overrideLogger.underlying))

  withCommand("-scheme=http", s"-port=$port", "-backend=memory")

  def getNetworkAliasUrl: URI =
    new URI(
      s"http://$networkAlias:$port",
    )

}

object GCPStorageContainer {
  private val dockerImage         = DockerImageName.parse("fsouza/fake-gcs-server")
  private val defaultTag          = "1.47.6"
  private val defaultNetworkAlias = "gstore"
  private val defaultPort         = 4443

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
    port:         Int    = defaultPort,
  ): GCPStorageContainer =
    new GCPStorageContainer(dockerImage, dockerTag, networkAlias, port)
}
