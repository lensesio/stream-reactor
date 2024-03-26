package io.lenses.streamreactor.connect.test

import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class WiremockContainer(
  dockerImageName: DockerImageName = DockerImageName.parse("wiremock/wiremock"),
  dockerTag:       String          = "3.4.2-1",
  networkAlias:    String          = "wiremock",
) extends GenericContainer[WiremockContainer](dockerImageName.withTag(dockerTag)) {

  private val port = 8080

  withNetworkAliases(networkAlias)
  withExposedPorts(port)
  waitingFor(Wait.forListeningPort())

  def getNetworkAliasUrl: String =
    s"http://$networkAlias:$port"

  def getEndpointUrl: String =
    s"http://$getHost:${getMappedPort(port)}"

}
