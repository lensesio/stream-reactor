package io.lenses.streamreactor.connect

import io.lenses.streamreactor.connect.MqttContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.MqttContainer.defaultPort
import io.lenses.streamreactor.connect.MqttContainer.defaultTag
import io.lenses.streamreactor.connect.testcontainers.RandomAuthentication
import io.lenses.streamreactor.connect.testcontainers.S3Authentication
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class MqttContainer(
  dockerImage:  DockerImageName,
  dockerTag:    String           = defaultTag,
  networkAlias: String           = defaultNetworkAlias,
  port:         Int              = defaultPort,
  val identity: S3Authentication = RandomAuthentication(),
) extends GenericContainer[MqttContainer](dockerImage.withTag(dockerTag)) {

  withFileSystemBind(this.getClass.getResource("/mosquitto.config").getPath, "/mosquitto/config/mosquitto.conf")
  withNetworkAliases(networkAlias)
  withExposedPorts(port)
  waitingFor(Wait.forListeningPort())

  def getExtMqttConnectionUrl: String = s"tcp://$getHost:${getMappedPort(port)}"
  def getMqttConnectionUrl:    String = s"tcp://$defaultNetworkAlias:$defaultPort"

  val mqttUser     = "user"
  val mqttPassword = "passwd"

}

object MqttContainer {
  private val dockerImage         = DockerImageName.parse("eclipse-mosquitto")
  private val defaultTag          = "2.0.15"
  private val defaultNetworkAlias = "mqtt"
  private val defaultPort         = 1883

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
    port:         Int    = defaultPort,
  ): MqttContainer =
    new MqttContainer(dockerImage, dockerTag, networkAlias, port)
}
