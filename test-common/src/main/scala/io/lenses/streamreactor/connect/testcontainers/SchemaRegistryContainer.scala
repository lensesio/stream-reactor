package io.lenses.streamreactor.connect.testcontainers

import io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer.{defaultNetworkAlias, defaultPort}
import org.testcontainers.containers.{GenericContainer, KafkaContainer}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

class SchemaRegistryContainer(
  dockerImage:      String,
  val networkAlias: String = defaultNetworkAlias,
  val port:         Int    = defaultPort,
  kafkaContainer:   KafkaContainer,
) extends GenericContainer[SchemaRegistryContainer](DockerImageName.parse(dockerImage)) {
  require(kafkaContainer != null, "You must define the kafka container")

  withNetwork(kafkaContainer.getNetwork)
  withNetworkAliases(networkAlias)
  withExposedPorts(port)
  waitingFor(Wait.forHttp("/subjects").forPort(port).forStatusCode(200))

  withEnv("SCHEMA_REGISTRY_HOST_NAME", networkAlias)
  withEnv("SCHEMA_REGISTRY_LISTENERS", s"http://0.0.0.0:$port")
  withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", kafkaContainer.bootstrapServers)

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    def schemaRegistryUrl: String = s"http://$getHost:${getMappedPort(port)}"
  }

  def schemaRegistryUrl: String = s"http://$networkAlias:$port"
}

object SchemaRegistryContainer {
  private val dockerImage                     = DockerImageName.parse("confluentinc/cp-schema-registry")
  private val defaultConfluentPlatformVersion = sys.env.getOrElse("CONFLUENT_VERSION", "6.1.0")
  private val defaultNetworkAlias             = "schema-registry"
  private val defaultPort                     = 8081

  def apply(
    confluentPlatformVersion: String = defaultConfluentPlatformVersion,
    networkAlias:             String = defaultNetworkAlias,
    schemaRegistryPort:       Int    = defaultPort,
    kafkaContainer:           KafkaContainer,
  ): SchemaRegistryContainer =
    new SchemaRegistryContainer(dockerImage.withTag(confluentPlatformVersion).toString,
                                networkAlias,
                                schemaRegistryPort,
                                kafkaContainer,
    )
}
