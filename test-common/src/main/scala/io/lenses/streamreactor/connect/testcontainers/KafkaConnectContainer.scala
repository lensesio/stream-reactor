package io.lenses.streamreactor.connect.testcontainers

import com.github.dockerjava.api.model.Ulimit
import io.lenses.streamreactor.connect.testcontainers.KafkaConnectContainer.{defaultNetworkAlias, defaultRestPort}
import org.testcontainers.containers.{GenericContainer, KafkaContainer}
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.Duration

class KafkaConnectContainer(
  dockerImage:             String,
  networkAlias:            String         = defaultNetworkAlias,
  restPort:                Int            = defaultRestPort,
  connectPluginPath:       Option[String] = None,
  kafkaContainer:          KafkaContainer,
  schemaRegistryContainer: Option[SchemaRegistryContainer],
) extends GenericContainer[KafkaConnectContainer](DockerImageName.parse(dockerImage)) {
  require(kafkaContainer != null, "You must define the kafka container")
  require(schemaRegistryContainer != null, "You must define the schema registry container")

  withNetwork(kafkaContainer.getNetwork)
  withNetworkAliases(networkAlias)
  withExposedPorts(restPort)
  waitingFor(Wait.forHttp("/connectors")
    .forPort(restPort)
    .forStatusCode(200))
    .withStartupTimeout(Duration.ofSeconds(120))
  withCreateContainerCmdModifier { cmd =>
    val _ = cmd.getHostConfig.withUlimits(Array(new Ulimit("nofile", 65536L, 65536L)))
  }

  connectPluginPath.foreach(f => withFileSystemBind(f, "/usr/share/plugins"))

  if (schemaRegistryContainer.isDefined) {
    withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
    withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryContainer.get.schemaRegistryUrl)
    withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
    withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", schemaRegistryContainer.get.schemaRegistryUrl)
  } else {
    withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
    withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
    withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
    withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.storage.StringConverter")
  }

  withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", networkAlias)
  withEnv("CONNECT_REST_PORT", restPort.toString)
  withEnv("CONNECT_BOOTSTRAP_SERVERS", kafkaContainer.bootstrapServers)
  withEnv("CONNECT_GROUP_ID", "connect")
  withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "connect_config")
  withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "connect_offset")
  withEnv("CONNECT_STATUS_STORAGE_TOPIC", "connect_status")
  withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
  withEnv("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components,/usr/share/plugins")

  lazy val hostNetwork = new HostNetwork()

  class HostNetwork {
    def restEndpointUrl: String = s"http://$getHost:${getMappedPort(restPort)}"
  }
}

object KafkaConnectContainer {
  private val dockerImage = DockerImageName.parse("confluentinc/cp-kafka-connect")
  private val defaultConfluentPlatformVersion: String = sys.env.getOrElse("CONFLUENT_VERSION", "6.1.0")
  private val defaultNetworkAlias = "connect"
  private val defaultRestPort     = 8083

  def apply(
    confluentPlatformVersion: String                          = defaultConfluentPlatformVersion,
    networkAlias:             String                          = defaultNetworkAlias,
    restPort:                 Int                             = defaultRestPort,
    connectPluginPath:        Option[String]                  = None,
    kafkaContainer:           KafkaContainer,
    schemaRegistryContainer:  Option[SchemaRegistryContainer] = None,
  ): KafkaConnectContainer =
    new KafkaConnectContainer(dockerImage.withTag(confluentPlatformVersion).toString,
                              networkAlias,
                              restPort,
                              connectPluginPath,
                              kafkaContainer,
                              schemaRegistryContainer,
    )
}
