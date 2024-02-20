/*
 * Copyright 2017-2024 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.testcontainers

import io.lenses.streamreactor.connect.testcontainers.KafkaVersions.ConfluentVersion
import io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer.defaultPort
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.KafkaContainer
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
  private val dockerImage         = DockerImageName.parse("confluentinc/cp-schema-registry")
  private val defaultNetworkAlias = "schema-registry"
  private val defaultPort         = 8081

  def apply(
    confluentPlatformVersion: String = ConfluentVersion,
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
