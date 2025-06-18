/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import io.lenses.streamreactor.connect.cosmos.CosmosCertLoader
import io.lenses.streamreactor.connect.cosmos.TempKeyStoreWriter
import io.lenses.streamreactor.connect.testcontainers.CosmosContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.CosmosContainer.defaultPort
import io.lenses.streamreactor.connect.testcontainers.CosmosContainer.defaultTag
import org.testcontainers.containers.CosmosDBEmulatorContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.io.File
import java.net.URI
import java.security.KeyStore

class CosmosContainer(
  dockerImage:  DockerImageName,
  dockerTag:    String = defaultTag,
  networkAlias: String = defaultNetworkAlias,
  port:         Int    = defaultPort,
) extends CosmosDBEmulatorContainer(dockerImage.withTag(dockerTag)) {

  withNetworkAliases(networkAlias)

  withExposedPorts(port)

  withEnv("PROTOCOL", "https")

  waitingFor(Wait.forHttps("/").allowInsecure().forStatusCode(200))

  val trustStore:   KeyStore = CosmosCertLoader.extractCertAsTrustStore(this)
  val keyStoreFile: File     = TempKeyStoreWriter.writeToTempFile(trustStore, "changeIt".toCharArray)

  System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getPath)
  System.setProperty("javax.net.ssl.trustStorePassword", "changeIt")

  ()

  def getNetworkAliasUrl: URI =
    new URI(
      s"http://s3:$port",
    )

  def getEndpointUrl: URI =
    new URI(
      s"http://$getHost:${getMappedPort(port)}",
    )

  def container: PausableContainer = new TestContainersPausableContainer(this)
}

object CosmosContainer {
  private val dockerImage         = DockerImageName.parse("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator")
  private val defaultTag          = "vnext-preview"
  private val defaultNetworkAlias = "cosmos-emulator"
  private val defaultPort         = 8081

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
    port:         Int    = defaultPort,
  ): CosmosContainer =
    new CosmosContainer(dockerImage, dockerTag, networkAlias, port)
}
