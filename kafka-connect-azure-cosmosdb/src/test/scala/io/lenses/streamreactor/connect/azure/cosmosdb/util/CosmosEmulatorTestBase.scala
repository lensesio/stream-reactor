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
package io.lenses.streamreactor.connect.azure.cosmosdb.util

import io.lenses.streamreactor.connect.cosmos.{CosmosCertLoader, TempKeyStoreWriter}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.containers.CosmosDBEmulatorContainer
import org.testcontainers.containers.Network
import org.testcontainers.utility.DockerImageName

trait CosmosEmulatorTestBase extends AnyFunSuiteLike with BeforeAndAfterAll {

  protected val network: Network = Network.newNetwork()
  protected var cosmosEmulatorContainer: CosmosDBEmulatorContainer = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    startCosmosEmulator()
  }

  private def startCosmosEmulator(): Unit = {
    cosmosEmulatorContainer = new CosmosDBEmulatorContainer(
      DockerImageName.parse("mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview"),
    ).withEnv("PROTOCOL", "https")
      .withNetwork(network)
      .withNetworkAliases("cosmos-emulator")
      .waitingFor(Wait.forHttps("/").allowInsecure().forStatusCode(200))

    cosmosEmulatorContainer.start()

    val trustStore   = CosmosCertLoader.extractCertAsTrustStore(cosmosEmulatorContainer)
    val keyStoreFile = TempKeyStoreWriter.writeToTempFile(trustStore, "changeIt".toCharArray)

    System.setProperty("javax.net.ssl.trustStore", keyStoreFile.getPath)
    System.setProperty("javax.net.ssl.trustStorePassword", "changeIt")

    ()
  }

  override def afterAll(): Unit = {
    if (cosmosEmulatorContainer != null) {
      cosmosEmulatorContainer.stop()
    }
    cosmosEmulatorContainer = null
    super.afterAll()
  }

}
