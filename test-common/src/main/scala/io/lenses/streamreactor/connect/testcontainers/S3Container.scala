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

import io.lenses.streamreactor.connect.testcontainers.S3Container.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.S3Container.defaultPort
import io.lenses.streamreactor.connect.testcontainers.S3Container.defaultTag
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.net.URI
import java.util.UUID

trait S3Authentication {
  val identity:   String
  val credential: String
}

case class RandomAuthentication(
  identity:   String = UUID.randomUUID().toString,
  credential: String = UUID.randomUUID().toString,
) extends S3Authentication

class S3Container(
  dockerImage:  DockerImageName,
  dockerTag:    String           = defaultTag,
  networkAlias: String           = defaultNetworkAlias,
  port:         Int              = defaultPort,
  val identity: S3Authentication = RandomAuthentication(),
) extends GenericContainer[S3Container](dockerImage.withTag(dockerTag)) {

  withNetworkAliases(networkAlias)
  withExposedPorts(port)
  waitingFor(Wait.forListeningPort())

  Map[String, String](
    "S3PROXY_ENDPOINT" -> ("http://0.0.0.0:" + port),
    // S3Proxy currently has an issue with authorization, therefore it is disabled for the time being
    // https://github.com/gaul/s3proxy/issues/392
    "S3PROXY_AUTHORIZATION" -> "none",
    "S3PROXY_IDENTITY"      -> identity.identity,
    "S3PROXY_CREDENTIAL"    -> identity.credential,
    // using the AWS library requires this to be set for testing
    "S3PROXY_IGNORE_UNKNOWN_HEADERS" -> "true",
    "JCLOUDS_REGIONS"                -> "eu-west-1",
    "JCLOUDS_PROVIDER"               -> "filesystem",
    "JCLOUDS_IDENTITY"               -> identity.identity,
    "JCLOUDS_CREDENTIAL"             -> identity.credential,
  ).foreach { case (k, v) => addEnv(k, v) }

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

object S3Container {
  private val dockerImage         = DockerImageName.parse("andrewgaul/s3proxy")
  private val defaultTag          = "sha-ba0fd6d"
  private val defaultNetworkAlias = "s3"
  private val defaultPort         = 8080

  def apply(
    networkAlias: String = defaultNetworkAlias,
    dockerTag:    String = defaultTag,
    port:         Int    = defaultPort,
  ): S3Container =
    new S3Container(dockerImage, dockerTag, networkAlias, port)
}
