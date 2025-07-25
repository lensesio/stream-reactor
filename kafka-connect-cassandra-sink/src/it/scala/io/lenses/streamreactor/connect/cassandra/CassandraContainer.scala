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
package io.lenses.streamreactor.connect.cassandra

import com.datastax.oss.driver.api.core.Version
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster
import com.datastax.oss.dsbulk.tests.driver.VersionUtils
import io.lenses.streamreactor.connect.cassandra.CassandraContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.cassandra.CassandraContainer.defaultTag
import org.testcontainers.containers.wait.CassandraQueryWaitStrategy
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.{ CassandraContainer => JavaCassandraContainer }
import org.testcontainers.utility.DockerImageName

import java.net.InetSocketAddress

abstract class SingleContainer[T <: GenericContainer[_]] {

  def container: T

  def start(): Unit = container.start()

  def stop(): Unit = container.stop()

  def withNetwork(network: Network): this.type = {
    container.withNetwork(network)
    this
  }

  def withExposedPorts(ports: Integer*): this.type = {
    container.withExposedPorts(ports: _*)
    this
  }
}

class CassandraContainer(
  dockerImage:           DockerImageName,
  dockerTag:             String         = defaultTag,
  val networkAlias:      String         = defaultNetworkAlias,
  configurationOverride: Option[String] = None,
  initScript:            Option[String] = None,
  enableJmxReporting:    Boolean        = false,
) extends SingleContainer[JavaCassandraContainer[_]] {

  override val container: JavaCassandraContainer[_] = new JavaCassandraContainer(dockerImage.withTag(dockerTag))
  container.waitingFor(new CassandraQueryWaitStrategy())
  container.withNetworkAliases(networkAlias)

  if (configurationOverride.isDefined) container.withConfigurationOverride(configurationOverride.get)
  if (initScript.isDefined) container.withInitScript(initScript.get)
  if (enableJmxReporting) container.withJmxReporting(enableJmxReporting)

  def getCqlSession: com.datastax.oss.driver.api.core.CqlSession =
    com.datastax.oss.driver.api.core.CqlSession.builder()
      .addContactPoint(container.getContactPoint)
      .withLocalDatacenter(container.getLocalDatacenter)
      .build()

  def getContactPoint: InetSocketAddress = container.getContactPoint

  def username: String = container.getUsername

  def password: String = container.getPassword

  def version: String = container.getDockerImageName.split(":").lastOption.getOrElse(defaultTag)

  def getClusterType: CCMCluster.Type =
    if (container.getDockerImageName.contains("dse")) CCMCluster.Type.DSE
    else CCMCluster.Type.OSS
  val hasDateRange: Boolean =
    (getClusterType eq CCMCluster.Type.DSE) && VersionUtils.isWithinRange(Version.parse("5.1.0"),
                                                                          null,
                                                                          Version.parse(version),
    )

}

object CassandraContainer {
  private val dockerImage         = DockerImageName.parse("cassandra")
  private val defaultTag          = "4.1.0"
  private val defaultNetworkAlias = "cassandra"

  def apply(
    networkAlias:          String         = defaultNetworkAlias,
    dockerTag:             String         = defaultTag,
    configurationOverride: Option[String] = None,
    initScript:            Option[String] = None,
    enableJmxReporting:    Boolean        = false,
  ): CassandraContainer =
    new CassandraContainer(dockerImage, dockerTag, networkAlias, configurationOverride, initScript, enableJmxReporting)
}
