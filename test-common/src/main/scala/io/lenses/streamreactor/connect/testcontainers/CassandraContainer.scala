package io.lenses.streamreactor.connect.testcontainers

import com.datastax.driver.core.Cluster
import io.lenses.streamreactor.connect.testcontainers.CassandraContainer.defaultNetworkAlias
import io.lenses.streamreactor.connect.testcontainers.CassandraContainer.defaultTag
import org.testcontainers.containers.{ CassandraContainer => JavaCassandraContainer }
import org.testcontainers.utility.DockerImageName

import scala.annotation.nowarn

class CassandraContainer(
  dockerImage:           DockerImageName,
  dockerTag:             String         = defaultTag,
  val networkAlias:      String         = defaultNetworkAlias,
  configurationOverride: Option[String] = None,
  initScript:            Option[String] = None,
  enableJmxReporting:    Boolean        = false,
) extends SingleContainer[JavaCassandraContainer[_]] {

  override val container: JavaCassandraContainer[_] = new JavaCassandraContainer(dockerImage.withTag(dockerTag))
  container.withNetworkAliases(networkAlias)

  if (configurationOverride.isDefined) container.withConfigurationOverride(configurationOverride.get)
  if (initScript.isDefined) container.withInitScript(initScript.get)
  if (enableJmxReporting) container.withJmxReporting(enableJmxReporting)

  @nowarn
  def cluster: Cluster = container.getCluster

  def username: String = container.getUsername

  def password: String = container.getPassword
}

object CassandraContainer {
  private val dockerImage         = DockerImageName.parse("cassandra")
  private val defaultTag          = "3.11.2"
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
