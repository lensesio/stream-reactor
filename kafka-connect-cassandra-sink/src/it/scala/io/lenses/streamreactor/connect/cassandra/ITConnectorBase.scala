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

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.metadata.EndPoint
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster
import com.datastax.oss.dsbulk.tests.driver.factory.SessionFactory
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.cassandra.cluster.ClusterConfig
import io.lenses.streamreactor.connect.cassandra.cluster.ClusterFactory
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.jupiter.api.extension.ExtensionContext
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

import java.net.InetSocketAddress
import java.util.{ Map => JavaMap }
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

/**
 * Base class for Cassandra integration tests.
 * Provides common functionality for setting up and managing Cassandra sink connector tests.
 */
abstract class ITConnectorBase(
  cluster: CCMCluster,
  session: CqlSession,
) extends AnyFunSuite
    with MockitoSugar
    with StrictLogging {

  protected val TEST_NAMESPACE: ExtensionContext.Namespace =
    ExtensionContext.Namespace.create("com.datastax.oss.dsbulk.tests")

  /**
   * Creates a CqlSession using a SessionFactory and ExtensionContext (Scala translation of Java version)
   */
  protected def createSession(sessionFactory: SessionFactory): CqlSession = {
    val contactPoints = getContactPoints()
    try {
      val session = sessionFactory.createSessionBuilder().addContactEndPoints(contactPoints.asJava).build()
      sessionFactory.configureSession(session)
      session
    } catch {
      case e: RuntimeException =>
        logger.error("Could not create session", e)
        throw e
    }
  }

  protected def getLocalDatacenter(): String =
    try cluster.getDC(1)
    catch {
      case _: Exception =>
        logger.warn("Could not determine local DC name, using default names instead")
        if (cluster.isMultiDC) "dc1"
        else "Cassandra"
    }

  protected def createCluster(config: ClusterConfig): CCMCluster = {
    val factory = new ClusterFactory(config)
    factory.createCCMClusterBuilder().build()
  }

  val keyspaceName: String = session.getKeyspace.orElse(CqlIdentifier.fromInternal("unknown")).asInternal

  // Connector and task instances
  protected val conn = new CassandraSinkConnector()
  protected val task = new CassandraSinkTask()

  // Initialize task context
  private val taskContext = mock[SinkTaskContext]
  task.initialize(taskContext)

  /**
   * Stops the connector and task
   */
  protected def stopConnector(): Unit = {
    task.stop()
    conn.stop()
  }

  /**
   * Runs the task with the given records
   */
  protected def runTaskWithRecords(records: SinkRecord*): Unit = {
    initConnectorAndTask()
    task.put(records.toList.asJava)
  }

  /**
   * Initializes the connector and task
   */
  protected def initConnectorAndTask(): Unit = {
    val taskProps = conn.taskConfigs(1)
    task.start(taskProps.get(0))
  }

  /**
   * Creates connector properties with the given parameters
   */
  protected def makeConnectorProperties(
    sqlProjections: String,
    tableName:      String,
    extras:         Map[String, String],
    topicName:      String,
  ): JavaMap[String, String] = {
    val props = Map.newBuilder[String, String]

    val contactPoints =
      getContactPoints().map(addr => addr.resolve().asInstanceOf[InetSocketAddress].getHostString()).mkString(",")

    props += "name"                                  -> "myinstance"
    props += CassandraConfigConstants.CONTACT_POINTS -> contactPoints

    props += CassandraConfigConstants.PORT                    -> cluster.getBinaryPort.toString
    props += CassandraConfigConstants.LOAD_BALANCING_LOCAL_DC -> cluster.getDC(1)
    props += CassandraConfigConstants.KCQL                    -> s"INSERT INTO $keyspaceName.$tableName SELECT ${sqlProjections} FROM $topicName"
    props += "topics"                                         -> topicName

    if (extras.nonEmpty) {
      props ++= extras
    }

    props.result().asJava
  }

  /**
   * Creates connector properties with default mapping
   */
  protected def makeConnectorProperties(extras: Map[String, String]): JavaMap[String, String] =
    makeConnectorProperties("value as bigintcol", extras)

  /**
   * Creates connector properties with custom mapping
   */
  protected def makeConnectorProperties(
    sqlProjections: String,
    extras:         Map[String, String],
  ): JavaMap[String, String] =
    makeConnectorProperties(sqlProjections, "types", extras)

  /**
   * Creates connector properties with custom mapping and table name
   */
  protected def makeConnectorProperties(
    sqlProjections: String,
    tableName:      String,
    extras:         Map[String, String],
  ): JavaMap[String, String] =
    makeConnectorProperties(sqlProjections, tableName, extras, "mytopic")

  /**
   * Creates connector properties with default mapping and no extras
   */
  protected def makeConnectorProperties(mappingString: String): JavaMap[String, String] =
    makeConnectorProperties(mappingString, Map.empty)

  /**
   * Gets the contact points
   */
  def getContactPoints(): List[EndPoint] = cluster.getInitialContactPoints().asScala.toList

}
