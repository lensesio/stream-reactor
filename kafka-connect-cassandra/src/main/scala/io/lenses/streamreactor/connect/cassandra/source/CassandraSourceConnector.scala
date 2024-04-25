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
package io.lenses.streamreactor.connect.cassandra.source

import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.cassandra.config.{CassandraConfigConstants, CassandraConfigSource}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils

import java.util
import scala.jdk.CollectionConverters.{ListHasAsScala, MapHasAsJava, MapHasAsScala, SeqHasAsJava}

/**
  * <h1>CassandraSourceConnector</h1>
  * Kafka connect Cassandra Source connector
  *
  * Sets up CassandraSourceTask and configurations for the tasks.
  */
class CassandraSourceConnector extends SourceConnector with StrictLogging with JarManifestProvided {

  private var configProps: Option[util.Map[String, String]] = None
  private val configDef = CassandraConfigSource.sourceConfig

  /**
    * Defines the sink class to use
    *
    * @return
    */
  override def taskClass(): Class[_ <: Task] = classOf[CassandraSourceTask]

  /**
    * Set the configuration for each work and determine the split.
    *
    * @param maxTasks The max number of task workers be can spawn.
    * @return a List of configuration properties per worker.
    */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    val raw = configProps.get.get(CassandraConfigConstants.KCQL).split(";")

    val tables = raw.map(r => Kcql.parse(r).getSource).toList

    val numGroups = Math.min(tables.size, maxTasks)

    logger.info(s"Setting task configurations for $numGroups workers.")
    val groups = ConnectorUtils.groupPartitions(tables.asJava, maxTasks).asScala

    //setup the config for each task and set assigned tables
    groups
      .withFilter(g => g.asScala.nonEmpty)
      .map { g =>
        val taskConfigs = new java.util.HashMap[String, String](configProps.get)
        taskConfigs.put(CassandraConfigConstants.ASSIGNED_TABLES, g.asScala.mkString(","))
        taskConfigs.asScala.asJava
      }
  }.asJava

  /**
    * Start the sink and set to configuration.
    *
    * @param props A map of properties for the connector and worker.
    */
  override def start(props: util.Map[String, String]): Unit =
    configProps = Some(props)

  override def stop(): Unit = {}

  override def config(): ConfigDef = configDef
}
