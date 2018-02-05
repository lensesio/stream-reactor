/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.rethink.source

import java.util

import com.datamountaineer.streamreactor.connect.rethink.config.{ReThinkConfigConstants, ReThinkSourceConfig}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector
import org.apache.kafka.connect.util.ConnectorUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Created by andrew@datamountaineer.com on 22/09/16. 
  * stream-reactor
  */
class ReThinkSourceConnector extends SourceConnector with StrictLogging {
  private var configProps: util.Map[String, String] = _
  private val configDef = ReThinkSourceConfig.config
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * States which SinkTask class to use
    **/
  override def taskClass(): Class[_ <: Task] = classOf[ReThinkSourceTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {

    val raw = configProps.asScala.get(ReThinkConfigConstants.KCQL)
    require(raw != null && raw.isDefined, s"No ${ReThinkConfigConstants.KCQL} provided!")

    //sql1, sql2
    val kcqls = raw.get.split(";")
    val groups = ConnectorUtils.groupPartitions(kcqls.toList, maxTasks).asScala

    //split up the kcql statement based on the number of tasks.
    groups
      .filterNot(g => g.isEmpty)
      .map(g => {
        val taskConfigs = new java.util.HashMap[String, String]
        taskConfigs.putAll(configProps)
        taskConfigs.put(ReThinkConfigConstants.KCQL, g.mkString(";")) //overwrite
        taskConfigs.toMap.asJava
      })
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    **/
  override def start(props: util.Map[String, String]): Unit = {
    configProps = props
  }

  override def stop(): Unit = {}

  override def config(): ConfigDef = configDef

  override def version(): String = manifest.version()
}
