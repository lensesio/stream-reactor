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

package com.datamountaineer.streamreactor.connect.mqtt.source

import java.util
import java.util.Collections

import com.datamountaineer.streamreactor.connect.mqtt.config.{MqttSourceConfig, MqttSourceSettings}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._

class MqttSourceConnector extends SourceConnector with StrictLogging {
  private val configDef = MqttSourceConfig.config
  private var configProps: util.Map[String, String] = _
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * States which SinkTask class to use
    **/
  override def taskClass(): Class[_ <: Task] = classOf[MqttSourceTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {

    val settings = MqttSourceSettings(MqttSourceConfig(configProps))
    val kcql = settings.kcql
    if (maxTasks == 1 || kcql.length == 1) {
      Collections.singletonList(configProps)
    } else {
      val groups = kcql.length / maxTasks + kcql.length % maxTasks

      settings.kcql.grouped(groups)
        .zipWithIndex
        .map { case (p, index) =>
          val map = settings.copy(kcql = p, clientId = settings.clientId + "-" + index).asMap()
          import scala.collection.JavaConversions._
          configProps
            .filterNot { case (k, _) => map.containsKey(k) }
            .foreach { case (k, v) => map.put(k, v) }
          map
        }
        .toList.asJava
    }
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
