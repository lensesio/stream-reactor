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
package io.lenses.streamreactor.connect.mqtt.source

import io.lenses.streamreactor.common.util.JarManifest

import java.util
import java.util.Collections
import io.lenses.streamreactor.connect.mqtt.config.MqttSourceConfig
import io.lenses.streamreactor.connect.mqtt.config.MqttSourceSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

import scala.jdk.CollectionConverters.MapHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

class MqttSourceConnector extends SourceConnector with StrictLogging {
  private val configDef = MqttSourceConfig.config
  private var configProps: util.Map[String, String] = _
  private val manifest = new JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * States which SourceTask class to use
    */
  override def taskClass(): Class[_ <: Task] = classOf[MqttSourceTask]

  /**
    * Set the configuration for each work and determine the split
    *
    * @param maxTasks The max number of task workers be can spawn
    * @return a List of configuration properties per worker
    */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {

    val settings = MqttSourceSettings(MqttSourceConfig(configProps.asScala.toMap))
    val kcql     = settings.kcql
    if (maxTasks == 1 || kcql.length == 1) {
      Collections.singletonList(configProps)
    } else {
      val groups = kcql.length / maxTasks + kcql.length % maxTasks

      // If the option is enabled, copy every KCQL instruction with a shared subscription to every tasks, otherwise
      // the shared subscriptions are distributed as every other instructions.
      val (replicated, distributed) =
        if (settings.replicateShared) kcql.partition(shouldReplicate) else (Array[String](), kcql)
      val configs = Array.fill(maxTasks)(replicated)
        .zipAll(distributed.grouped(groups).toList, Array[String](), Array[String]())
        .map(z => z._2 ++ z._1)
        .filter(_.nonEmpty)
        .zipWithIndex
        .map {
          case (p, index) =>
            val map = settings.copy(kcql = p, clientId = settings.clientId + "-" + index).asMap()
            configProps.asScala
              .filterNot { case (k, _) => map.containsKey(k) }
              .foreach { case (k, v) => map.put(k, v) }
            map
        }
      configs.toList.asJava
    }
  }

  /**
    * Indicate if an instruction should be replicated to every tasks
    * @param instruction A KCQL instruction
    * @return true if the instruction should be replicated and false if
    *         the instruction should be given to only one task
    */
  def shouldReplicate(instruction: String): Boolean =
    instruction.contains("$share/")

  /**
    * Start the source and set to configuration
    *
    * @param props A map of properties for the connector and worker
    */
  override def start(props: util.Map[String, String]): Unit =
    configProps = props

  override def stop(): Unit = {}

  override def config(): ConfigDef = configDef

  override def version(): String = manifest.getVersion()
}
