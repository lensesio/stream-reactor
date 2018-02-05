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

package com.datamountaineer.streamreactor.connect.blockchain.source

import java.util

import com.datamountaineer.streamreactor.connect.blockchain.config.BlockchainConfig
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConversions._

class BlockchainSourceConnector extends SourceConnector with StrictLogging {
  private var configProps: Option[util.Map[String, String]] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Defines the source class to use
    *
    * @return
    */
  override def taskClass(): Class[_ <: Task] = classOf[BlockchainSourceTask]

  /**
    * Set the configuration for each work and determine the split.
    *
    * @param maxTasks The max number of task workers be can spawn.
    * @return a List of configuration properties per worker.
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = Seq(configProps.get)

  /**
    * Start the sink and set to configuration.
    *
    * @param props A map of properties for the connector and worker.
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Blockchain source task with ${props.toString}.")
    configProps = Some(props)
  }

  override def stop(): Unit = {}

  /**
    * Gets the version of this sink.
    *
    * @return
    */
  override def version(): String = manifest.version()

  override def config(): ConfigDef = BlockchainConfig.config
}

