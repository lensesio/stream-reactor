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

package com.datamountaineer.streamreactor.connect.bloomberg

import java.util

import com.datamountaineer.streamreactor.connect.bloomberg.config.BloombergSourceConfig
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._


/**
  * <h1>BloombergSourceConnector</h1>
  * Kafka connect Bloomberg Source connector
  *
  * Sets up BloombergSourceTask and configurations for the tasks.
  */
class BloombergSourceConnector extends SourceConnector with StrictLogging {
  private var bloombergSettings: Option[BloombergSettings] = None
  private val configDef = BloombergSourceConfig.config
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Defines the sink class to use
    *
    * @return
    */
  override def taskClass(): Class[_ <: Task] = classOf[BloombergSourceTask]

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    //ConnectorUtils.groupPartitions()
    logger.info(s"Setting task configurations for $maxTasks workers.")

    val partitions = Math.min(bloombergSettings.get.subscriptions.size, maxTasks)
    bloombergSettings.get.subscriptions.grouped(partitions)
      .map(p => bloombergSettings.get.copy(subscriptions = p).asMap())
      .toList.asJava
  }

  override def stop(): Unit = {
  }

  /**
    * Start the sink and set to configuration
    *
    * @param props A map of properties for the connector and worker
    **/
  override def start(props: util.Map[String, String]): Unit = {
    try {
      bloombergSettings = Some(BloombergSettings(new BloombergSourceConfig(props)))
    }
    catch {
      case t: Throwable => throw new ConnectException("Cannot start the Bloomberg connector due to invalid configuration.", t)
    }

  }

  /**
    * Gets the version of this sink
    *
    * @return
    */
  override def version(): String = manifest.version()

  override def config(): ConfigDef = configDef
}
