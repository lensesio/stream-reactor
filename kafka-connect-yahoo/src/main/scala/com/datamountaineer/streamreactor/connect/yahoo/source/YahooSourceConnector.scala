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

package com.datamountaineer.streamreactor.connect.yahoo.source

import java.util
import java.util.logging.Logger

import com.datamountaineer.streamreactor.connect.yahoo.config.{DistributeConfigurationFn, YahooSourceConfig}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.source.SourceConnector

import scala.collection.JavaConverters._

/**
  * <h1>YahooSourceConnector</h1>
  * Kafka connect Yahoo Source connector
  *
  * Sets up YahooSourceTask and configurations for the tasks.
  */
class YahooSourceConnector extends SourceConnector with YahooSourceConfig {
  val logger: Logger = Logger.getLogger(getClass.getName)
  private var configProps: Option[util.Map[String, String]] = None

  /**
    * Defines the source class to use
    *
    * @return
    */
  override def taskClass(): Class[_ <: Task] = classOf[YahooSourceTask]

  /**
    * Set the configuration for each work and determine the split.
    *
    * @param maxTasks The max number of task workers be can spawn.
    * @return a List of configuration properties per worker.
    **/
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    DistributeConfigurationFn(maxTasks, configProps.get.asScala.toMap).map(_.asJava).asJava
  }

  /**
    * Start the sink and set to configuration.
    *
    * @param props A map of properties for the connector and worker.
    **/
  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Yahoo source task with ${props.toString}.")
    configProps = Some(props)
  }

  override def stop(): Unit = {}

  /**
    * Gets the version of this sink.
    *
    * @return
    */
  override def version(): String = getClass.getPackage.getImplementationVersion

  override def config(): ConfigDef = configDef
}
