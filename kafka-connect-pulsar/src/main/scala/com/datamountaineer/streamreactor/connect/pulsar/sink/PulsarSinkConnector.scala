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

package com.datamountaineer.streamreactor.connect.pulsar.sink

import java.util

import com.datamountaineer.streamreactor.connect.config.Helpers
import com.datamountaineer.streamreactor.connect.pulsar.config.{PulsarConfigConstants, PulsarSinkConfig}
import com.datamountaineer.streamreactor.connect.utils.JarManifest
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

class PulsarSinkConnector extends SinkConnector with StrictLogging {
  private val configDef = PulsarSinkConfig.config
  private var configProps: Option[util.Map[String, String]] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Starting Pulsar sink connector.")
    Helpers.checkInputTopics(PulsarConfigConstants.KCQL_CONFIG, props.asScala.toMap)
    configProps = Some(props)
  }

  override def taskClass(): Class[_ <: Task] = classOf[PulsarSinkTask]

  override def version(): String = manifest.version()

  override def stop(): Unit = {}

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Setting task configurations for $maxTasks workers.")
    (1 to maxTasks).map(_ => configProps.get).toList
  }

  override def config(): ConfigDef = configDef
}
