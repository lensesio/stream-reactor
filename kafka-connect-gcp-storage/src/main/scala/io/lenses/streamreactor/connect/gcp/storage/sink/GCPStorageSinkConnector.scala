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
package io.lenses.streamreactor.connect.gcp.storage.sink

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.cloud.common.config.TaskDistributor
import io.lenses.streamreactor.connect.gcp.storage.config.GCPConfigSettings
import io.lenses.streamreactor.connect.gcp.storage.sink.config.GCPStorageSinkConfigDef
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import java.util

class GCPStorageSinkConnector extends SinkConnector with LazyLogging with JarManifestProvided {

  private val props: util.Map[String, String] = new util.HashMap[String, String]()

  override def taskClass(): Class[_ <: Task] = classOf[GCPStorageSinkTask]

  override def config(): ConfigDef = GCPStorageSinkConfigDef.config

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Creating GCP Storage sink connector")
    this.props.putAll(props)
  }

  override def stop(): Unit = ()

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    logger.info(s"Creating $maxTasks tasks config")
    new TaskDistributor(GCPConfigSettings.CONNECTOR_PREFIX).distributeTasks(props, maxTasks)
  }
}
