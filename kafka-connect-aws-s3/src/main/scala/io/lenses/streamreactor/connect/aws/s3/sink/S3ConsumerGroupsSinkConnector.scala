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
package io.lenses.streamreactor.connect.aws.s3.sink

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.connect.aws.s3.config.S3ConfigSettings
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3ConsumerGroupsSinkConfigDef
import io.lenses.streamreactor.connect.cloud.common.config.TaskDistributor
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector

import java.util

/**
  * A connector which stores the latest Kafka consumer group offset from "__consumer_offsets" topic in S3.
  */
class S3ConsumerGroupsSinkConnector extends SinkConnector with LazyLogging with JarManifestProvided {

  private val props: util.Map[String, String] = new util.HashMap[String, String]()

  override def taskClass(): Class[_ <: Task] = classOf[S3ConsumerGroupsSinkTask]

  override def config(): ConfigDef = S3ConsumerGroupsSinkConfigDef.config

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(s"Creating S3 consumer groups sink connector")
    this.props.putAll(props)
  }

  override def stop(): Unit = ()

  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] =
    new TaskDistributor(S3ConfigSettings.CONNECTOR_PREFIX).distributeTasks(props, maxTasks)
}
