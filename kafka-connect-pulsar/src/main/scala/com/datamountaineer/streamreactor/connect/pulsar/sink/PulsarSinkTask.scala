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
import java.util.UUID

import com.datamountaineer.streamreactor.connect.errors.ErrorPolicyEnum
import com.datamountaineer.streamreactor.connect.pulsar.config.{PulsarConfigConstants, PulsarSinkConfig, PulsarSinkSettings}
import com.datamountaineer.streamreactor.connect.utils.{JarManifest, ProgressCounter}
import com.typesafe.scalalogging.slf4j.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}

import scala.collection.JavaConversions._


/**
  * Created by andrew@datamountaineer.com on 27/08/2017. 
  * stream-reactor
  */
class PulsarSinkTask extends SinkTask with StrictLogging {
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private var writer: Option[PulsarWriter] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  private var name = ""
  private var settings : Option[PulsarSinkSettings] = None

  override def start(props: util.Map[String, String]): Unit = {
    logger.info(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/pulsar-sink-ascii.txt")).mkString + s" v $version")
    logger.info(manifest.printManifest())

    PulsarSinkConfig.config.parse(props)
    val sinkConfig = new PulsarSinkConfig(props)
    enableProgress = sinkConfig.getBoolean(PulsarConfigConstants.PROGRESS_COUNTER_ENABLED)
    settings = Some(PulsarSinkSettings(sinkConfig))

    //if error policy is retry set retry interval
    if (settings.get.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(PulsarConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    name = props.getOrDefault("name", s"kafka-connect-pulsar-sink-${UUID.randomUUID().toString}")
    writer = Some(PulsarWriter(name, settings.get))
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.toVector
    writer.foreach(w => w.write(records.toSet))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  /**
    * Clean up writer
    **/
  override def stop(): Unit = {
    logger.info("Stopping Pulsar sink.")
    writer.foreach(w => w.close)
    progressCounter.empty
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.flush)
  }

  override def version: String = manifest.version()
}
