/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.pulsar.sink

import io.lenses.streamreactor.common.errors.ErrorPolicyEnum
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifest
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.pulsar.config.PulsarConfigConstants
import io.lenses.streamreactor.connect.pulsar.config.PulsarSinkConfig
import io.lenses.streamreactor.connect.pulsar.config.PulsarSinkSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import java.util.UUID
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
  * Created by andrew@datamountaineer.com on 27/08/2017.
  * stream-reactor
  */
class PulsarSinkTask extends SinkTask with StrictLogging {
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean              = false
  private var writer:         Option[PulsarWriter] = None
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)
  private var name     = ""
  private var settings: Option[PulsarSinkSettings] = None

  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, "/pulsar-sink-ascii.txt")

    val conf = if (context.configs().isEmpty) props else context.configs()

    PulsarSinkConfig.config.parse(conf)
    val sinkConfig = new PulsarSinkConfig(conf)
    enableProgress = sinkConfig.getBoolean(PulsarConfigConstants.PROGRESS_COUNTER_ENABLED)
    settings       = Some(PulsarSinkSettings(sinkConfig))

    //if error policy is retry set retry interval
    if (settings.get.errorPolicy.equals(ErrorPolicyEnum.RETRY)) {
      context.timeout(sinkConfig.getInt(PulsarConfigConstants.ERROR_RETRY_INTERVAL).toLong)
    }

    name   = conf.getOrDefault("name", s"kafka-connect-pulsar-sink-${UUID.randomUUID().toString}")
    writer = Some(PulsarWriter(name, settings.get))
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val recs = records.asScala
    writer.foreach(w => w.write(recs.toSet))

    if (enableProgress) {
      progressCounter.update(recs.toVector)
    }
  }

  /**
    * Clean up writer
    */
  override def stop(): Unit = {
    logger.info("Stopping Pulsar sink.")
    writer.foreach(w => w.close())
    progressCounter.empty()
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.flush())
  }

  override def version: String = manifest.version()
}
