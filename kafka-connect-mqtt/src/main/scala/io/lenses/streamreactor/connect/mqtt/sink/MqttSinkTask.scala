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
package io.lenses.streamreactor.connect.mqtt.sink

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.mqtt.config.MqttConfigConstants
import io.lenses.streamreactor.connect.mqtt.config.MqttSinkConfig
import io.lenses.streamreactor.connect.mqtt.config.MqttSinkSettings
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

/**
 * Created by andrew@datamountaineer.com on 27/08/2017.
 * stream-reactor
 */
class MqttSinkTask extends SinkTask with StrictLogging with JarManifestProvided {
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean            = false
  private var writer:         Option[MqttWriter] = None

  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, "/mqtt-sink-ascii.txt")

    val conf = if (context.configs().isEmpty) props else context.configs()

    MqttSinkConfig.config.parse(conf)
    val sinkConfig = new MqttSinkConfig(conf.asScala.toMap)
    enableProgress = sinkConfig.getBoolean(MqttConfigConstants.PROGRESS_COUNTER_ENABLED)
    val settings = MqttSinkSettings(sinkConfig)

    //if error policy is retry set retry interval
    settings.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(sinkConfig.getInt(MqttConfigConstants.ERROR_RETRY_INTERVAL).toLong)
      case _                  =>
    }

    writer = Some(MqttWriter(settings))
  }

  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val recs = records.asScala
    writer.foreach { w =>
      w.write(recs) match {
        case Left(error)  => throw error
        case Right(value) => // success case, no action needed
      }
    }

    if (enableProgress) {
      progressCounter.update(recs.toVector)
    }
  }

  /**
   * Clean up writer
   */
  override def stop(): Unit = {
    logger.info("Stopping Mqtt sink.")
    writer.foreach(w => w.close())
    progressCounter.empty()
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    writer.foreach(w => w.flush())
  }
}
