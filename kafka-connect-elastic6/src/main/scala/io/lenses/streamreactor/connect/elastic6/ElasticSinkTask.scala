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
package io.lenses.streamreactor.connect.elastic6

import io.lenses.streamreactor.common.errors.RetryErrorPolicy
import io.lenses.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.util.JarManifest
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfigConstants
import io.lenses.streamreactor.connect.elastic6.config.ElasticSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

class ElasticSinkTask extends SinkTask with StrictLogging {
  private var writer: Option[ElasticJsonWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest =  new JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Parse the configurations and setup the writer
    */
  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, "/elastic-ascii.txt")

    val conf = if (context.configs().isEmpty) props else context.configs()

    ElasticConfig.config.parse(conf)
    val sinkConfig = ElasticConfig(conf.asScala.toMap)
    enableProgress = sinkConfig.getBoolean(ElasticConfigConstants.PROGRESS_COUNTER_ENABLED)

    //if error policy is retry set retry interval
    val settings = ElasticSettings(sinkConfig)
    settings.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(sinkConfig.getInt(ElasticConfigConstants.ERROR_RETRY_INTERVAL).toLong)
      case _                  =>
    }

    writer = Some(ElasticWriter(sinkConfig))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.asScala.toVector
    writer.foreach(_.write(seq))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  /**
    * Clean up writer
    */
  override def stop(): Unit = {
    logger.info("Stopping Elastic sink.")
    writer.foreach(w => w.close())
    progressCounter.empty()
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit =
    logger.info("Flushing Elastic Sink")

  override def version: String = manifest.getVersion()
}
