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
package com.datamountaineer.streamreactor.connect.jms.sink

import com.datamountaineer.streamreactor.common.errors.RetryErrorPolicy
import com.datamountaineer.streamreactor.common.utils.AsciiArtPrinter.printAsciiHeader
import com.datamountaineer.streamreactor.common.utils.JarManifest
import com.datamountaineer.streamreactor.common.utils.ProgressCounter
import com.datamountaineer.streamreactor.connect.jms.config.JMSConfig
import com.datamountaineer.streamreactor.connect.jms.config.JMSConfigConstants
import com.datamountaineer.streamreactor.connect.jms.config.JMSSettings
import com.datamountaineer.streamreactor.connect.jms.sink.writers.JMSWriter
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.IterableHasAsScala

/**
  * <h1>JMSSinkTask</h1>
  *
  * Kafka Connect JMS sink task. Called by framework to put records to the target sink
  */
class JMSSinkTask extends SinkTask with StrictLogging {

  var writer: Option[JMSWriter] = None
  val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private val manifest = JarManifest(getClass.getProtectionDomain.getCodeSource.getLocation)

  /**
    * Parse the configurations and setup the writer
    */
  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, "/jms-sink-ascii.txt")

    val conf = if (context.configs().isEmpty) props else context.configs()
    JMSConfig.config.parse(conf)
    val sinkConfig = new JMSConfig(conf)
    val settings   = JMSSettings(sinkConfig, sink = true)
    enableProgress = sinkConfig.getBoolean(JMSConfigConstants.PROGRESS_COUNTER_ENABLED)

    //if error policy is retry set retry interval
    settings.errorPolicy match {
      case RetryErrorPolicy() => context.timeout(sinkConfig.getInt(JMSConfigConstants.ERROR_RETRY_INTERVAL).toLong)
      case _                  =>
    }

    writer = Some(JMSWriter(settings))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    //filter out records which have null value. it will fail otherwise projecting the payload in order to be sent
    //to the JMS system
    val seq = records.asScala.filter(_.value() != null).toVector
    writer.foreach(w => w.write(seq))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  /**
    * Clean up connections
    */
  override def stop(): Unit = {
    logger.info("Stopping JMS sink.")
    writer.foreach(w => w.close())
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    //TODO
    //have the writer expose a is busy; can expose an await using a countdownlatch internally
  }
  override def version: String = manifest.version()
}
