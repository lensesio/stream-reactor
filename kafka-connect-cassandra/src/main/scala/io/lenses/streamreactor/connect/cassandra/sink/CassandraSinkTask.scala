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
package io.lenses.streamreactor.connect.cassandra.sink

import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.common.utils.ProgressCounter

import java.util
import io.lenses.streamreactor.connect.cassandra.config.CassandraConfigSink
import io.lenses.streamreactor.connect.cassandra.config.CassandraSettings
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
  * <h1>CassandraSinkTask</h1>
  *
  * Kafka Connect Cassandra sink task. Called by
  * framework to put records to the target sink
  */
class CassandraSinkTask extends SinkTask with StrictLogging with JarManifestProvided {
  private var writer: Option[CassandraJsonWriter] = None
  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  logger.info("Task initialising")

  /**
    * Parse the configurations and setup the writer
    */
  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, "/cass-sink-ascii.txt")

    val config = if (context.configs().isEmpty) props else context.configs()

    val taskConfig = Try(new CassandraConfigSink(config.asScala.toMap)) match {
      case Failure(f) => throw new ConnectException("Couldn't start CassandraSink due to configuration error.", f)
      case Success(s) => s
    }

    val sinkSettings = CassandraSettings.configureSink(taskConfig)
    enableProgress = sinkSettings.enableProgress

    writer = Some(CassandraWriter(connectorConfig = taskConfig, context = context))
  }

  /**
    * Pass the SinkRecords to the writer for Writing
    */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writer.nonEmpty, "Writer is not set!")
    val seq = records.asScala.toVector
    writer.foreach(w => w.write(seq))
    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  /**
    * Clean up Cassandra connections
    */
  override def stop(): Unit = {
    logger.info("Stopping Cassandra sink.")
    writer.foreach(w => w.close())
    if (enableProgress) {
      progressCounter.empty()
    }
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
}
