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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.OffsetJavaScalaConverter._

import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfig
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfigConstants
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer.CosmosDbWriterFactory
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer.CosmosDbWriterManager
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * <h1>DocumentSinkTask</h1>
 *
 * Kafka Connect Azure Document DB sink task. Called by
 * framework to put records to the target sink
 */
class CosmosDbSinkTask extends SinkTask with StrictLogging with JarManifestProvided {
  private var writerManager: Option[CosmosDbWriterManager] = None

  private val progressCounter = new ProgressCounter
  private var enableProgress: Boolean = false
  private var enableBulk:     Boolean = false

  /**
   * Parse the configurations and setup the writerManager
   */
  override def start(props: util.Map[String, String]): Unit = {
    val config = if (context.configs().isEmpty) props else context.configs()

    val taskConfig = Try(CosmosDbConfig(config.asScala.toMap)) match {
      case Failure(f) =>
        throw new ConnectException("Couldn't start Azure Document DB Sink due to configuration error.", f)
      case Success(s) => s
    }

    implicit val settings: CosmosDbSinkSettings = CosmosDbSinkSettings(taskConfig)
    enableBulk = settings.bulkEnabled

    printAsciiHeader(manifest, "/cosmosdb-sink-ascii.txt")

    writerManager  = Some(CosmosDbWriterFactory(taskConfig, context, settings))
    enableProgress = taskConfig.getBoolean(CosmosDbConfigConstants.PROGRESS_COUNTER_ENABLED)
  }

  /**
   * Pass the SinkRecords to the Azure Document DB writerManager for storing them
   */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writerManager.nonEmpty, "Writer is not set!")
    val seq = records.asScala.toVector
    writerManager.foreach(w => w.write(seq))

    if (enableProgress) {
      progressCounter.update(seq)
    }
  }

  override def stop(): Unit = {
    logger.info("Stopping Azure Document DB sink.")
    writerManager.foreach(w => w.close())
    progressCounter.empty()
  }

  override def flush(map: util.Map[KafkaTopicPartition, OffsetAndMetadata]): Unit = {}

  override def version(): String = manifest.getVersion

  override def preCommit(
    currentOffsets: util.Map[KafkaTopicPartition, OffsetAndMetadata],
  ): util.Map[KafkaTopicPartition, OffsetAndMetadata] =
    if (enableBulk) {
      writerManager.fold {
        logger.debug("Writer manager not set in preCommit, continuing...")
        super.preCommit(currentOffsets)
      }(wm => offsetMapToJava(wm.preCommit(offsetMapToScala(currentOffsets))))
    } else {
      super.preCommit(currentOffsets)
    }

}
