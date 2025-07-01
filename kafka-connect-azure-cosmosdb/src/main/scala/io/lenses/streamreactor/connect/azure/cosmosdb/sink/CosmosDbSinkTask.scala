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
import io.lenses.streamreactor.common.utils.EitherOps._
import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfig
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbConfigConstants
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.azure.cosmosdb.CosmosClientProvider
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer.CosmosDbWriterManagerFactory
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer.CosmosDbWriterManager
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{ TopicPartition => KafkaTopicPartition }
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

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
    logger.info("Starting Azure CosmosDb sink task initialization. Parsing task configuration.")
    val config = if (context.configs().isEmpty) props else context.configs()

    val setup = for {
      taskConfig   <- CosmosDbConfig(config.asScala.toMap)
      _             = logger.info("Task configuration parsed successfully. Creating CosmosDbSinkSettings.")
      settings     <- CosmosDbSinkSettings(taskConfig)
      _             = logger.info(s"CosmosDbSinkSettings created: $settings. Creating Cosmos DB client.")
      cosmosClient <- CosmosClientProvider.get(settings)
      _             = logger.info("Cosmos DB client created. Creating writer manager.")
      writerMgr <- CosmosDbWriterManagerFactory(
        settings.kcql.map(k => k.getSource -> k).toMap,
        settings,
        context,
        cosmosClient,
      ).left.map { err =>
        logger.error("Failed to create CosmosDbWriterManager", err)
        err
      }
      _ = logger.info("Writer manager created.")
    } yield (settings, writerMgr)

    val (settings, writerMgr) = setup.unpackOrThrow
    enableBulk = settings.bulkEnabled

    printAsciiHeader(manifest, "/cosmosdb-sink-ascii.txt")

    writerManager = Some(writerMgr)
    enableProgress =
      config.asScala.get(CosmosDbConfigConstants.PROGRESS_COUNTER_ENABLED).map(_.toBoolean).getOrElse(false)
  }

  /**
   * Pass the SinkRecords to the Azure Document DB writerManager for storing them
   */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    require(writerManager.nonEmpty, "Writer is not set!")
    val seq = records.asScala
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
