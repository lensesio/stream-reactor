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
package io.lenses.streamreactor.connect.cassandra

import com.datastax.oss.common.sink.AbstractSinkRecord
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.util.AsciiArtPrinter.printAsciiHeader
import io.lenses.streamreactor.common.utils.JarManifestProvided
import io.lenses.streamreactor.common.utils.ProgressCounter
import io.lenses.streamreactor.connect.cassandra.CassandraSettings.getOSSCassandraSinkConfig
import io.lenses.streamreactor.connect.cassandra.adapters.RecordAdapter
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.apache.kafka.connect.sink.SinkTask

import java.util
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava
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
  private val progressCounter = new ProgressCounter
  private var cassandraProcessor: Option[CassandraProcessor]                         = None
  private var sinkSettings:       Option[CassandraSettings]                          = None
  private val errors:             AtomicReference[List[CassandraProcessorException]] = new AtomicReference(List.empty)

  override def start(props: util.Map[String, String]): Unit = {
    printAsciiHeader(manifest, "/cass-sink-ascii.txt")

    val config = if (context.configs().isEmpty) props else context.configs()

    val trySettings = for {
      taskConfig <- Try(new CassandraConfig(config.asScala.toMap))
      settings   <- Try(CassandraSettings.configureSink(taskConfig))
    } yield settings

    trySettings match {
      case Success(settings) =>
        sinkSettings = Some(settings)
        val processor = new CassandraProcessor(settings.connectorName,
                                               settings.ignoredError,
                                               new TrieMap[TopicPartition, OffsetAndMetadata](),
                                               context,
                                               errors,
        )
        try {
          processor.start(getOSSCassandraSinkConfig(settings).asJava)
        } catch {
          case e: Throwable =>
            logger.error(s"[$connectorName]Failed to start Cassandra Processor due to configuration error.", e)
            throw new ConnectException("Couldn't start Cassandra Processor due to configuration error.", e)
        }
        cassandraProcessor = Some(processor)
      case Failure(e) =>
        logger.error(s"[$connectorName]Failed to start Cassandra Sink Task due to configuration error.", e)
        throw new ConnectException("Couldn't start Cassandra Sink due to configuration error.", e)
    }
  }

  override def preCommit(
    currentOffsets: util.Map[TopicPartition, OffsetAndMetadata],
  ): util.Map[TopicPartition, OffsetAndMetadata] =
    cassandraProcessor.map { processor =>
      // Update the failure offsets in the processor
      currentOffsets.putAll(processor.getFailureOffsets.asJava)
      currentOffsets
    }.getOrElse(currentOffsets)

  /**
   * Pass the SinkRecords to the writer for Writing
   */
  override def put(records: util.Collection[SinkRecord]): Unit = {
    logger.debug(s"[$connectorName]Putting records to Cassandra sink: " + records.size())
    //if errors are present aggregate them and throw an exception
    val errorList = errors.getAndSet(List.empty)
    if (errorList.nonEmpty) {
      val errorMessage = errorList.map(_.getMessage).mkString("\n")
      logger.error(s"[$connectorName]Errors occurred during processing: $errorMessage")
      throw new ConnectException(s"Errors occurred during processing: $errorMessage")
    }
    val seq = records.asScala.toVector
    sinkSettings.filter(_.enableProgress).foreach(_ => progressCounter.update(seq))
    cassandraProcessor.foreach { processor =>
      val adapted =
        records.asScala.foldLeft(new util.ArrayList[AbstractSinkRecord](records.size())) {
          (acc, record) =>
            acc.add(RecordAdapter(record))
            acc
        }
      logger.debug(s"[$connectorName]Processing records: " + adapted.size())
      processor.put(adapted)
    }
  }

  /**
   * Clean up Cassandra connections
   */
  override def stop(): Unit = {
    logger.info(s"[$connectorName]Stopping the sink.")

    sinkSettings.filter(_.enableProgress).foreach(_ => progressCounter.empty())

    cassandraProcessor match {
      case Some(processor) =>
        Try(processor.stop()) match {
          case Success(_) => logger.info(s"[$connectorName]Cassandra processor closed successfully.")
          case Failure(exception) =>
            logger.error(s"[$connectorName]Error closing Cassandra processor.", exception)
            throw new ConnectException("Error closing Cassandra processor.", exception)
        }
      case None =>
        logger.warn(s"[$connectorName]Cassandra processor was not initialized, nothing to close.")
    }
  }

  override def flush(map: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}

  private def connectorName: String =
    sinkSettings.map(_.connectorName).getOrElse("Unknown-CassandraSinkTask")
}
