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
package io.lenses.streamreactor.connect.azure.cosmosdb.sink.writer
import cats.effect.unsafe.IORuntime
import cats.implicits._
import com.azure.cosmos.CosmosClient
import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.azure.cosmosdb.sink.converter.CosmosDbBulkRecordConverter
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.mutable
import scala.util.Failure

/**
 * <h1>CosmosDbWriter</h1>
 * Azure CosmosDb Json writer for Kafka connect
 * Writes a list of Kafka connect sink records to Azure CosmosDb using the JSON support.
 */
class CosmosDbWriterManager(
  sinkName:       String,
  configMap:      Map[String, Kcql],
  settings:       CosmosDbSinkSettings,
  documentClient: CosmosClient,
) extends StrictLogging
    with ErrorHandler {

  private val writers:         mutable.Map[Topic, CosmosDbWriter] = mutable.Map[Topic, CosmosDbWriter]()
  implicit lazy val ioRuntime: IORuntime                          = IORuntime.global

  //initialize error tracker
  initialize(settings.taskRetries, settings.errorPolicy)

  /**
   * Write SinkRecords to Azure Document Db.
   *
   * @param records A list of SinkRecords from Kafka Connect to write.
   */
  def write(records: Seq[SinkRecord]): Unit =
    if (records.nonEmpty) {
      val _ = insert(records)
    }

  /**
   * Write SinkRecords to Azure Document Db
   *
   * @param records A list of SinkRecords from Kafka Connect to write.
   * @return boolean indication successful write.
   */
  private def insert(records: Seq[SinkRecord]): Unit =
    records
      .groupBy(sinkRecord => new Topic(sinkRecord.topic()))
      .toList
      .traverse {
        case (partition, records) =>
          val writer = writers.getOrElseUpdate(partition, createWriter(partition))
          writer.insert(records)
      } match {
      case Left(exception) =>
        logger.error(s"There was an error inserting the records [${exception.getMessage}]", exception)
        handleTry(Failure(exception)).getOrElse(())

      case Right(_) =>
        ()
    }

  private def createWriter(recordTopic: Topic): CosmosDbWriter = {
    val topicName = recordTopic.value
    val kcql =
      configMap.getOrElse(topicName, throw new ConnectException(s"[$topicName] is not handled by the configuration."))
    if (settings.bulkEnabled) {
      val recordsQueue = new CosmosRecordsQueue[PendingRecord](
        settings.maxQueueSize,
        settings.maxQueueOfferTimeout,
      )
      val queueProcessor = new CosmosDbQueueProcessor(
        sinkName,
        settings.errorThreshold,
        settings.executorThreads,
        settings.delay,
        recordsQueue,
        documentClient,
        settings,
        kcql,
      )

      new CosmosDbBulkWriter(
        config              = kcql,
        recordsQueue        = recordsQueue,
        bulkRecordConverter = new CosmosDbBulkRecordConverter(settings),
        queueProcessor      = queueProcessor,
      )

    } else {
      new CosmosDbSingleWriter(
        kcql,
        settings,
        documentClient,
      )
    }
  }

  def close(): Unit = {
    logger.info("Shutting down Document DB writer.")
    documentClient.close()
  }

  def preCommit(
    topicPartitionsToMetadata: Map[TopicPartition, OffsetAndMetadata],
  ): Map[TopicPartition, OffsetAndMetadata] =
    topicPartitionsToMetadata
      .groupBy { case (partition, _) => partition.topic }
      .map {
        case (topic, partitionToMetadata) =>
          writers
            .get(topic)
            .map(_.preCommit(partitionToMetadata))
            .getOrElse(partitionToMetadata)
      }
      .flatten
      .toMap

}
