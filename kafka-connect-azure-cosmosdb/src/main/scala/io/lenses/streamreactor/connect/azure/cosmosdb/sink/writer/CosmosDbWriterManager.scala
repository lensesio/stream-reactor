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
import cats.implicits._
import com.azure.cosmos.CosmosClient
import com.azure.cosmos.implementation.Document
import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.errors.ErrorHandler
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord

import scala.collection.mutable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * <h1>CosmosDbWriter</h1>
 * Azure CosmosDb Json writer for Kafka connect
 * Writes a list of Kafka connect sink records to Azure CosmosDb using the JSON support.
 */
class CosmosDbWriterManager(
  sinkName:            String,
  configMap:           Map[String, Kcql],
  batchPolicyMap:      Map[String, BatchPolicy],
  settings:            CosmosDbSinkSettings,
  documentClient:      CosmosClient,
  fnConvertToDocument: (SinkRecord, Map[String, String], Set[String]) => Either[Throwable, Document],
) extends StrictLogging
    with ErrorHandler {

  private[cosmosdb] val writers: mutable.Map[Topic, CosmosDbWriter] = mutable.Map.empty

  //initialize error tracker
  initialize(settings.taskRetries, settings.errorPolicy)

  /**
   * Write SinkRecords to Azure Document Db.
   *
   * @param records A list of SinkRecords from Kafka Connect to write.
   */
  def write(records: Iterable[SinkRecord]): Unit =
    if (records.nonEmpty) {
      insert(records)
    }

  /**
   * Write SinkRecords to Azure Document Db
   *
   * @param records A list of SinkRecords from Kafka Connect to write.
   * @return boolean indication successful write.
   */
  private def insert(records: Iterable[SinkRecord]): Unit = {
    val results = records
      .groupBy(sinkRecord => new Topic(sinkRecord.topic()))
      .toList
      .map {
        case (topic, records) =>
          writers.get(topic) match {
            case Some(writer) =>
              insertToWriter(topic, records, writer)
            case None =>
              createWriter(topic) match {
                case Right(newWriter) =>
                  writers.update(topic, newWriter)
                  insertToWriter(topic, records, newWriter)
                case Left(err) =>
                  Left((topic, err))
              }
          }
      }

    val errors = results.collect { case Left((partition, ex)) => (partition, ex) }

    if (errors.nonEmpty) {
      errors.foreach { case (partition, ex) =>
        logger.error(s"There was an error inserting records for topic [$partition]: ${ex.getMessage}", ex)
      }
      // Agregate all exceptions into one, or handle as needed
      // For now, just handle the first error (to preserve previous behavior)
      handleTry(Failure(errors.head._2)).getOrElse(())
    } else {
      handleTry(Success(Some(())))
    }
    ()
  }

  /**
   * Attempts to insert records into the specified writer for a given topic.
   *
   * @param topic   The topic associated with the records being inserted.
   * @param records The iterable collection of SinkRecords to be inserted.
   * @param writer  The CosmosDbWriter responsible for handling the insertion.
   * @return        Either a tuple containing the topic and an exception if an error occurs,
   *                or a successful unit result if the insertion is completed without issues.
   */
  private def insertToWriter(topic: Topic, records: Iterable[SinkRecord], writer: CosmosDbWriter) =
    Try(writer.insert(records))
      .toEither
      .leftMap(ex => (topic, ex))

  private[cosmosdb] def createWriter(recordTopic: Topic): Either[ConnectException, CosmosDbWriter] = {
    val topicName = recordTopic.value
    val newWriter = for {
      kcql <-
        configMap.get(topicName).toRight(new ConnectException(s"[$topicName] is not handled by the configuration."))
      writer <-
        if (settings.bulkEnabled) {
          for {
            _ <- logger.info(s"Creating bulk writer for topic $topicName").asRight
            bulkWriter <- CosmosDbBulkWriter(
              sinkName,
              topicName,
              kcql,
              batchPolicyMap,
              settings,
              documentClient,
              fnConvertToDocument,
            )
          } yield bulkWriter
        } else {
          for {
            _ <- logger.info(s"Creating single writer for topic $topicName").asRight
            singleWriter = new CosmosDbSingleWriter(
              kcql,
              settings,
              documentClient,
              fnConvertToDocument,
            )
          } yield singleWriter
        }
    } yield writer
    newWriter
  }

  def close(): Unit = {
    logger.info("Shutting down Document DB writer.")
    documentClient.close()
    writers.values.foreach(_.close())
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
