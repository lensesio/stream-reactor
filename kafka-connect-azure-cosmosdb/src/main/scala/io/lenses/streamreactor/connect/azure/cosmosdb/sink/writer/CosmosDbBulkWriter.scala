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

import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.sink.SinkRecord
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import com.azure.cosmos.models.CosmosBulkOperations
import com.azure.cosmos.models.CosmosItemOperation
import com.azure.cosmos.models.CosmosItemRequestOptions
import com.azure.cosmos.models.PartitionKey
import com.azure.cosmos.implementation.Document
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import com.azure.cosmos.CosmosClient
import io.lenses.streamreactor.common.batch.BatchPolicy
import org.apache.kafka.connect.errors.ConnectException
import cats.implicits._
import io.lenses.kcql.WriteModeEnum

import scala.util.Try

class CosmosDbBulkWriter(
  config:              Kcql,
  recordsQueue:        CosmosRecordsQueue[PendingRecord],
  queueProcessor:      CosmosDbQueueProcessor,
  settings:            CosmosDbSinkSettings,
  fnConvertToDocument: (SinkRecord, Map[String, String], Set[String]) => Either[Throwable, Document],
) extends CosmosDbWriter
    with StrictLogging {

  private val requestOptionsInsert: CosmosItemRequestOptions = new CosmosItemRequestOptions()
    .setConsistencyLevel(settings.consistency)

  private def sinkRecordToDocument(sinkRecord: SinkRecord): Either[Throwable, Document] =
    fnConvertToDocument(
      sinkRecord,
      settings.fields(sinkRecord.topic()),
      settings.ignoredField(sinkRecord.topic()),
    )

  private def documentToItemOperation(document: Document): Either[Throwable, CosmosItemOperation] =
    Right(
      config.getWriteMode match {
        case WriteModeEnum.INSERT =>
          CosmosBulkOperations.getCreateItemOperation(
            document,
            new PartitionKey(document.getId),
            requestOptionsInsert,
          )
        case WriteModeEnum.UPSERT =>
          CosmosBulkOperations.getUpsertItemOperation(
            document,
            new PartitionKey(document.getId),
            requestOptionsInsert,
          )
      },
    )

  private def convert(sinkRecord: SinkRecord): Either[Throwable, PendingRecord] =
    for {
      doc    <- sinkRecordToDocument(sinkRecord)
      itemOp <- documentToItemOperation(doc)
    } yield PendingRecord(
      Topic(sinkRecord.topic()).withPartition(sinkRecord.kafkaPartition()).atOffset(sinkRecord.kafkaOffset()),
      doc,
      itemOp,
    )

  def insert(newRecords: Iterable[SinkRecord]): Either[Throwable, Unit] = {
    val (errors, pendingRecords) =
      newRecords.map(convert).toList.partitionMap(identity)
    if (errors.nonEmpty) {
      errors.foreach(e => logger.error("Failed to convert record to cosmos db document", e))
    }
    for {
      _      <- unrecoverableError().toLeft[Unit](())
      insert <- Try(recordsQueue.enqueueAll(pendingRecords)).toEither
    } yield insert
  }

  override def preCommit(
    offsetAndMetadatas: Map[TopicPartition, OffsetAndMetadata],
  ): Map[TopicPartition, OffsetAndMetadata] = queueProcessor.preCommit(offsetAndMetadatas)

  override def unrecoverableError(): Option[Throwable] = queueProcessor.unrecoverableError()

  override def close(): Unit =
    queueProcessor.close()
}

object CosmosDbBulkWriter {
  def apply(
    sinkName:            String,
    topicName:           String,
    kcql:                Kcql,
    batchPolicyMap:      Map[String, BatchPolicy],
    settings:            CosmosDbSinkSettings,
    documentClient:      CosmosClient,
    fnConvertToDocument: (SinkRecord, Map[String, String], Set[String]) => Either[Throwable, Document],
  ): Either[ConnectException, CosmosDbBulkWriter] =
    for {
      batchPolicy <- batchPolicyMap.get(topicName).toRight(
        new ConnectException(s"[$topicName] is not handled by the configuration."),
      )
      recordsQueue <- new CosmosRecordsQueue[PendingRecord](
        sinkName,
        settings.maxQueueSize,
        settings.maxQueueOfferTimeout,
        batchPolicy,
      ).asRight
      queueProcessor <- new CosmosDbQueueProcessor(
        sinkName,
        settings.errorThreshold,
        settings.executorThreads,
        settings.delay,
        recordsQueue,
        documentClient,
        settings.database,
        kcql.getTarget,
      ).asRight
    } yield new CosmosDbBulkWriter(
      kcql,
      recordsQueue,
      queueProcessor,
      settings,
      fnConvertToDocument,
    )
}
