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

import cats.data.NonEmptySeq
import com.azure.cosmos.CosmosClient
import com.azure.cosmos.implementation.Document
import com.azure.cosmos.models.CosmosBulkOperationResponse
import com.azure.cosmos.models.CosmosItemOperation
import com.typesafe.scalalogging.LazyLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.batch.OffsetMergeUtils.updateCommitContextPostCommit
import io.lenses.streamreactor.common.batch.EmptyBatchInfo
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.common.batch.NonEmptyBatchInfo
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.clients.consumer.OffsetAndMetadata

import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._

class CosmosDbQueueProcessor(
  sinkName:        String,
  errorThreshold:  Int,
  executorThreads: Int,
  delay:           FiniteDuration,
  recordsQueue:    CosmosRecordsQueue[PendingRecord],
  documentClient:  CosmosClient,
  settings:        CosmosDbSinkSettings,
  config:          Kcql,
) extends Runnable
    with LazyLogging {

  private val executorService = Executors.newScheduledThreadPool(executorThreads)
  executorService.scheduleWithFixedDelay(this, delay.toMillis, delay.toMillis, TimeUnit.MILLISECONDS)

  private val commitContextRef      = new AtomicReference[HttpCommitContext](HttpCommitContext.default(sinkName))
  private val unrecoverableErrorRef = new AtomicReference[Option[Throwable]](Option.empty)
  def unrecoverableError(): Option[Throwable] = unrecoverableErrorRef.get()

  override def run(): Unit =
    try {
      process()
    } catch {
      case e: Throwable =>
        addErrorToCommitContext(e) match {
          case Some(e) =>
            logger.error("Error in HttpWriter", e)
            unrecoverableErrorRef.set(addErrorToCommitContext(e))
            throw e
          case None =>
            logger.error("Error in HttpWriter but not reached threshold so ignoring", e)
        }
    }

  private def process(): Unit = {
    val batchInfo = recordsQueue.popBatch()
    batchInfo match {
      case EmptyBatchInfo(totalQueueSize) =>
        logger.debug(s"[$sinkName] No batch yet, queue size: $totalQueueSize")
      case nonEmptyBatchInfo: NonEmptyBatchInfo[_] =>
        processBatch(
          nonEmptyBatchInfo.asInstanceOf[NonEmptyBatchInfo[PendingRecord]],
          nonEmptyBatchInfo.batch.asInstanceOf[NonEmptySeq[PendingRecord]],
          nonEmptyBatchInfo.queueSize,
        )
    }
  }

  private def processBatch(
    nonEmptyBatchInfo: NonEmptyBatchInfo[PendingRecord],
    batch:             NonEmptySeq[PendingRecord],
    totalQueueSize:    Int,
  ): Unit = {
    logger.debug(s"[$sinkName] HttpWriter.process, batch of ${batch.length}, queue size: $totalQueueSize")
    recordsQueue.dequeue(batch.toSeq.toList)
    logger.trace(s"[$sinkName] modifyCommitContext for batch of ${nonEmptyBatchInfo.batch.length}")
    flush(nonEmptyBatchInfo.batch)
    val updatedCommitContext = updateCommitContextPostCommit(nonEmptyBatchInfo.updatedCommitContext)
    logger.trace(s"[$sinkName] Updating sink context to: $updatedCommitContext")
    commitContextRef.set(updatedCommitContext)
    resetErrorsInCommitContext()
  }

  private def addErrorToCommitContext(e: Throwable): Option[Throwable] = {
    commitContextRef.getAndUpdate {
      commitContext => commitContext.addError(e)
    }
    commitContextRef.get().errors
      .maxByOption { case (_, errSeq) => errSeq.size }
      .filter {
        case (_, errSeq) => errSeq.size > errorThreshold
      }
      .flatMap {
        case (_, errSeq) => errSeq.headOption
      }
  }

  private def resetErrorsInCommitContext(): Unit = {
    commitContextRef.getAndUpdate {
      commitContext => commitContext.resetErrors
    }
    ()
  }

  private def flush(records: NonEmptySeq[PendingRecord]): List[CosmosBulkOperationResponse[Document]] = {
    val bulkOps: java.lang.Iterable[CosmosItemOperation] = records
      .map(_.cosmosItemOperation)
      .toSeq
      .asJava

    documentClient.getDatabase(settings.database)
      .getContainer(config.getTarget)
      .executeBulkOperations(bulkOps)
      .asScala
      .toList
  }

  def preCommit(offsetAndMetadatas: Map[TopicPartition, OffsetAndMetadata]): Map[TopicPartition, OffsetAndMetadata] = {
    val commitContext = commitContextRef.get()
    offsetAndMetadatas.map {
      case (partition, metadata) =>
        val offsetForPartition: Option[Offset] = commitContext.committedOffsets.get(partition)
        offsetForPartition match {
          case None => partition -> metadata
          case Some(offset) => partition -> new OffsetAndMetadata(
              offset.value,
              metadata.leaderEpoch(),
              metadata.metadata(),
            )
        }
    }

  }

}
