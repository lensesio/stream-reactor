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
import io.lenses.streamreactor.common.batch.OffsetMergeUtils.updateCommitContextPostCommit
import io.lenses.streamreactor.common.batch.EmptyBatchInfo
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.common.batch.NonEmptyBatchInfo
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
  database:        String,
  target:          String,
) extends Runnable
    with LazyLogging {

  private val executorService = Executors.newScheduledThreadPool(executorThreads)
  executorService.scheduleWithFixedDelay(this, delay.toMillis, delay.toMillis, TimeUnit.MILLISECONDS)

  private[writer] val commitContextRef      = new AtomicReference[HttpCommitContext](HttpCommitContext.default(sinkName))
  private[writer] val unrecoverableErrorRef = new AtomicReference[Option[Throwable]](Option.empty)
  def unrecoverableError(): Option[Throwable] = unrecoverableErrorRef.get()

  override def run(): Unit =
    try {
      process()
    } catch {
      case e: Throwable =>
        val unrecoverable = addErrorToCommitContext(e)
        unrecoverable match {
          case Some(e) =>
            logger.error("Error in HttpWriter", e)
            unrecoverableErrorRef.set(unrecoverable)
            throw e
          case None =>
            logger.error("Error in HttpWriter but not reached threshold so ignoring", e)
        }
    }

  private[writer] def process(): Unit = {
    val batchInfo = recordsQueue.peekBatch()
    batchInfo match {
      case EmptyBatchInfo(totalQueueSize) =>
        logger.debug(s"[$sinkName] No batch yet, queue size: $totalQueueSize")
      case nonEmptyBatchInfo: NonEmptyBatchInfo[_] =>
        val batchInfo = nonEmptyBatchInfo.asInstanceOf[NonEmptyBatchInfo[PendingRecord]]
        processBatch(
          batchInfo,
          batchInfo.batch,
          batchInfo.queueSize,
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

  /**
   * Adds the given error to the commit context for tracking.
   * If the number of errors for any partition exceeds the error threshold,
   * returns the first error for that partition (to be treated as unrecoverable).
   * Otherwise, returns None.
   *
   * Note: Although getAndUpdate returns the previous value, we must check the updated
   * commit context (after the error is added) to see if the threshold is exceeded.
   * This is why we use commitContextRef.get() after updating, not the value returned by getAndUpdate.
   *
   * @param e the error to add
   * @return Some(error) if the error threshold is exceeded for any partition, otherwise None
   */
  private[writer] def addErrorToCommitContext(e: Throwable): Option[Throwable] = {
    commitContextRef.getAndUpdate(_.addError(e))
    // We must check the updated context (with the error added)
    commitContextRef.get().errors
      .maxByOption { case (_, errSeq) => errSeq.size }
      .filter { case (_, errSeq) => errSeq.size > errorThreshold }
      .flatMap { case (_, errSeq) => errSeq.headOption }
  }

  private[writer] def resetErrorsInCommitContext(): Unit = {
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

    documentClient.getDatabase(database)
      .getContainer(target)
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

  def close(): Unit =
    executorService.shutdown()

}
