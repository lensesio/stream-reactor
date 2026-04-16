/*
 * Copyright 2017-2026 Lenses.io Ltd
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

import io.lenses.streamreactor.common.batch.RecordsQueueBatcher.takeBatch
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.batch.BatchRecord
import io.lenses.streamreactor.common.batch._
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.connect.errors.RetriableException

import scala.collection.immutable.Queue
import scala.concurrent.duration.FiniteDuration
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

class CosmosRecordsQueue[B <: BatchRecord](
  val sinkName:     String,
  val maxSize:      Int,
  val offerTimeout: FiniteDuration,
  val batchPolicy:  BatchPolicy,
) extends LazyLogging {

  private val recordsQueue = new AtomicReference[Queue[B]](Queue.empty)
  private val offsetMap    = new AtomicReference[Map[TopicPartition, Offset]](Map.empty)
  private val commitContextRef =
    new AtomicReference[HttpCommitContext](
      HttpCommitContext.default(sinkName),
    )

  def enqueueAll(records: List[B]): Unit = {
    def filterDuplicates(records: List[B], offsetMap: Map[TopicPartition, Offset]): List[B] =
      records.filter { record =>
        val tp = record.topicPartitionOffset.toTopicPartition
        offsetMap.get(tp)
          .forall(record.topicPartitionOffset.offset.value > _.value)
      }

    @tailrec
    def attemptEnqueueCAS(remainingRecords: List[B], startTime: Long): Unit = {
      if (remainingRecords.isEmpty) return

      val currentTime = System.currentTimeMillis()
      val elapsedTime = currentTime - startTime
      if (elapsedTime >= offerTimeout.toMillis) {
        throw new RetriableException("Enqueue timed out and records remain")
      } else {
        val oldQueue         = recordsQueue.get()
        val queueSize        = oldQueue.size
        val spaceAvailable   = maxSize - queueSize
        val recordsToAdd     = remainingRecords.take(spaceAvailable)
        val recordsRemaining = remainingRecords.drop(spaceAvailable)
        val newQueue         = oldQueue.enqueueAll(recordsToAdd)

        if (recordsQueue.compareAndSet(oldQueue, newQueue)) {
          // Atomically update offsetMap for the added records
          if (recordsToAdd.nonEmpty) {
            offsetMap.getAndUpdate { currentOffsets =>
              recordsToAdd.foldLeft(currentOffsets) { (accOffsets, record) =>
                val tp            = record.topicPartitionOffset.toTopicPartition
                val offset        = record.topicPartitionOffset.offset
                val updatedOffset = accOffsets.get(tp).filter(_.value >= offset.value).getOrElse(offset)
                accOffsets.updated(tp, updatedOffset)
              }
            }
          }
          if (recordsRemaining.nonEmpty) {
            Thread.sleep(5)
            attemptEnqueueCAS(recordsRemaining, startTime)
          }
        } else {
          // CAS failed, retry with latest queue
          attemptEnqueueCAS(remainingRecords, startTime)
        }
      }
    }

    val uniqueRecords = filterDuplicates(records, offsetMap.get())
    val startTime     = System.currentTimeMillis()
    attemptEnqueueCAS(uniqueRecords, startTime)
  }

  def peekBatch(): BatchInfo = {
    val initialContext = commitContextRef.get()
    val queue          = recordsQueue.get()
    val queueState     = takeBatch(batchPolicy, initialContext, queue)
    queueState match {
      case EmptyBatchInfo(queueSize) =>
        logger.debug(s"no records taken from ($queueSize) $recordsQueue")
      case NonEmptyBatchInfo(batch, _, queueSize) =>
        logger.debug(s"${batch.length} records taken from ($queueSize) $recordsQueue")
    }
    queueState
  }

  @tailrec
  final def dequeue(nonEmptyBatch: List[B]): Unit = {
    val lookup   = nonEmptyBatch.toSet
    val oldQueue = recordsQueue.get()
    val newQueue = oldQueue.dropWhile(lookup.contains)
    if (!recordsQueue.compareAndSet(oldQueue, newQueue)) {
      dequeue(nonEmptyBatch) // retry if concurrent update
    } else {
      logger.debug("Queue before: {}, after: {}", oldQueue, newQueue)
    }
  }
}
