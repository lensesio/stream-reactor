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
  val maxSize:      Int,
  val offerTimeout: FiniteDuration,
) extends LazyLogging {

  private val recordsQueue = new AtomicReference[Queue[B]](Queue.empty)
  private val offsetMap    = new AtomicReference[Map[TopicPartition, Offset]](Map.empty)
  private val commitContextRef =
    new AtomicReference[HttpCommitContext](null) // Replace null with actual initial context
  private val batchPolicy: BatchPolicy = null // Set appropriately

  def enqueueAll(records: List[B]): Unit = {
    def filterDuplicates(records: List[B], offsetMap: Map[TopicPartition, Offset]): List[B] =
      records.filter { record =>
        val tp = record.topicPartitionOffset.toTopicPartition
        offsetMap.get(tp) match {
          case Some(lastOffset) if record.topicPartitionOffset.offset.value <= lastOffset.value => false
          case _                                                                                => true
        }
      }

    @tailrec
    def attemptEnqueue(remainingRecords: List[B], startTime: Long): Unit = {
      if (remainingRecords.isEmpty) return

      val currentTime = System.currentTimeMillis()
      val elapsedTime = currentTime - startTime
      if (elapsedTime >= offerTimeout.toMillis) {
        throw new RetriableException("Enqueue timed out and records remain")
      } else {
        val queue            = recordsQueue.get()
        val queueSize        = queue.size
        val spaceAvailable   = maxSize - queueSize
        val recordsToAdd     = remainingRecords.take(spaceAvailable)
        val recordsRemaining = remainingRecords.drop(spaceAvailable)
        val newQueue         = queue.enqueueAll(recordsToAdd)
        recordsQueue.set(newQueue)

        if (recordsToAdd.nonEmpty) {
          val currentOffsets = offsetMap.get()
          val updatedOffsets = recordsToAdd.foldLeft(currentOffsets) { (accOffsets, record) =>
            val tp     = record.topicPartitionOffset.toTopicPartition
            val offset = record.topicPartitionOffset.offset
            val updatedOffset = accOffsets.get(tp) match {
              case Some(existingOffset) if existingOffset.value >= offset.value => existingOffset
              case _                                                            => offset
            }
            accOffsets.updated(tp, updatedOffset)
          }
          offsetMap.set(updatedOffsets)
        }

        if (recordsRemaining.nonEmpty) {
          Thread.sleep(5)
          attemptEnqueue(recordsRemaining, startTime)
        }
      }
    }

    val uniqueRecords = filterDuplicates(records, offsetMap.get())
    val startTime     = System.currentTimeMillis()
    attemptEnqueue(uniqueRecords, startTime)
  }

  def popBatch(): BatchInfo = {
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

  def dequeue(nonEmptyBatch: List[B]): Unit = {
    val queue    = recordsQueue.get()
    val lookup   = nonEmptyBatch.toSet
    val newQueue = queue.dropWhile(lookup.contains)
    recordsQueue.set(newQueue)
    logger.debug("Queue before: {}, after: {}", queue, newQueue)
  }
}
