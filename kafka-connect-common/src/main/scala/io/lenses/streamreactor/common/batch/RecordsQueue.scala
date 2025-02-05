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
package io.lenses.streamreactor.common.batch

import cats.data.NonEmptySeq
import cats.effect.IO
import cats.effect.Ref
import cats.implicits.toFoldableOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.batch.RecordsQueueBatcher.takeBatch
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.apache.kafka.connect.errors.RetriableException

import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/**
 * The `RecordsQueue` class manages a queue of `RenderedRecord` objects and handles the logic for
 * enqueuing, dequeuing, and processing batches of records based on a commit policy.
 *
 * @param recordsQueue The mutable queue holding the `RenderedRecord` objects.
 * @param commitContextRef A reference to the current commit context.
 * @param commitPolicy The policy that determines when a batch of records should be committed.
 */
class RecordsQueue[B <: BatchRecord](
  val recordsQueue: Ref[IO, Queue[B]],
  commitContextRef: Ref[IO, HttpCommitContext],
  batchPolicy:      BatchPolicy,
  maxSize:          Int,
  offerTimeout:     FiniteDuration,
  offsetMapRef:     Ref[IO, Map[TopicPartition, Offset]],
) extends LazyLogging {

  /**
   * Enqueues a sequence of `RenderedRecord` objects into the queue, with a maximum size limit.
   * If the queue is full, it retries adding the remaining records within the specified timeout.
   * If after the timeout records remain, it throws a RetriableException.
   * Also, it discards any records for which the offset was already queued.
   *
   * @param records The records to be enqueued.
   * @return An `IO` action that enqueues the records or throws a RetriableException if the queue remains full.
   */
  def enqueueAll(records: NonEmptySeq[B]): IO[Unit] = {

    // Filter out records with offsets that have already been processed
    def filterDuplicates(records: List[B], offsetMap: Map[TopicPartition, Offset]): List[B] =
      records.filter { record =>
        val tp = record.topicPartitionOffset.toTopicPartition
        offsetMap.get(tp) match {
          case Some(lastOffset) if record.topicPartitionOffset.offset.value <= lastOffset.value =>
            // Offset already processed, discard this record
            false
          case _ =>
            true
        }
      }

    def attemptEnqueue(remainingRecords: List[B], startTime: Long): IO[Unit] =
      if (remainingRecords.isEmpty) {
        IO.unit
      } else {
        for {
          currentTime <- IO.realTime.map(_.toMillis)
          elapsedTime  = currentTime - startTime
          _ <-
            if (elapsedTime >= offerTimeout.toMillis) {
              IO.raiseError(new RetriableException("Enqueue timed out and records remain"))
            } else {
              for {
                (recordsToAdd, recordsRemaining) <- recordsQueue.modify { queue =>
                  val queueSize        = queue.size
                  val spaceAvailable   = maxSize - queueSize
                  val recordsToAdd     = remainingRecords.take(spaceAvailable)
                  val recordsRemaining = remainingRecords.drop(spaceAvailable)
                  val newQueue         = queue.enqueueAll(recordsToAdd)
                  (newQueue, (recordsToAdd, recordsRemaining))
                }
                _ <-
                  if (recordsToAdd.nonEmpty) {
                    // Update the offset map with the offsets of the records that were actually enqueued
                    offsetMapRef.update { offsetMap =>
                      recordsToAdd.foldLeft(offsetMap) { (accOffsets, record) =>
                        val tp     = record.topicPartitionOffset.toTopicPartition
                        val offset = record.topicPartitionOffset.offset
                        // Only update if the new offset is greater
                        val updatedOffset: Offset = accOffsets.get(tp) match {
                          case Some(existingOffset) if existingOffset.value >= offset.value => existingOffset
                          case _                                                            => offset
                        }
                        accOffsets.updated(tp, updatedOffset)
                      }
                    }
                  } else IO.unit
                _ <-
                  if (recordsRemaining.nonEmpty) {
                    IO.sleep(5.millis) *>
                      attemptEnqueue(recordsRemaining, startTime)
                  } else IO.unit
              } yield ()
            }
        } yield ()
      }

    for {
      offsetMap    <- offsetMapRef.get
      uniqueRecords = filterDuplicates(records.toList, offsetMap)
      startTime    <- IO.realTime.map(_.toMillis)
      _            <- attemptEnqueue(uniqueRecords, startTime)
    } yield ()
  }

  /**
   * Takes a batch of records from the queue based on the commit policy.
   *
   * @return An `IO` action that returns a `BatchInfo` object representing the batch of records.
   */
  def popBatch(): IO[BatchInfo] =
    for {
      initialContext <- commitContextRef.get
      queueState <- recordsQueue.get.map { records =>
        takeBatch(batchPolicy, initialContext, records)
      }
      _ <- queueState match {
        case EmptyBatchInfo(queueSize) => IO.delay(logger.debug(s"no records taken from ($queueSize) $recordsQueue"))
        case NonEmptyBatchInfo(batch, _, queueSize) =>
          IO.delay(logger.debug(s"${batch.length} records taken from ($queueSize) $recordsQueue"))
      }
    } yield queueState

  /**
   * Dequeues a non-empty batch of `RenderedRecord` objects from the queue.
   *
   * @param nonEmptyBatch The batch of records to be dequeued.
   * @return An `IO` action that dequeues the records.
   */
  def dequeue(nonEmptyBatch: NonEmptySeq[B]): IO[Unit] =
    recordsQueue.access.flatMap {
      case (records, updater) =>
        val lookup = nonEmptyBatch.toSeq.toSet
        for {
          newQueue <- IO(records.dropWhile(lookup.contains))
          _        <- updater(newQueue)
          _        <- IO.delay(logger.debug("Queue before: {}, after: {}", records, newQueue))
        } yield ()
    }

}
