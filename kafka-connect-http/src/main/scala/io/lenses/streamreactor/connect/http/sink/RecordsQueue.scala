/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.http.sink
import cats.data.NonEmptySeq
import cats.effect.IO
import cats.effect.Ref
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.http.sink.RecordsQueueBatcher.takeBatch
import io.lenses.streamreactor.connect.http.sink.commit.BatchPolicy
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord

import scala.collection.immutable.Queue

/**
  * The `RecordsQueue` class manages a queue of `RenderedRecord` objects and handles the logic for
  * enqueuing, dequeuing, and processing batches of records based on a commit policy.
  *
  * @param recordsQueue The mutable queue holding the `RenderedRecord` objects.
  * @param commitContextRef A reference to the current commit context.
  * @param commitPolicy The policy that determines when a batch of records should be committed.
  */
class RecordsQueue(
  val recordsQueue: Ref[IO, Queue[RenderedRecord]],
  commitContextRef: Ref[IO, HttpCommitContext],
  batchPolicy:      BatchPolicy,
) extends LazyLogging {

  /**
    * Enqueues a sequence of `RenderedRecord` objects into the queue.
    *
    * @param records The records to be enqueued.
    * @return An `IO` action that enqueues the records.
    */
  def enqueueAll(records: NonEmptySeq[RenderedRecord]): IO[Unit] =
    for {
      _ <- IO.delay(logger.debug(s"${records.length} records added to $recordsQueue"))
      _ <- recordsQueue.getAndUpdate(q => q ++ records.toSeq).void
    } yield ()

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
  def dequeue(nonEmptyBatch: NonEmptySeq[RenderedRecord]): IO[Unit] =
    recordsQueue.access.flatMap {
      case (records, updater) =>
        for {
          newQueue <- IO(records.dropWhile(nonEmptyBatch.toSeq.contains))
          _        <- updater(newQueue)
          _        <- IO.delay(logger.debug("Queue before: {}, after: {}", records, newQueue))
        } yield ()
    }

}
