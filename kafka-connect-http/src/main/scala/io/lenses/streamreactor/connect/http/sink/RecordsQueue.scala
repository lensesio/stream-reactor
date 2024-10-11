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
import cats.effect.std.Mutex
import cats.implicits.toFoldableOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.http.sink.OffsetMergeUtils.createCommitContextForEvaluation
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord

import scala.collection.mutable

/**
  * The `RecordsQueue` class manages a queue of `RenderedRecord` objects and handles the logic for
  * enqueuing, dequeuing, and processing batches of records based on a commit policy.
  *
  * @param recordsQueue The mutable queue holding the `RenderedRecord` objects.
  * @param recordsQueueLock A mutex to ensure thread-safe access to the queue.
  * @param commitPolicy The policy that determines when a batch of records should be committed.
  * @param commitContextRef A reference to the current commit context.
  */
class RecordsQueue(
  val recordsQueue: mutable.Queue[RenderedRecord],
  recordsQueueLock: Mutex[IO],
  commitContextRef: Ref[IO, HttpCommitContext],
  commitPolicy:     CommitPolicy,
) extends LazyLogging {

  /**
    * Enqueues a sequence of `RenderedRecord` objects into the queue.
    *
    * @param records The records to be enqueued.
    * @return An `IO` action that enqueues the records.
    */
  def enqueueAll(records: Seq[RenderedRecord]): IO[Unit] =
    recordsQueueLock.lock.surround(IO(recordsQueue.enqueueAll(records)))

  /**
    * Takes a batch of records from the queue based on the commit policy.
    *
    * @return An `IO` action that returns a `BatchInfo` object representing the batch of records.
    */
  def takeBatch(): IO[BatchInfo] =
    recordsQueueLock.lock.surround(iterateQueueUntilRecordsTriggerCommit)

  /**
    * Iterates through the queue until the records trigger a commit based on the commit policy.
    * Uses `AnyM` to fold over the queue and stop at the first record that triggers a commit.
    * If the commit policy is met, it returns a `Left` with the batch of records and the updated commit context.
    * If the commit policy is not met, it returns a `Right` with the batch of records and the updated commit context.
    *
    * @return An `IO` action that returns a `BatchInfo` object representing the batch of records.
    */
  private def iterateQueueUntilRecordsTriggerCommit: IO[BatchInfo] = {
    for {
      initialContext <- commitContextRef.get
      result <- IO(
        recordsQueue.toList.foldM((Seq.empty[RenderedRecord], initialContext)) {
          case ((accRecords, currentContext), record) =>
            val updatedRecords   = accRecords :+ record
            val newCommitContext = createCommitContextForEvaluation(updatedRecords, currentContext)

            Either.cond(
              !commitPolicy.shouldFlush(newCommitContext),
              (updatedRecords, newCommitContext),
              (updatedRecords, newCommitContext),
            )
        },
      )
    } yield result
  }.map {
    case Left((records: Seq[RenderedRecord], lastContext: HttpCommitContext)) =>
      NonEmptyBatchInfo(
        NonEmptySeq.fromSeqUnsafe(records),
        lastContext,
        recordsQueue.size,
      )
    case Right((_, _)) =>
      EmptyBatchInfo(recordsQueue.size)
  }

  /**
    * Dequeues a non-empty batch of `RenderedRecord` objects from the queue.
    *
    * @param nonEmptyBatch The batch of records to be dequeued.
    * @return An `IO` action that dequeues the records.
    */
  def dequeue(nonEmptyBatch: NonEmptySeq[RenderedRecord]): IO[Unit] =
    for {
      _ <- IO.delay(logger.info("Queue before: {}", recordsQueue.toSeq))
      _ <- recordsQueueLock.lock.surround(
        IO {
          recordsQueue.removeHeadWhile(nonEmptyBatch.toSeq.contains)
        },
      )
      _ <- IO.delay(logger.info("Queue after: {}", recordsQueue.toSeq))
    } yield { () }

}
