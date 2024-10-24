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
import cats.implicits.toFoldableOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.http.sink.OffsetMergeUtils.createCommitContextForEvaluation
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
  commitPolicy:     CommitPolicy,
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
  def takeBatch(): IO[BatchInfo] = {
    for {
      initialContext <- commitContextRef.get
      queueState <- recordsQueue.get.map { records =>
        (foldRecordsToFindCommit(initialContext, records), records.size)
      }

      (result, size) = queueState
      _             <- IO.delay(logger.debug(s"$size records taken from $recordsQueue"))
    } yield (result, size)
  }.map {
    case (Left((records: Seq[RenderedRecord], lastContext: HttpCommitContext)), size) =>
      NonEmptyBatchInfo(
        NonEmptySeq.fromSeqUnsafe(records),
        lastContext,
        size,
      )
    case (Right((_, _)), size) =>
      EmptyBatchInfo(size)
  }

  /**
    * Iterates through the queue until the records trigger a commit based on the commit policy.
    * Uses `foldM` to fold over the queue and stop at the first record that triggers a commit.
    * If the commit policy is met, it returns a `Left` with the batch of records and the updated commit context.
    * If the commit policy is not met, it returns a `Right` with the batch of records and the updated commit context.
    *
    * @param initialContext The initial commit context.
    * @param records The queue of records to be processed.
    * @return Either a tuple of the batch of records and the updated commit context, or the same tuple if the commit policy is not met.
    */
  private def foldRecordsToFindCommit(
    initialContext: HttpCommitContext,
    records:        Queue[RenderedRecord],
  ): Either[(Seq[RenderedRecord], HttpCommitContext), (Seq[RenderedRecord], HttpCommitContext)] =
    records.foldM((Seq.empty[RenderedRecord], initialContext)) {
      case ((accRecords, currentContext), record) =>
        val updatedRecords   = accRecords :+ record
        val newCommitContext = createCommitContextForEvaluation(updatedRecords, currentContext)

        Either.cond(
          !commitPolicy.shouldFlush(newCommitContext),
          (updatedRecords, newCommitContext),
          (updatedRecords, newCommitContext),
        )
    }

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
