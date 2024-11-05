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
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.http.sink.OffsetMergeUtils.createCommitContextForEvaluation
import io.lenses.streamreactor.connect.http.sink.commit.BatchPolicy
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord

import scala.collection.immutable.Queue
import scala.collection.mutable

object RecordsQueueBatcher extends LazyLogging {

  /**
    * Iterates through the queue until the records trigger a commit based on the commit policy.
    * Uses `foldM` to fold over the queue and stop at the first record that triggers a commit.
    * If the commit policy is met, it returns a `Left` with the batch of records and the updated commit context.
    * If the commit policy is not met, it returns a `Right` with the batch of records and the updated commit context.
    *
    * @param commitPolicy The commit policy.
    * @param initialContext The initial commit context.
    * @param records The queue of records to be processed.
    * @return Either a tuple of the batch of records and the updated commit context, or the same tuple if the commit policy is not met.
    */
  def takeBatch(
    batchPolicy:    BatchPolicy,
    initialContext: HttpCommitContext,
    records:        Queue[RenderedRecord],
  ): BatchInfo = {

    val batch          = mutable.Buffer[RenderedRecord]()
    var currentContext = initialContext

    var greedyTriggerReached = false
    var triggerReached       = false
    records.takeWhile {
      record =>
        val updatedRecords = batch.toSeq :+ record
        val updatedContext = createCommitContextForEvaluation(updatedRecords, currentContext)
        val addToBatch     = batchPolicy.shouldBatch(updatedContext)
        triggerReached       = addToBatch.triggerReached
        greedyTriggerReached = addToBatch.greedyTriggerReached
        logger.debug(
          s"Trigger Reached: $triggerReached, Greedy trigger Reached: $greedyTriggerReached, Fits in batch: ${addToBatch.fitsInBatch}",
        )

        if (addToBatch.fitsInBatch) {
          batch.addOne(record)
          currentContext = updatedContext
        }
        !triggerReached || (!triggerReached && greedyTriggerReached)
    }

    if (triggerReached || (!triggerReached && greedyTriggerReached)) {
      NonEmptySeq.fromSeq(batch.toSeq)
        .map(value => NonEmptyBatchInfo(value, currentContext, records.size))
        .getOrElse(EmptyBatchInfo(records.size))
    } else {
      EmptyBatchInfo(records.size)
    }
  }

}
