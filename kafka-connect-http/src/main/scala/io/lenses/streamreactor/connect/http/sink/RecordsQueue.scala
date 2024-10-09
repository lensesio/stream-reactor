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
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.http.sink.OffsetMergeUtils.createCommitContextForEvaluation
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord

import scala.collection.mutable

class RecordsQueue(
  val recordsQueue:   mutable.Queue[RenderedRecord],
  commitPolicy:       CommitPolicy,
  fnGetCommitContext: () => HttpCommitContext,
) {

  def enqueueAll(records: Seq[RenderedRecord]): Unit =
    recordsQueue.enqueueAll(records)

  private def commitContextShouldFlush(combinedRecords: Seq[RenderedRecord]): Boolean =
    commitPolicy.shouldFlush(
      createCommitContextForEvaluation(combinedRecords, fnGetCommitContext()),
    )

  def takeBatch(): BatchInfo = {
    val accumulatedRecords = mutable.Buffer[RenderedRecord]()
    val queueIter          = recordsQueue.iterator

    while (queueIter.hasNext && !commitContextShouldFlush(accumulatedRecords.toSeq)) {
      accumulatedRecords += queueIter.next()
    }

    BatchInfo(
      NonEmptySeq.fromSeq(accumulatedRecords.toSeq),
      recordsQueue.size,
    )
  }

  def dequeue(nonEmptyBatch: NonEmptySeq[RenderedRecord]): Unit = {
    recordsQueue.removeHeadWhile(b => nonEmptyBatch.toSeq.contains(b))
    ()
  }

}
