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

import scala.util.control.Breaks._
import cats.data.NonEmptySeq
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
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

  def takeBatch(): BatchInfo = {
    val accumulatedRecords = mutable.Buffer[RenderedRecord]()
    val queueIter          = recordsQueue.iterator

    var finalCommitContext: Option[HttpCommitContext] = none
    breakable {
      while (queueIter.hasNext) {
        accumulatedRecords += queueIter.next()
        val commitContext = createCommitContextForEvaluation(accumulatedRecords.toSeq, fnGetCommitContext())
        if (commitPolicy.shouldFlush(commitContext)) {
          finalCommitContext = commitContext.some
          break()
        }
      }
    }

    (finalCommitContext, NonEmptySeq.fromSeq(accumulatedRecords.toSeq)) match {
      case (Some(context), Some(records)) =>
        NonEmptyBatchInfo(
          records,
          context,
          recordsQueue.size,
        )
      case (_, _) =>
        EmptyBatchInfo(recordsQueue.size)

    }

  }

  def dequeue(nonEmptyBatch: NonEmptySeq[RenderedRecord]): Unit = {
    recordsQueue.removeHeadWhile(b => nonEmptyBatch.toSeq.contains(b))
    ()
  }

}
