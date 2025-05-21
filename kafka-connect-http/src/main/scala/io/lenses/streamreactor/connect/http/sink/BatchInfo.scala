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
package io.lenses.streamreactor.connect.http.sink

import cats.data.NonEmptySeq
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord

/**
 * \`BatchInfo\` represents information about a batch of records in the queue.
 */
sealed trait BatchInfo {

  /**
   * Get the total number of records in the queue.
   * @return queueSize the total number of records in the queue.
   */
  def queueSize: Int
}

/**
 * EmptyBatchInfo may be returned in the following circumstances:
 *  - there is no data in the queue to process
 *  - there is data in the queue however not enough for a batch (the commit has not been triggered per the CommitPolicy)
 * @param queueSize the total number of records in the queue.
 */
case class EmptyBatchInfo(queueSize: Int) extends BatchInfo

/**
 * NonEmptyBatchInfo is returned for an actual batch of data.
 * @param batch the RenderedRecords to return
 * @param updatedCommitContext the amended commit context
 * @param queueSize the total number of records in the queue.
 */
case class NonEmptyBatchInfo(
  batch:                NonEmptySeq[RenderedRecord],
  updatedCommitContext: HttpCommitContext,
  queueSize:            Int,
) extends BatchInfo
