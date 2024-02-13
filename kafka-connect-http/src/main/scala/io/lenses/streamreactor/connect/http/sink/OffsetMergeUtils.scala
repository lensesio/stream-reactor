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

import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord

object OffsetMergeUtils {

  def createCommitContextForEvaluation(
    batch:                Seq[RenderedRecord],
    currentCommitContext: HttpCommitContext,
  ): HttpCommitContext = {
    val count          = batch.size.toLong
    val fileSize       = batch.map(_.recordRendered.length).sum.toLong
    val highestOffsets = maxOffsets(batch)
    currentCommitContext match {
      case httpCommitContext @ HttpCommitContext(_, httpCommittedOffsets, _, _, _, _, _) =>
        httpCommitContext.copy(
          committedOffsets = mergeOffsets(httpCommittedOffsets, highestOffsets),
          count            = count,
          fileSize         = fileSize,
        )
    }
  }

  def updateCommitContextPostCommit(
    currentCommitContext: HttpCommitContext,
  ): HttpCommitContext =
    currentCommitContext.copy(
      count                = 0L,
      fileSize             = 0L,
      lastFlushedTimestamp = System.currentTimeMillis().some,
    )

  private def maxOffsets(batch: Seq[RenderedRecord]): Map[TopicPartition, Offset] =
    batch
      .map(_.topicPartitionOffset.toTopicPartitionOffsetTuple)
      .groupBy { case (partition, _) => partition }
      .map {
        case (_, value) => value.maxBy {
            case (_, offset) => offset
          }
      }

  def mergeOffsets(
    committedOffsets: Map[TopicPartition, Offset],
    maxOffsets:       Map[TopicPartition, Offset],
  ): Map[TopicPartition, Offset] = {
    val asSet = committedOffsets.toSet ++ maxOffsets.toSet
    asSet.groupBy {
      case (partition, _) => partition
    }.map {
      case (_, value) => value.maxBy {
          case (_, offset) => offset
        }
    }
  }
}
