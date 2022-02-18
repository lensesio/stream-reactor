/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink.writer

import io.lenses.streamreactor.connect.aws.s3.model.{Offset, TopicPartition}
import org.apache.kafka.connect.data.Schema

case class CommitState(
                          topicPartition: TopicPartition,
                          createdTimestamp: Long,
                          committedOffset: Option[Offset],
                          lastFlushedTime: Option[Long],
                          recordCount: Long,
                          lastKnownFileSize: Long,
                          lastKnownSchema: Option[Schema],
                        ) {

  // lastKnownFileSize will come from the FormatWriter pointer
  def offsetChange(schema: Option[Schema], lastKnownFileSize: Long): CommitState = {
    copy(
      lastKnownFileSize = lastKnownFileSize,
      lastKnownSchema = schema,
      recordCount = recordCount + 1,
    )
  }

  def reset(): CommitState = {
    copy(
      lastKnownFileSize = 0L,
      recordCount = 0L,
      lastFlushedTime = Some(System.currentTimeMillis()),
    )
  }

  def withCommittedOffset(offset: Offset): CommitState = {
    copy(committedOffset = Some(offset))
  }

}

object CommitState {

  def apply(tp: TopicPartition, seekedOffset: Option[Offset]): CommitState = {
    CommitState(
      topicPartition = tp,
      createdTimestamp = System.currentTimeMillis(),
      committedOffset = seekedOffset,
      lastFlushedTime = None,
      recordCount = 0,
      lastKnownFileSize = 0,
      lastKnownSchema = None,
    )
  }
}
