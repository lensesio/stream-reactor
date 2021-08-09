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

package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.{Offset, TopicPartition, TopicPartitionOffset}
import org.apache.kafka.connect.data.Schema

object S3WriterState {

  def apply(tpo: TopicPartitionOffset, formatWriter: S3FormatWriter): S3WriterState = {
    S3WriterState(
      topicPartition = tpo.toTopicPartition,
      offset = tpo.offset,
      committedOffset = None,
      createdTimestamp = System.currentTimeMillis(),
      formatWriter = Some(formatWriter),
    )
  }
}

case class S3WriterState(topicPartition: TopicPartition,
                         offset: Offset,
                         committedOffset: Option[Offset],
                         createdTimestamp: Long = System.currentTimeMillis(),
                         lastFlushedQueuedTimestamp: Option[Long] = None,
                         recordCount: Long = 0,
                         lastKnownFileSize: Long = 0,
                         lastKnownSchema: Option[Schema] = None,
                         formatWriter: Option[S3FormatWriter]
                        ) {

  def show(): String = {
    s"${topicPartition.topic}-${topicPartition.partition}:${offset.value} ${committedOffset.map(_.value.toString).getOrElse("-")} $recordCount $lastKnownFileSize ${lastFlushedQueuedTimestamp.map(_.toString).getOrElse("-")}"
  }

  def reset(): S3WriterState = {
    copy(
      lastKnownFileSize = 0L,
      recordCount = 0L,
      formatWriter = None,
      lastFlushedQueuedTimestamp = Some(System.currentTimeMillis())
    )
  }

  def withCommittedOffset(offset: Offset): S3WriterState = {
    copy(committedOffset = Some(offset))
  }

  def withFormatWriter(fW: S3FormatWriter): S3WriterState = {
    copy(formatWriter = Some(fW))
  }

  def offsetChange(newOffset: Offset, schema: Option[Schema]): S3WriterState = {
    copy(
      lastKnownFileSize = formatWriter.getOrElse(throw new IllegalStateException("No format writer exists")).getPointer,
      lastKnownSchema = schema,
      recordCount = recordCount + 1,
      offset = newOffset
    )
  }

  def topicPartitionOffset = topicPartition.withOffset(offset)

  def getFormatWriter: Either[ProcessorException, S3FormatWriter] = {
    formatWriter
      .getOrElse(
        return ProcessorException("No format writer exists").asLeft
      ).asRight
  }

}
