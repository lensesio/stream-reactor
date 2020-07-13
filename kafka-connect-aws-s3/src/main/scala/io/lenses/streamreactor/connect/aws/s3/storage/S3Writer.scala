
/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.storage

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.{CommitContext, CommitPolicy, S3FileNamingStrategy}
import org.apache.kafka.connect.data.Schema

trait S3Writer {
  def shouldFlush: Boolean

  def close(): Unit

  def write(messageDetail: MessageDetail, tpo: TopicPartitionOffset): Unit

  def getCommittedOffset: Option[Offset]

  def shouldRollover(schema: Schema): Boolean

  def commit(): TopicPartitionOffset
}


case class S3WriterState(
                          topicPartition: TopicPartition,
                          offset: Offset,
                          committedOffset: Option[Offset],
                          createdTimestamp: Long = System.currentTimeMillis(),
                          recordCount: Long = 0,
                          lastKnownFileSize: Long = 0,
                          lastKnownSchema: Option[Schema] = None
                        )


class S3WriterImpl(
                    bucketAndPrefix: BucketAndPrefix,
                    commitPolicy: CommitPolicy,
                    formatWriterFn: (TopicPartition, Map[PartitionField, String]) => S3FormatWriter,
                    fileNamingStrategy: S3FileNamingStrategy,
                    partitionValues: Map[PartitionField, String]
                  )(implicit storageInterface: StorageInterface) extends S3Writer with LazyLogging {

  private var internalState: S3WriterState = _

  private var formatWriter: S3FormatWriter = _

  override def write(messageDetail: MessageDetail, tpo: TopicPartitionOffset): Unit = {

    if (formatWriter == null) {
      formatWriter = formatWriterFn(tpo.toTopicPartition, partitionValues)
    }

    logger.debug(s"S3Writer.write: Internal state: $internalState")

    if (internalState == null) {
      internalState = S3WriterState(
        tpo.toTopicPartition,
        tpo.offset,
        None,
        createdTimestamp = System.currentTimeMillis(),
      )
    }

    // appends to output stream
    formatWriter.write(messageDetail.keySinkData, messageDetail.valueSinkData, tpo.topic)

    internalState = internalState.copy(
      lastKnownFileSize = formatWriter.getPointer,
      lastKnownSchema = messageDetail.valueSinkData.schema(),
      recordCount = internalState.recordCount + 1,
      offset = tpo.offset
    )
  }

  override def shouldRollover(schema: Schema): Boolean = {
    rolloverOnSchemaChange &&
      internalState != null &&
      schemaHasChanged(schema)
  }

  private def schemaHasChanged(schema: Schema) = {
    internalState.lastKnownSchema.isEmpty ||
      internalState.lastKnownSchema.get != schema
  }

  private def rolloverOnSchemaChange = {
    formatWriter != null &&
      formatWriter.rolloverFileOnSchemaChange()
  }

  override def commit(): TopicPartitionOffset = {
    logger.debug(s"S3Writer - Commit")
    val topicPartitionOffset = TopicPartitionOffset(
      internalState.topicPartition.topic,
      internalState.topicPartition.partition,
      internalState.offset)

    formatWriter.close()
    if (formatWriter.getOutstandingRename) {
      renameFile(topicPartitionOffset, partitionValues)
    }

    resetState(topicPartitionOffset)

    topicPartitionOffset
  }

  private def renameFile(topicPartitionOffset: TopicPartitionOffset, partitionValues: Map[PartitionField, String]): Unit = {
    val originalFilename = fileNamingStrategy.stagingFilename(
      bucketAndPrefix,
      topicPartitionOffset.toTopicPartition,
      partitionValues
    )
    val finalFilename = fileNamingStrategy.finalFilename(
      bucketAndPrefix,
      topicPartitionOffset,
      partitionValues
    )
    storageInterface.rename(originalFilename, finalFilename)
  }

  private def resetState(topicPartitionOffset: TopicPartitionOffset): Unit = {
    logger.debug(s"S3Writer.resetState: Resetting state: $internalState")

    internalState = internalState.copy(
      committedOffset = Some(topicPartitionOffset.offset),
      lastKnownFileSize = 0.toLong,
      recordCount = 0.toLong
    )

    formatWriter = formatWriterFn(topicPartitionOffset.toTopicPartition, partitionValues)

    logger.debug(s"S3Writer.resetState: New internal state: $internalState")
  }

  override def close(): Unit = storageInterface.close()

  override def getCommittedOffset: Option[Offset] = internalState.committedOffset

  override def shouldFlush: Boolean = {

    val commitContext = CommitContext(
      TopicPartitionOffset(internalState.topicPartition.topic, internalState.topicPartition.partition, internalState.offset),
      internalState.recordCount,
      internalState.lastKnownFileSize,
      internalState.createdTimestamp
    )

    commitPolicy.shouldFlush(commitContext)
  }

}
