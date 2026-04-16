/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.sink.writer

import cats.implicits._
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.data.Schema

import java.io.File
import scala.collection.immutable
import scala.collection.mutable

case class MapKey(topicPartition: TopicPartition, partitionValues: immutable.Map[PartitionField, String])

/**
 * Manages the lifecycle of [[Writer]] instances.
 *
 * A given sink may be writing to multiple locations (partitions), and therefore
 * it is convenient to extract this to another class.
 *
 * This class is not thread safe as it is not designed to be shared between concurrent
 * sinks, since file handles cannot be safely shared without considerable overhead.
 */
class WriterManager[SM <: FileMetadata](
  commitPolicyFn:              TopicPartition => Either[SinkError, CommitPolicy],
  bucketAndPrefixFn:           TopicPartition => Either[SinkError, CloudLocation],
  keyNamerFn:                  TopicPartition => Either[SinkError, KeyNamer],
  stagingFilenameFn:           (TopicPartition, Map[PartitionField, String]) => Either[SinkError, File],
  objKeyBuilderFn:             (TopicPartition, Map[PartitionField, String]) => ObjectKeyBuilder,
  formatWriterFn:              (TopicPartition, File) => Either[SinkError, FormatWriter],
  indexManager:                IndexManager,
  transformerF:                MessageDetail => Either[RuntimeException, MessageDetail],
  schemaChangeDetector:        SchemaChangeDetector,
  skipNullValues:              Boolean,
  pendingOperationsProcessors: PendingOperationsProcessors,
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends StrictLogging {

  private val writers             = mutable.Map.empty[MapKey, Writer[SM]]
  private val writerCommitManager = new WriterCommitManager[SM](() => writers.toMap)

  def recommitPending(): Either[SinkError, Unit] = {
    logger.debug(s"[{}] Retry Pending", connectorTaskId.show)
    val result = writerCommitManager.commitPending()
    logger.debug(s"[{}] Retry Pending Complete", connectorTaskId.show)
    result
  }

  def commitFlushableWriters(): Either[BatchCloudSinkError, Unit] = {
    logger.debug(s"[{}] Received call to WriterManager.commitFlushableWriters", connectorTaskId.show)
    writerCommitManager.commitFlushableWriters()
  }

  def close(): Unit = {
    logger.debug(s"[{}] Received call to WriterManager.close", connectorTaskId.show)
    writers.values.foreach(_.close())
    writers.clear()
  }

  def write(topicPartitionOffset: TopicPartitionOffset, messageDetail: MessageDetail): Either[SinkError, Unit] = {

    logger.debug(
      s"[${connectorTaskId.show}] Received call to WriterManager.write for ${topicPartitionOffset.topic}-${topicPartitionOffset.partition}:${topicPartitionOffset.offset}",
    )
    for {
      writer    <- writer(topicPartitionOffset.toTopicPartition, messageDetail)
      shouldSkip = writer.shouldSkip(topicPartitionOffset.offset)
      resultIfNotSkipped <-
        if (!shouldSkip) {
          transformerF(messageDetail).leftMap(ex =>
            new FatalCloudSinkError(ex.getMessage, ex.some, topicPartitionOffset.toTopicPartition),
          ).flatMap { transformed =>
            writeAndCommit(topicPartitionOffset, transformed, writer)
          }
        } else {
          ().asRight
        }
    } yield resultIfNotSkipped
  }

  private def writeAndCommit(
    topicPartitionOffset: TopicPartitionOffset,
    messageDetail:        MessageDetail,
    writer:               Writer[SM],
  ): Either[SinkError, Unit] =
    for {
      // commitException can not be recovered from
      _ <- rollOverTopicPartitionWriters(writer, topicPartitionOffset.toTopicPartition, messageDetail)
      // a processErr can potentially be recovered from in the next iteration.  Can be due to network problems
      _         <- writer.write(messageDetail)
      commitRes <- writerCommitManager.commitFlushableWritersForTopicPartition(topicPartitionOffset.toTopicPartition)
    } yield commitRes

  private def rollOverTopicPartitionWriters(
    writer:         Writer[SM],
    topicPartition: TopicPartition,
    message:        MessageDetail,
  ): Either[BatchCloudSinkError, Unit] =
    //TODO: fix this; it cannot always be VALUE and it depends on writer requiring a roll over to new file
    message.value.schema() match {
      case Some(value: Schema) if writer.shouldRollover(value) =>
        writerCommitManager.commitForTopicPartition(topicPartition)
      case _ => ().asRight
    }

  private def processPartitionValues(
    messageDetail:  MessageDetail,
    keyNamer:       KeyNamer,
    topicPartition: TopicPartition,
  ): Either[SinkError, immutable.Map[PartitionField, String]] =
    keyNamer.processPartitionValues(messageDetail, topicPartition)

  /**
   * Returns a writer that can write records for a particular topic and partition.
   * The writer will create a file inside the given directory if there is no open writer.
   */
  private def writer(topicPartition: TopicPartition, messageDetail: MessageDetail): Either[SinkError, Writer[SM]] =
    for {
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      keyNamer        <- keyNamerFn(topicPartition)
      partitionValues <- processPartitionValues(messageDetail, keyNamer, topicPartition)
      key              = MapKey(topicPartition, partitionValues)
      maybeWriter      = writers.get(key)
      writer <- maybeWriter match {
        case Some(w) => w.asRight
        case None =>
          createWriter(bucketAndPrefix, topicPartition, partitionValues)
            .map { w =>
              writers.put(key, w)
              w
            }
      }
    } yield writer

  private def createWriter(
    bucketAndPrefix: CloudLocation,
    topicPartition:  TopicPartition,
    partitionValues: Map[PartitionField, String],
  ): Either[SinkError, Writer[SM]] = {
    logger.debug(s"[${connectorTaskId.show}] Creating new writer for bucketAndPrefix:$bucketAndPrefix")
    for {
      commitPolicy <- commitPolicyFn(topicPartition)
    } yield {
      new Writer(
        topicPartition,
        commitPolicy,
        indexManager,
        () => stagingFilenameFn(topicPartition, partitionValues),
        objKeyBuilderFn(topicPartition, partitionValues),
        formatWriterFn.curried(topicPartition),
        schemaChangeDetector,
        pendingOperationsProcessors,
      )
    }
  }

  def preCommit(
    currentOffsets: immutable.Map[TopicPartition, OffsetAndMetadata],
  ): immutable.Map[TopicPartition, OffsetAndMetadata] =
    currentOffsets
      .flatMap { case (tp, offAndMeta) =>
        getOffsetAndMeta(tp, offAndMeta).map(tp -> _)
      }

  private def writerForTopicPartitionWithMaxOffset(topicPartition: TopicPartition): Option[Writer[SM]] =
    // Collect writers for the topic-partition that have a committed offset, convert to Seq and
    // pick the writer with the highest committed offset value using maxByOption (Scala 2.13)
    writers
      .collect {
        case (key, writer) if key.topicPartition == topicPartition && writer.getCommittedOffset.nonEmpty => writer
      }
      .toSeq
      .maxByOption(_.getCommittedOffset.get.value)

  private def getOffsetAndMeta(
    topicPartition:    TopicPartition,
    offsetAndMetadata: OffsetAndMetadata,
  ): Option[OffsetAndMetadata] =
    // Compose Options functionally: find the writer with max offset, then build the new OffsetAndMetadata
    for {
      writer    <- writerForTopicPartitionWithMaxOffset(topicPartition)
      committed <- writer.getCommittedOffset
    } yield new OffsetAndMetadata(
      // kafka last offset for a partition is the last committed + 1
      // Therefore the connector should report last committed + 1 or it will lead to a constant lag og 1
      // which might confuse the users.
      committed.value + 1,
      offsetAndMetadata.leaderEpoch(),
      offsetAndMetadata.metadata(),
    )

  def cleanUp(topicPartition: TopicPartition): Unit =
    writers
      .view.filterKeys(mapKey =>
        mapKey
          .topicPartition == topicPartition,
      )
      .keys
      .foreach(writers.remove)

  def shouldSkipNullValues(): Boolean = skipNullValues

}
