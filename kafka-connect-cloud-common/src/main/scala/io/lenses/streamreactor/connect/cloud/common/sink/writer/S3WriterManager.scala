/*
 * Copyright 2017-2023 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.S3FormatWriter
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.sink
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.BatchS3SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalS3SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.data.Schema

import java.io.File
import scala.collection.immutable
import scala.collection.mutable
import scala.util.Try

case class MapKey(topicPartition: TopicPartition, partitionValues: immutable.Map[PartitionField, String])

/**
  * Manages the lifecycle of [[S3Writer]] instances.
  *
  * A given sink may be writing to multiple locations (partitions), and therefore
  * it is convenient to extract this to another class.
  *
  * This class is not thread safe as it is not designed to be shared between concurrent
  * sinks, since file handles cannot be safely shared without considerable overhead.
  */
class S3WriterManager(
  commitPolicyFn:    TopicPartition => Either[SinkError, CommitPolicy],
  bucketAndPrefixFn: TopicPartition => Either[SinkError, CloudLocation],
  keyNamerFn:        TopicPartition => Either[SinkError, KeyNamer],
  stagingFilenameFn: (TopicPartition, Map[PartitionField, String]) => Either[SinkError, File],
  finalFilenameFn:   (TopicPartition, Map[PartitionField, String], Offset) => Either[SinkError, CloudLocation],
  formatWriterFn:    (TopicPartition, File) => Either[SinkError, S3FormatWriter],
  indexManager:      IndexManager,
  transformerF:      MessageDetail => Either[RuntimeException, MessageDetail],
)(
  implicit
  connectorTaskId:  ConnectorTaskId,
  storageInterface: StorageInterface,
) extends StrictLogging {

  private val writers = mutable.Map.empty[MapKey, S3Writer]

  private val seekedOffsets = mutable.Map.empty[TopicPartition, Offset]

  def commitAllWritersIfFlushRequired(): Either[BatchS3SinkError, Unit] =
    if (writers.values.exists(_.shouldFlush)) {
      commitAllWriters()
    } else {
      ().asRight
    }

  private def commitAllWriters(): Either[BatchS3SinkError, Unit] = {
    logger.debug(s"[{}] Received call to S3WriterManager.commit", connectorTaskId.show)
    commitWritersWithFilter(_ => true)
  }

  def recommitPending(): Either[SinkError, Unit] = {
    logger.debug(s"[{}] Retry Pending", connectorTaskId.show)
    val result = commitWritersWithFilter(_._2.hasPendingUpload())
    logger.debug(s"[{}] Retry Pending Complete", connectorTaskId.show)
    result
  }

  private def commitWriters(topicPartition: TopicPartition): Either[BatchS3SinkError, Unit] =
    commitWritersWithFilter(_._1.topicPartition == topicPartition)

  private def commitWriters(writer: S3Writer, topicPartition: TopicPartition): Either[BatchS3SinkError, Unit] = {
    logger.debug(s"[{}] Received call to S3WriterManager.commitWritersWithFilter (w, tp {})",
                 connectorTaskId.show,
                 topicPartition,
    )
    if (writer.shouldFlush) {
      commitWritersWithFilter((kv: (MapKey, S3Writer)) => kv._1.topicPartition == topicPartition)
    } else {
      ().asRight
    }
  }

  private def commitWritersWithFilter(
    keyValueFilterFn: ((MapKey, S3Writer)) => Boolean,
  ): Either[BatchS3SinkError, Unit] = {

    logger.debug(s"[{}] Received call to S3WriterManager.commitWritersWithFilter (filter)", connectorTaskId.show)
    val writerCommitErrors = writers
      .filter(keyValueFilterFn)
      .view.mapValues(_.commit)
      .collect {
        case (_, Left(err)) => err
      }.toSet

    if (writerCommitErrors.nonEmpty) {
      sink.BatchS3SinkError(writerCommitErrors).asLeft
    } else {
      ().asRight
    }

  }

  def open(partitions: Set[TopicPartition]): Either[SinkError, Map[TopicPartition, Offset]] = {
    logger.debug(s"[{}] Received call to S3WriterManager.open", connectorTaskId.show)

    partitions
      .map(seekOffsetsForTopicPartition)
      .partitionMap(identity) match {
      case (throwables, _) if throwables.nonEmpty => BatchS3SinkError(throwables).asLeft
      case (_, offsets) =>
        val seeked = offsets.flatten.map(
          _.toTopicPartitionOffsetTuple,
        ).toMap
        seekedOffsets ++= seeked
        seeked.asRight
    }
  }

  private def seekOffsetsForTopicPartition(
    topicPartition: TopicPartition,
  ): Either[SinkError, Option[TopicPartitionOffset]] = {
    logger.debug(s"[{}] seekOffsetsForTopicPartition {}", connectorTaskId.show, topicPartition)
    for {
      keyNamer        <- keyNamerFn(topicPartition)
      bucketAndPrefix <- bucketAndPrefixFn(topicPartition)
      offset          <- indexManager.seek(topicPartition, bucketAndPrefix.bucket)
    } yield offset
  }

  def close(): Unit = {
    logger.debug(s"[{}] Received call to S3WriterManager.close", connectorTaskId.show)
    writers.values.foreach(_.close())
    writers.clear()
  }

  def write(topicPartitionOffset: TopicPartitionOffset, messageDetail: MessageDetail): Either[SinkError, Unit] = {

    logger.debug(
      s"[${connectorTaskId.show}] Received call to S3WriterManager.write for ${topicPartitionOffset.topic}-${topicPartitionOffset.partition}:${topicPartitionOffset.offset}",
    )
    for {
      writer    <- writer(topicPartitionOffset.toTopicPartition, messageDetail)
      shouldSkip = writer.shouldSkip(topicPartitionOffset.offset)
      resultIfNotSkipped <- if (!shouldSkip) {
        transformerF(messageDetail).leftMap(ex =>
          FatalS3SinkError(ex.getMessage, ex, topicPartitionOffset.toTopicPartition),
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
    writer:               S3Writer,
  ): Either[SinkError, Unit] =
    for {
      // commitException can not be recovered from
      _ <- rollOverTopicPartitionWriters(writer, topicPartitionOffset.toTopicPartition, messageDetail)
      // a processErr can potentially be recovered from in the next iteration.  Can be due to network problems
      _         <- writer.write(messageDetail)
      commitRes <- commitWriters(writer, topicPartitionOffset.toTopicPartition)
    } yield commitRes

  private def rollOverTopicPartitionWriters(
    s3Writer:       S3Writer,
    topicPartition: TopicPartition,
    message:        MessageDetail,
  ): Either[BatchS3SinkError, Unit] =
    //TODO: fix this; it cannot always be VALUE and it depends on writer requiring a roll over to new file
    message.value.schema() match {
      case Some(value: Schema) if s3Writer.shouldRollover(value) => commitWriters(topicPartition)
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
  private def writer(topicPartition: TopicPartition, messageDetail: MessageDetail): Either[SinkError, S3Writer] =
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
  ): Either[SinkError, S3Writer] = {
    logger.debug(s"[${connectorTaskId.show}] Creating new writer for bucketAndPrefix:$bucketAndPrefix")
    for {
      commitPolicy <- commitPolicyFn(topicPartition)
    } yield {
      new S3Writer(
        topicPartition,
        commitPolicy,
        indexManager,
        () => stagingFilenameFn(topicPartition, partitionValues),
        finalFilenameFn.curried(topicPartition)(partitionValues),
        formatWriterFn.curried(topicPartition),
        seekedOffsets.get(topicPartition),
      )
    }
  }

  def preCommit(
    currentOffsets: immutable.Map[TopicPartition, OffsetAndMetadata],
  ): immutable.Map[TopicPartition, OffsetAndMetadata] =
    currentOffsets
      .map {
        case (tp, offAndMeta) => (tp, getOffsetAndMeta(tp, offAndMeta))
      }
      .collect {
        case (k, v) if v.nonEmpty => (k, v.get)
      }

  private def writerForTopicPartitionWithMaxOffset(topicPartition: TopicPartition): Option[S3Writer] =
    Try(
      writers.collect {
        case (key, writer) if key.topicPartition == topicPartition && writer.getCommittedOffset.nonEmpty => writer
      }
        .maxBy(_.getCommittedOffset),
    ).toOption

  private def getOffsetAndMeta(topicPartition: TopicPartition, offsetAndMetadata: OffsetAndMetadata) =
    for {
      writer <- writerForTopicPartitionWithMaxOffset(topicPartition)
      offsetAndMeta <- Try {
        new OffsetAndMetadata(
          writer.getCommittedOffset.get.value,
          offsetAndMetadata.leaderEpoch(),
          offsetAndMetadata.metadata(),
        )
      }.toOption
    } yield offsetAndMeta

  def cleanUp(topicPartition: TopicPartition): Unit =
    writers
      .view.filterKeys(mapKey =>
        mapKey
          .topicPartition == topicPartition,
      )
      .keys
      .foreach(writers.remove)

}
