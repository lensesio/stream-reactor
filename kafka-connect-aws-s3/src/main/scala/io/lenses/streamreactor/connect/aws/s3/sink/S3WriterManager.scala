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
package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.aws.s3.formats.writer.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model.Offset.orderingByOffsetValue
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.aws.s3.sink.config.PartitionField
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.sink.config.SinkBucketOptions
import io.lenses.streamreactor.connect.aws.s3.sink.seek._
import io.lenses.streamreactor.connect.aws.s3.sink.transformers.TopicsTransformers
import io.lenses.streamreactor.connect.aws.s3.sink.writer.S3Writer
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
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
  commitPolicyFn:       TopicPartition => Either[SinkError, CommitPolicy],
  bucketAndPrefixFn:    TopicPartition => Either[SinkError, S3Location],
  fileNamingStrategyFn: TopicPartition => Either[SinkError, S3FileNamingStrategy],
  stagingFilenameFn:    (TopicPartition, Map[PartitionField, String]) => Either[SinkError, File],
  finalFilenameFn:      (TopicPartition, Map[PartitionField, String], Offset) => Either[SinkError, S3Location],
  formatWriterFn:       (TopicPartition, File) => Either[SinkError, S3FormatWriter],
  indexManager:         IndexManager,
  transformerF:         MessageDetail => Either[RuntimeException, MessageDetail],
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
    logger.trace(s"[{}] Retry Pending", connectorTaskId.show)
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
      BatchS3SinkError(writerCommitErrors).asLeft
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
      fileNamingStrategy <- fileNamingStrategyFn(topicPartition)
      bucketAndPrefix    <- bucketAndPrefixFn(topicPartition)
      offset             <- indexManager.seek(topicPartition, fileNamingStrategy, bucketAndPrefix.bucket)
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
    messageDetail:      MessageDetail,
    fileNamingStrategy: S3FileNamingStrategy,
    topicPartition:     TopicPartition,
  ): Either[SinkError, immutable.Map[PartitionField, String]] =
    if (fileNamingStrategy.shouldProcessPartitionValues) {
      fileNamingStrategy.processPartitionValues(messageDetail, topicPartition)
    } else {
      Map.empty[PartitionField, String].asRight
    }

  /**
    * Returns a writer that can write records for a particular topic and partition.
    * The writer will create a file inside the given directory if there is no open writer.
    */
  private def writer(topicPartition: TopicPartition, messageDetail: MessageDetail): Either[SinkError, S3Writer] =
    for {
      bucketAndPrefix    <- bucketAndPrefixFn(topicPartition)
      fileNamingStrategy <- fileNamingStrategyFn(topicPartition)
      partitionValues    <- processPartitionValues(messageDetail, fileNamingStrategy, topicPartition)
      key                 = MapKey(topicPartition, partitionValues)
      maybeWriter         = writers.get(key)
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
    bucketAndPrefix: S3Location,
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

object S3WriterManager extends LazyLogging {

  def from(
    config: S3SinkConfig,
  )(
    implicit
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface,
  ): S3WriterManager = {

    val bucketAndPrefixFn: TopicPartition => Either[SinkError, S3Location] = topicPartition => {
      bucketOptsForTopic(config, topicPartition.topic) match {
        case Some(sBO) => sBO.bucketAndPrefix.asRight
        case None      => FatalS3SinkError.apply(s"No bucket config for ${topicPartition.topic}", topicPartition).asLeft
      }
    }

    val commitPolicyFn: TopicPartition => Either[SinkError, CommitPolicy] = topicPartition =>
      bucketOptsForTopic(config, topicPartition.topic) match {
        case Some(bucketOptions) => bucketOptions.commitPolicy.asRight
        case None                => FatalS3SinkError("Can't find commitPolicy in config", topicPartition).asLeft
      }

    val fileNamingStrategyFn: TopicPartition => Either[SinkError, S3FileNamingStrategy] = topicPartition =>
      bucketOptsForTopic(config, topicPartition.topic) match {
        case Some(bucketOptions) => bucketOptions.fileNamingStrategy.asRight
        case None                => FatalS3SinkError("Can't find fileNamingStrategy in config", topicPartition).asLeft
      }

    val stagingFilenameFn: (TopicPartition, immutable.Map[PartitionField, String]) => Either[SinkError, File] =
      (topicPartition, partitionValues) =>
        bucketOptsForTopic(config, topicPartition.topic) match {
          case Some(bucketOptions) =>
            for {
              fileNamingStrategy <- fileNamingStrategyFn(topicPartition)
              stagingFilename <- fileNamingStrategy.stagingFile(bucketOptions.localStagingArea.dir,
                                                                bucketOptions.bucketAndPrefix,
                                                                topicPartition,
                                                                partitionValues,
              )
            } yield stagingFilename
          case None => FatalS3SinkError("Can't find fileNamingStrategy in config", topicPartition).asLeft
        }

    val finalFilenameFn: (
      TopicPartition,
      immutable.Map[PartitionField, String],
      Offset,
    ) => Either[SinkError, S3Location] = (topicPartition, partitionValues, offset) =>
      bucketOptsForTopic(config, topicPartition.topic) match {
        case Some(bucketOptions) =>
          for {
            fileNamingStrategy <- fileNamingStrategyFn(topicPartition)
            stagingFilename <- fileNamingStrategy.finalFilename(bucketOptions.bucketAndPrefix,
                                                                topicPartition.withOffset(offset),
                                                                partitionValues,
            )
          } yield stagingFilename
        case None => FatalS3SinkError("Can't find fileNamingStrategy in config", topicPartition).asLeft
      }

    val formatWriterFn: (TopicPartition, File) => Either[SinkError, S3FormatWriter] =
      (topicPartition: TopicPartition, stagingFilename) =>
        bucketOptsForTopic(config, topicPartition.topic) match {
          case Some(bucketOptions) =>
            for {
              formatWriter <- S3FormatWriter(
                bucketOptions.formatSelection,
                stagingFilename,
                topicPartition,
              )(config.compressionCodec)
            } yield formatWriter
          case None => FatalS3SinkError("Can't find format choice in config", topicPartition).asLeft
        }

    val indexManager = new IndexManager(config.offsetSeekerOptions.maxIndexFiles)

    val transformers = TopicsTransformers.from(config.bucketOptions)
    new S3WriterManager(
      commitPolicyFn,
      bucketAndPrefixFn,
      fileNamingStrategyFn,
      stagingFilenameFn,
      finalFilenameFn,
      formatWriterFn,
      indexManager,
      transformers.transform,
    )
  }

  private def bucketOptsForTopic(config: S3SinkConfig, topic: Topic): Option[SinkBucketOptions] =
    config.bucketOptions.find(bo => bo.sourceTopic.isEmpty || bo.sourceTopic.contains(topic.value))

}
