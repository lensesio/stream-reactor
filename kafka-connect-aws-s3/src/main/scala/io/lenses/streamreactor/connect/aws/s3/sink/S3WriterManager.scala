
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

package io.lenses.streamreactor.connect.aws.s3.sink

import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.{MultipartBlobStoreOutputStream, S3Writer, S3WriterImpl, StorageInterface}
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.errors.ConnectException

import scala.collection.mutable

/**
  * Manages the lifecycle of [[S3Writer]] instances.
  *
  * A given sink may be writing to multiple locations (partitions), and therefore
  * it is convenient to extract this to another class.
  *
  * This class is not thread safe as it is not designed to be shared between concurrent
  * sinks, since file handles cannot be safely shared without considerable overhead.
  */
class S3WriterManager(formatWriterFn: (TopicPartition, Map[PartitionField, String]) => S3FormatWriter,
                      commitPolicyFn: Topic => CommitPolicy,
                      bucketAndPrefixFn: Topic => BucketAndPrefix,
                      fileNamingStrategyFn: Topic => S3FileNamingStrategy
                     )
                     (implicit storageInterface: StorageInterface) {

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  private val latestFileMap = mutable.Map[TopicPartition, (Offset, BucketAndPath)]()

  case class MapKey(topicPartition: TopicPartition, bucketAndPath: BucketAndPath)

  private val writers = scala.collection.mutable.Map.empty[MapKey, S3Writer]

  def commitAllWritersIfFlushRequired() = {
    val shouldFlush = writers.values.exists(_.shouldFlush)
    if (shouldFlush) commitAllWriters()
  }

  private def commitAllWriters(): Map[TopicPartition, Offset] = {

    logger.debug("Received call to S3WriterManager.commit")
    
    writers.keys.map(_.topicPartition)
      .toSet
      .map(commitTopicPartitionWriters)
      .toMap
  }

  private def commitTopicPartitionWriters(topicPartition: TopicPartition) = {

    val tpo = writers
      .filterKeys(_.topicPartition == topicPartition)
      .mapValues(_.commit())
      .values
      .maxBy(_.offset)

    (tpo.toTopicPartition, tpo.offset)
  }

  def open(partitions: Set[TopicPartition]): Map[TopicPartition, Offset] = {
    logger.debug("Received call to S3WriterManager.open")

    partitions.flatMap {
      case topicPartition: TopicPartition =>
        fileNamingStrategyFn(topicPartition.topic)
          .createOffsetSeeker(storageInterface)
          .seek(bucketAndPrefixFn(topicPartition.topic), topicPartition)
          .map {
            case (bucketAndPath, topicPartitionOffset) =>
              latestFileMap(topicPartition) = (topicPartitionOffset.offset, bucketAndPath)
              (topicPartition, topicPartitionOffset.offset)
          }
    }
      .toMap

  }

  def close(): Unit = {
    logger.debug("Received call to S3WriterManager.close")
    writers.values.foreach(_.close())
  }

  def write(topicPartitionOffset: TopicPartitionOffset, messageDetail: MessageDetail): Unit = {
    logger.debug(s"Received call to S3WriterManager.write")

    val newWriter = writer(topicPartitionOffset, messageDetail)

    val schema = messageDetail.valueSinkData.schema()
    if (schema.isDefined && newWriter.shouldRollover(schema.get)) {
      commitTopicPartitionWriters(topicPartitionOffset.toTopicPartition)
    }

    newWriter.write(messageDetail, topicPartitionOffset)

    if (newWriter.shouldFlush)
      commitTopicPartitionWriters(topicPartitionOffset.toTopicPartition)

  }

  /**
    * Returns a writer that can write records for a particular topic and partition.
    * The writer will create a file inside the given directory if there is no open writer.
    */
  private def writer(topicPartitionOffset: TopicPartitionOffset, messageDetail: MessageDetail): S3Writer = {
    val topicPartition = topicPartitionOffset.toTopicPartition
    val bucketAndPrefix = bucketAndPrefixFn(topicPartition.topic)
    val fileNamingStrategy: S3FileNamingStrategy = fileNamingStrategyFn(topicPartition.topic)

    val partitionValues = if (fileNamingStrategy.shouldProcessPartitionValues) fileNamingStrategy.processPartitionValues(messageDetail) else Map.empty[PartitionField, String]

    val tempBucketAndPath: BucketAndPath = fileNamingStrategy.stagingFilename(bucketAndPrefix, topicPartition, partitionValues)

    writers.getOrElseUpdate(MapKey(topicPartition, tempBucketAndPath), createWriter(bucketAndPrefix, topicPartitionOffset.toTopicPartition, partitionValues))
  }

  private def createWriter(bucketAndPrefix: BucketAndPrefix, topicPartition: TopicPartition, partitionValues: Map[PartitionField, String]): S3Writer = {

    logger.debug(s"Creating new writer for bucketAndPrefix [$bucketAndPrefix]")

    new S3WriterImpl(
      bucketAndPrefix,
      commitPolicyFn(topicPartition.topic),
      formatWriterFn,
      fileNamingStrategyFn(topicPartition.topic),
      partitionValues,
      latestFileMap
    )
  }

  def preCommit(
                 currentOffsets: Map[TopicPartition, OffsetAndMetadata]
               ): Map[TopicPartition, OffsetAndMetadata] = {
    logger.debug("Received call to S3WriterManager.preCommit")

    currentOffsets
      .flatMap {
        case (topicPartition, offsetAndMetadata) =>
          val candidateWriters = writers
            .filter {
              case (key, writer) => key.topicPartition == topicPartition && writer.getCommittedOffset.nonEmpty
            }
            .values
          if (candidateWriters.isEmpty) {
            None
          } else {
            Some(
              topicPartition,
              createOffsetAndMetadata(offsetAndMetadata, candidateWriters
                .maxBy(_.getCommittedOffset))
            )
          }
      }
  }

  private def createOffsetAndMetadata(offsetAndMetadata: OffsetAndMetadata, writer: S3Writer) = {
    new OffsetAndMetadata(
      writer.getCommittedOffset.get.value,
      offsetAndMetadata.leaderEpoch(),
      offsetAndMetadata.metadata()
    )
  }
}


object S3WriterManager {
  def from(config: S3SinkConfig)
          (implicit storageInterface: StorageInterface): S3WriterManager = {

    // TODO: make this configurable
    val MinAllowedMultipartSize: Int = 5242880

    val bucketAndPrefixFn: Topic => BucketAndPrefix = topic => config.bucketOptions.find(_.sourceTopic == topic.value)
      .getOrElse(throw new ConnectException(s"No bucket config for $topic")).bucketAndPrefix

    val commitPolicyFn: Topic => CommitPolicy = topic => config.bucketOptions.find(_.sourceTopic == topic.value) match {
      case Some(bucketOptions) => bucketOptions.commitPolicy
      case None => throw new IllegalArgumentException("Can't find commitPolicy in config")
    }

    val fileNamingStrategyFn: Topic => S3FileNamingStrategy = topic => config.bucketOptions
      .find(_.sourceTopic == topic.value) match {
      case Some(bucketOptions) => bucketOptions.fileNamingStrategy
      case None => throw new IllegalArgumentException("Can't find fileNamingStrategy in config")
    }

    val minAllowedMultipartSizeFn: () => Int = () => MinAllowedMultipartSize

    val outputStreamFn: (BucketAndPath, Int) => () => MultipartBlobStoreOutputStream = {
      (bucketAndPath, int) =>
        () => new MultipartBlobStoreOutputStream(bucketAndPath, int)
    }

    val formatWriterFn: (TopicPartition, Map[PartitionField, String]) => S3FormatWriter = (topicPartition, partitionValues) =>
      config.bucketOptions.find(_.sourceTopic == topicPartition.topic.value) match {
        case Some(bucketOptions) =>
          val fileNamingStrategy = fileNamingStrategyFn(topicPartition.topic)

          val path: BucketAndPath = fileNamingStrategy
            .stagingFilename(bucketOptions.bucketAndPrefix, topicPartition, partitionValues)
          val size: Int = minAllowedMultipartSizeFn()
          S3FormatWriter(bucketOptions.formatSelection, outputStreamFn(path, size))
        case None => throw new IllegalArgumentException("Can't find commitPolicy in config")
      }

    new S3WriterManager(
      formatWriterFn,
      commitPolicyFn,
      bucketAndPrefixFn,
      fileNamingStrategyFn
    )
  }
}
