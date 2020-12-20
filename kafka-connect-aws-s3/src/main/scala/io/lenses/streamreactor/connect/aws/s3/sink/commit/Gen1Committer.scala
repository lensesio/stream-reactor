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

package io.lenses.streamreactor.connect.aws.s3.sink.commit

import io.lenses.streamreactor.connect.aws.s3.model.BucketAndPath
import io.lenses.streamreactor.connect.aws.s3.model.BucketAndPrefix
import io.lenses.streamreactor.connect.aws.s3.model.Offset
import io.lenses.streamreactor.connect.aws.s3.model.PartitionField
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartition
import io.lenses.streamreactor.connect.aws.s3.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.aws.s3.sink.CommittedFileName
import io.lenses.streamreactor.connect.aws.s3.sink.S3FileNamingStrategy
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.storage.Storage
import org.apache.kafka.connect.errors.ConnectException

import scala.util.control.NonFatal

class Gen1Committer(storage: Storage,
                    fileNamingStrategyFn: Topic => S3FileNamingStrategy,
                    bucketAndPrefixFn: Topic => BucketAndPrefix) extends Committer {
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass.getName)

  override def latest(partitions: Set[TopicPartition]): Map[TopicPartition, Offset] = {
    partitions.collect {
      case topicPartition: TopicPartition =>
        val fileNamingStrategy = fileNamingStrategyFn(topicPartition.topic)
        val bucketAndPrefix = bucketAndPrefixFn(topicPartition.topic)
        val topicPartitionPrefix = fileNamingStrategy.topicPartitionPrefix(bucketAndPrefix, topicPartition)
        val seekResult = seek(topicPartitionPrefix, fileNamingStrategy)
          seekResult.find(_.toTopicPartition == topicPartition) match {
          case Some(topicPartitionOffset) => Some(topicPartition, topicPartitionOffset.offset)
          case None => None
        }
    }.flatten.toMap
  }

  private def seek(bucketAndPath: BucketAndPath,
                   fileNamingStrategy: S3FileNamingStrategy): Set[TopicPartitionOffset] = {
    try {
      // the path may not have been created, in which case we have no offsets defined
      if (storage.pathExists(bucketAndPath)) {
        val listOfFilesInBucketTopicPartition = storage.list(bucketAndPath)

        listOfFilesInBucketTopicPartition.flatMap(CommittedFileName.from(_, fileNamingStrategy))
          .filter(_.format == fileNamingStrategy.getFormat)
          .map(file => TopicPartitionOffset(file.topic, file.partition, file.offset))
          .groupBy(_.toTopicPartition)
          .map { case (tp, tpo) =>
            tp.withOffset(tpo.maxBy(_.offset.value).offset)
          }.toSet
      } else {
        Set.empty
      }
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error seeking bucket/prefix $bucketAndPath", e)
        throw e
    }
  }
  override def commit(bucketAndPrefix: BucketAndPrefix,
                      topicPartitionOffset: TopicPartitionOffset,
                      partitionValues: Map[PartitionField, String]): Unit = {
    val fileNamingStrategy = fileNamingStrategyFn(topicPartitionOffset.topic)
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
    storage.rename(originalFilename, finalFilename)
  }
}

object Gen1Committer {
  def from(config: S3SinkConfig, storage: Storage): Gen1Committer = {
    val bucketAndPrefixFn: Topic => BucketAndPrefix = topic => config.bucketOptions.find(_.sourceTopic == topic.value)
      .getOrElse(throw new ConnectException(s"No bucket config for $topic")).bucketAndPrefix

    val fileNamingStrategyFn: Topic => S3FileNamingStrategy = topic => config.bucketOptions
      .find(_.sourceTopic == topic.value) match {
      case Some(bucketOptions) => bucketOptions.fileNamingStrategy
      case None => throw new IllegalArgumentException("Can't find fileNamingStrategy in config")
    }

    new Gen1Committer(storage, fileNamingStrategyFn, bucketAndPrefixFn)
  }
}