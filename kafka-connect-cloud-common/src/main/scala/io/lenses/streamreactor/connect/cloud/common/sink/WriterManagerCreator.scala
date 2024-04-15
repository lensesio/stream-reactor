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
package io.lenses.streamreactor.connect.cloud.common.sink

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.formats
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.CloudSinkBucketOptions
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.naming.FinalFileNameBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.transformers.TopicsTransformers
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface

import java.io.File
import scala.collection.immutable

class WriterManagerCreator[MD <: FileMetadata, SC <: CloudSinkConfig] extends LazyLogging {

  def from(
    config: SC,
  )(
    implicit
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface[MD],
  ): WriterManager[MD] = {

    val bucketAndPrefixFn: TopicPartition => Either[SinkError, CloudLocation] = topicPartition => {
      bucketOptsForTopic(config, topicPartition.topic) match {
        case Some(sBO) => sBO.bucketAndPrefix.asRight
        case None      => fatalErrorTopicNotConfigured(topicPartition).asLeft
      }
    }

    val commitPolicyFn: TopicPartition => Either[SinkError, CommitPolicy] = topicPartition =>
      bucketOptsForTopic(config, topicPartition.topic) match {
        case Some(bucketOptions) => bucketOptions.commitPolicy.asRight
        case None                => fatalErrorTopicNotConfigured(topicPartition).asLeft
      }

    val keyNamerFn: TopicPartition => Either[SinkError, KeyNamer] = topicPartition =>
      bucketOptsForTopic(config, topicPartition.topic) match {
        case Some(bucketOptions) => bucketOptions.keyNamer.asRight
        case None                => fatalErrorTopicNotConfigured(topicPartition).asLeft
      }

    val stagingFilenameFn: (TopicPartition, immutable.Map[PartitionField, String]) => Either[SinkError, File] =
      (topicPartition, partitionValues) =>
        bucketOptsForTopic(config, topicPartition.topic) match {
          case Some(bucketOptions) =>
            for {
              keyNamer <- keyNamerFn(topicPartition)
              stagingFilename <- keyNamer.stagingFile(bucketOptions.localStagingArea.dir,
                                                      bucketOptions.bucketAndPrefix,
                                                      topicPartition,
                                                      partitionValues,
              )
            } yield stagingFilename
          case None => fatalErrorTopicNotConfigured(topicPartition).asLeft
        }

    val finalFilenameFn: (
      TopicPartition,
      immutable.Map[PartitionField, String],
    ) => FinalFileNameBuilder = (topicPartition, partitionValues) =>
      (offset: Offset, earliestRecordTimestamp: Long) => {
        bucketOptsForTopic(config, topicPartition.topic) match {
          case Some(bucketOptions) =>
            for {
              keyNamer <- keyNamerFn(topicPartition)
              finalFilename <- keyNamer.finalFilename(bucketOptions.bucketAndPrefix,
                                                      topicPartition.withOffset(offset),
                                                      partitionValues,
                                                      earliestRecordTimestamp,
              )
            } yield finalFilename
          case None => fatalErrorTopicNotConfigured(topicPartition).asLeft
        }

      }

    val formatWriterFn: (TopicPartition, File) => Either[SinkError, FormatWriter] =
      (topicPartition: TopicPartition, stagingFilename) =>
        bucketOptsForTopic(config, topicPartition.topic) match {
          case Some(bucketOptions) =>
            for {
              formatWriter <- formats.writer.FormatWriter(
                bucketOptions.formatSelection,
                stagingFilename,
                topicPartition,
              )(config.compressionCodec)
            } yield formatWriter
          case None => FatalCloudSinkError("Can't find format choice in config", topicPartition).asLeft
        }

    val indexManager = new IndexManager(config.offsetSeekerOptions.maxIndexFiles)

    val transformers = TopicsTransformers.from(config.bucketOptions)
    new WriterManager(
      commitPolicyFn,
      bucketAndPrefixFn,
      keyNamerFn,
      stagingFilenameFn,
      finalFilenameFn,
      formatWriterFn,
      indexManager,
      transformers.transform,
    )
  }

  private def bucketOptsForTopic(config: CloudSinkConfig, topic: Topic): Option[CloudSinkBucketOptions] =
    config.bucketOptions.find(bo => bo.sourceTopic.isEmpty || bo.sourceTopic.contains(topic.value))

  private def fatalErrorTopicNotConfigured(topicPartition: TopicPartition): SinkError =
    FatalCloudSinkError(
      s"Can't find the KCQL for source topic [${topicPartition.topic}]. The topics defined via [topics] or [topics.regex] need to have an equivalent KCQL statement: INSERT INTO {DESTINATION} SELECT * FROM {TOPIC}.",
      topicPartition,
    )
}
