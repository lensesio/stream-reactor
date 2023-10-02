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

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.sink.config.S3SinkConfig
import io.lenses.streamreactor.connect.aws.s3.sink.config.SinkBucketOptions
import io.lenses.streamreactor.connect.aws.s3.sink.transformers.TopicsTransformers
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats
import io.lenses.streamreactor.connect.cloud.common.formats.writer.S3FormatWriter
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.FatalS3SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.writer.S3WriterManager
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface

import java.io.File
import scala.collection.immutable

object S3WriterManagerCreator extends LazyLogging {

  def from(
    config: S3SinkConfig,
  )(
    implicit
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface,
  ): S3WriterManager = {

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
      Offset,
    ) => Either[SinkError, CloudLocation] = (topicPartition, partitionValues, offset) =>
      bucketOptsForTopic(config, topicPartition.topic) match {
        case Some(bucketOptions) =>
          for {
            keyNamer <- keyNamerFn(topicPartition)
            stagingFilename <- keyNamer.finalFilename(bucketOptions.bucketAndPrefix,
                                                      topicPartition.withOffset(offset),
                                                      partitionValues,
            )
          } yield stagingFilename
        case None => fatalErrorTopicNotConfigured(topicPartition).asLeft
      }

    val formatWriterFn: (TopicPartition, File) => Either[SinkError, S3FormatWriter] =
      (topicPartition: TopicPartition, stagingFilename) =>
        bucketOptsForTopic(config, topicPartition.topic) match {
          case Some(bucketOptions) =>
            for {
              formatWriter <- formats.writer.S3FormatWriter(
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
      keyNamerFn,
      stagingFilenameFn,
      finalFilenameFn,
      formatWriterFn,
      indexManager,
      transformers.transform,
    )
  }

  private def bucketOptsForTopic(config: S3SinkConfig, topic: Topic): Option[SinkBucketOptions] =
    config.bucketOptions.find(bo => bo.sourceTopic.isEmpty || bo.sourceTopic.contains(topic.value))

  private def fatalErrorTopicNotConfigured(topicPartition: TopicPartition): SinkError =
    FatalS3SinkError(
      s"Can't find the KCQL for source topic [${topicPartition.topic}]. The topics defined via [topics] or [topics.regex] need to have an equivalent KCQL statement: INSERT INTO {S3_BUCKET} SELECT * FROM {TOPIC}.",
      topicPartition,
    )
}
