/*
 * Copyright 2017-2025 Lenses.io Ltd
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
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.{BatchCloudSinkError, FatalCloudSinkError, SinkError}
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.Try

/**
 * Manages the commit operations for writers.
 *
 * @param fnGetWriters Function to retrieve the current map of writers.
 * @param connectorTaskId Implicit task ID for logging purposes.
 * @param executionContext Execution context for asynchronous operations.
 * @param uploadDurationTimeout Duration to wait for upload operations to complete.
 * @tparam SM Type parameter for file metadata.
 */
class WriterCommitManager[SM <: FileMetadata](
  fnGetWriters: () => Map[MapKey, Writer[SM]],
  executionContext: ExecutionContext,
  private val uploadDurationTimeout: Duration,
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends LazyLogging {
  implicit private val uploadExecutionContext: ExecutionContext = executionContext


  /**
   * Commits writers that have pending uploads.
   *
   * @return Either a SinkError or Unit if successful.
   */
  def commitPending(): Either[SinkError, Unit] =
    commitWritersWithFilter {
      case (_, writer) => writer.hasPendingUpload
    }

  /**
   * Commits writers for a specific topic partition.
   *
   * @param topicPartition The topic partition to commit writers for.
   * @return Either a BatchCloudSinkError or Unit if successful.
   */
  def commitForTopicPartition(topicPartition: TopicPartition): Either[BatchCloudSinkError, Unit] =
    commitWritersWithFilter {
      case (mapKey, _) =>
        mapKey.topicPartition == topicPartition
    }

  /**
   * Commits writers that should be flushed.
   *
   * @return Either a BatchCloudSinkError or Unit if successful.
   */
  def commitFlushableWriters(): Either[BatchCloudSinkError, Unit] =
    commitWritersWithFilter {
      case (_, writer) => writer.shouldFlush
    }

  /**
   * Commits writers that should be flushed for a specific topic partition.
   *
   * @param topicPartition The topic partition to commit flushable writers for.
   * @return Either a BatchCloudSinkError or Unit if successful.
   */
  def commitFlushableWritersForTopicPartition(topicPartition: TopicPartition): Either[BatchCloudSinkError, Unit] =
    commitWritersWithFilter {
      case (MapKey(tp, _), writer) => tp == topicPartition && writer.shouldFlush
    }

  /**
   * Commits writers based on a filter function.
   *
   * @param keyValueFilterFn The filter function to determine which writers to commit.
   * @return Either a BatchCloudSinkError or Unit if successful.
   */
  private def commitWritersWithFilter(
    keyValueFilterFn: ((MapKey, Writer[SM])) => Boolean,
  ): Either[BatchCloudSinkError, Unit] = {

    // 1 get TPs for all writers that match filter
    val affectedTopicPartitions = fnGetWriters().filter(keyValueFilterFn).map(_._1.topicPartition).toSet

    // 2 all writers from topic partition that matches filter must be committed
    val allWritersToCommit = fnGetWriters().filter {
      case (MapKey(tp, _), _) =>
        affectedTopicPartitions.contains(tp)
    }

    logger.debug(s"[{}] Received call to WriterCommitManager.commitWritersWithFilter (filter)", connectorTaskId.show)

    val writerFutures = Future
      .sequence(allWritersToCommit
        .values
        .map(_.startCommit))

    val writerCommitErrors =
      Try {
        Await.result(writerFutures, uploadDurationTimeout)
      }.fold(
        ex => Set[SinkError](FatalCloudSinkError("Timed out waiting for writer commits", Some(ex), affectedTopicPartitions.head)),
        results =>
          results
            .collect {
              case Left(err) => err
            }.toSet
      )

    Either.cond(
      writerCommitErrors.isEmpty,
      (),
      BatchCloudSinkError(writerCommitErrors),
    )
  }
}
