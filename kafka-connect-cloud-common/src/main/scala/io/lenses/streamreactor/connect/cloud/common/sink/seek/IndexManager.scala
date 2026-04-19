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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError

/**
 * A trait for managing indexing operations in a cloud sink.
 * Provides methods to check if indexing is enabled, open topic partitions,
 * update offsets, and retrieve seeked offsets.
 */
trait IndexManager {

  /**
   * Checks if indexing is enabled.
   *
   * @return A boolean value indicating whether indexing is enabled.
   */
  def indexingEnabled: Boolean

  /**
   * Opens a set of topic partitions for writing.
   * If an index file is not found, a new one is created.
   *
   * @param topicPartitions A set of `TopicPartition` objects to open.
   * @return An `Either` containing a `SinkError` on failure or a map of
   *         `TopicPartition` to `Option[Offset]` on success.
   */
  def open(
    topicPartitions: Set[TopicPartition],
  ): Either[
    SinkError,
    Map[TopicPartition, Option[Offset]],
  ]

  /**
   * Updates the state for a specific topic partition.
   *
   * @param topicPartition  The `TopicPartition` to update.
   * @param committedOffset An optional committed offset.
   * @param pendingState    An optional pending state.
   * @return An `Either` containing a `SinkError` on failure or an `Option[Offset]` on success.
   */
  def update(
    topicPartition:  TopicPartition,
    committedOffset: Option[Offset],
    pendingState:    Option[PendingState],
  ): Either[
    SinkError,
    Option[Offset],
  ]

  /**
   * Retrieves the seeked offset for a specific topic partition.
   *
   * @param topicPartition The `TopicPartition` to retrieve the offset for.
   * @return An `Option[Offset]` containing the seeked offset, or `None` if not available.
   */
  def getSeekedOffsetForTopicPartition(
    topicPartition: TopicPartition,
  ): Option[Offset]

  /**
   * Retrieves the seeked offset for a specific partition key within a topic partition.
   * Used by writers with PARTITIONBY to consult their own granular lock.
   *
   * @param topicPartition The `TopicPartition` to retrieve the offset for.
   * @param partitionKey   The sanitized partition key identifying the granular lock.
   * @return A `Right(Some(offset))` if the granular lock exists and has a committed offset,
   *         `Right(None)` if the granular lock does not exist (e.g. fresh deployment),
   *         or `Left(SinkError)` if a transient error prevented reading the lock.
   */
  def getSeekedOffsetForPartitionKey(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Either[SinkError, Option[Offset]]

  /**
   * Updates the granular lock for a specific partition key within a topic partition.
   *
   * @param topicPartition  The `TopicPartition` to update.
   * @param partitionKey    The sanitized partition key identifying the granular lock.
   * @param committedOffset An optional committed offset.
   * @param pendingState    An optional pending state.
   * @return An `Either` containing a `SinkError` on failure or an `Option[Offset]` on success.
   */
  def updateForPartitionKey(
    topicPartition:  TopicPartition,
    partitionKey:    String,
    committedOffset: Option[Offset],
    pendingState:    Option[PendingState],
  ): Either[SinkError, Option[Offset]]

  /**
   * Updates the master lock with the global safe offset.
   * Writes `globalSafeOffset - 1` as the committedOffset to preserve existing semantics.
   *
   * @param topicPartition   The `TopicPartition` to update.
   * @param globalSafeOffset The global safe offset (min of first buffered offsets, or max committed + 1).
   * @return An `Either` containing a `SinkError` on failure or `Unit` on success.
   */
  def updateMasterLock(
    topicPartition:   TopicPartition,
    globalSafeOffset: Offset,
  ): Either[SinkError, Unit]

  /**
   * Deletes granular lock files whose committed offset is below the global safe offset.
   * Lock files belonging to active writers (identified by `activePartitionKeys`) are never deleted,
   * even if their committed offset is below the threshold, because the writer still needs
   * the file for its next conditional commit.
   *
   * @param topicPartition     The `TopicPartition` whose granular locks to clean up.
   * @param globalSafeOffset   The global safe offset; locks below this are eligible for deletion.
   * @param activePartitionKeys Partition keys that have an active writer and must not be deleted.
   * @return An `Either` containing a `SinkError` on failure or `Unit` on success.
   */
  def cleanUpObsoleteLocks(
    topicPartition:      TopicPartition,
    globalSafeOffset:    Offset,
    activePartitionKeys: Set[String],
  ): Either[SinkError, Unit]

  /**
   * Ensures a granular lock file exists for the given partition key.
   * Creates it if it doesn't exist. No-op if already present.
   *
   * @param topicPartition The `TopicPartition`.
   * @param partitionKey   The sanitized partition key.
   * @return An `Either` containing a `SinkError` on failure or `Unit` on success.
   */
  def ensureGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Either[SinkError, Unit]

  /**
   * Evicts a single granular lock entry from the in-memory cache.
   * The underlying lock file in cloud storage is NOT deleted.
   *
   * @param topicPartition The `TopicPartition`.
   * @param partitionKey   The sanitized partition key identifying the granular lock to evict.
   */
  def evictGranularLock(
    topicPartition: TopicPartition,
    partitionKey:   String,
  ): Unit

  /**
   * Evicts all granular lock entries for a topic partition from the in-memory cache.
   * The underlying lock files in cloud storage are NOT deleted.
   *
   * @param topicPartition The `TopicPartition` whose granular locks should be evicted.
   */
  def evictAllGranularLocks(
    topicPartition: TopicPartition,
  ): Unit

  /**
   * Removes all in-memory state for a topic partition (seeked offsets, eTags).
   * Called during partition revocation (rebalance) and task shutdown to prevent
   * background threads (e.g. the orphan sweep) from operating on stale partitions.
   *
   * @param topicPartition The `TopicPartition` whose state should be cleared.
   */
  def clearTopicPartitionState(topicPartition: TopicPartition): Unit

  /**
   * Signals that partitions are being closed and background threads should skip work
   * until the next `open()` call. This is a best-effort gate: it prevents new scheduled
   * invocations of `drainGcQueue` and `sweepOrphanedLocks` from starting, but cannot
   * stop an already-running invocation. In-flight executions are benign -- see the
   * architecture doc for the full safety argument.
   */
  def suspendBackgroundWork(): Unit

  /**
   * Releases any resources held by this IndexManager (e.g. background executors).
   * Called during connector task shutdown.
   */
  def close(): Unit
}
