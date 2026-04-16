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
}
