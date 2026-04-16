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

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError

/**
 * A class representing a no-op implementation of the `IndexManager` trait.
 * This implementation disables indexing and provides default behavior for
 * managing offsets and pending states.
 */
class NoIndexManager extends IndexManager {

  /**
   * Opens the specified topic partitions and returns an empty offset map.
   *
   * @param topicPartitions A set of `TopicPartition` objects to open.
   * @return An `Either` containing a map of `TopicPartition` to `Option[Offset]`,
   *         where all offsets are empty.
   */
  override def open(topicPartitions: Set[TopicPartition]): Either[SinkError, Map[TopicPartition, Option[Offset]]] =
    topicPartitions.map(tp => tp -> Option.empty[Offset]).toMap.asRight

  /**
   * Updates the state for a specific topic partition. This implementation
   * always returns an empty offset.
   *
   * @param topicPartition  The `TopicPartition` to update.
   * @param committedOffset An optional committed offset.
   * @param pendingState    An optional pending state.
   * @return An `Either` containing an empty `Option[Offset]`.
   */
  override def update(
    topicPartition:  TopicPartition,
    committedOffset: Option[Offset],
    pendingState:    Option[PendingState],
  ): Either[SinkError, Option[Offset]] =
    Option.empty.asRight

  /**
   * Retrieves the seeked offset for a specific topic partition. This implementation
   * always returns `None`.
   *
   * @param topicPartition The `TopicPartition` to retrieve the offset for.
   * @return An `Option[Offset]`, which is always `None`.
   */
  override def getSeekedOffsetForTopicPartition(topicPartition: TopicPartition): Option[Offset] = Option.empty

  /**
   * Indicates whether indexing is enabled. This implementation always returns `false`.
   *
   * @return A boolean value indicating that indexing is disabled.
   */
  override def indexingEnabled: Boolean = false
}
