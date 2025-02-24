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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError

class NoIndexManager extends IndexManager {

  /**
    * Opens a topic/partition for writing
    * If an index file hasn't been found, we create one.
    *
    * @param topicPartition
    * @return
    */
  override def open(topicPartitions: Set[TopicPartition]): Either[SinkError, Map[TopicPartition, Option[Offset]]] =
    topicPartitions.map(tp => tp -> Option.empty[Offset]).toMap.asRight

  override def update(
    topicPartition:  TopicPartition,
    committedOffset: Option[Offset],
    pendingState:    Option[PendingState],
  ): Either[SinkError, Option[Offset]] =
    Option.empty.asRight

  override def getSeekedOffsetForTopicPartition(topicPartition: TopicPartition): Option[Offset] = Option.empty

  override def indexingEnabled: Boolean = false
}
