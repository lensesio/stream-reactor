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

import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition

class NoIndexManagerTest extends AnyFunSuiteLike with Matchers {

  private val noIndexManager = new NoIndexManager

  test("open should return empty offsets for all topic partitions") {
    val topicPartitions = Set(
      TopicPartition(Topic("topic1"), 0),
      TopicPartition(Topic("topic2"), 1),
    )

    val result = noIndexManager.open(topicPartitions)

    result shouldBe Right(Map(
      TopicPartition(Topic("topic1"), 0) -> None,
      TopicPartition(Topic("topic2"), 1) -> None,
    ))
  }

  test("update should always return None for any topic partition") {
    val topicPartition = TopicPartition(Topic("topic1"), 0)
    val result         = noIndexManager.update(topicPartition, Some(Offset(100)), None)

    result shouldBe Right(None)
  }

  test("getSeekedOffsetForTopicPartition should always return None") {
    val topicPartition = TopicPartition(Topic("topic1"), 0)
    val result         = noIndexManager.getSeekedOffsetForTopicPartition(topicPartition)

    result shouldBe None
  }

  test("indexingEnabled should always return false") {
    noIndexManager.indexingEnabled shouldBe false
  }
}
