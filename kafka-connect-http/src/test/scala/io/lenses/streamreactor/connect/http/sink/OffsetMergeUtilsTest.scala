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
package io.lenses.streamreactor.connect.http.sink

import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.commit.HttpCommitContext
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class OffsetMergeUtilsTest extends AnyFunSuite {

  private val sinkName = "mySinkName"

  test(
    "createCommitContextFromBatch should create a new HttpCommitContext with merged offsets when currentCommitContext is provided",
  ) {
    val batch = Seq(
      RenderedRecord(Topic("topic1").withPartition(0).withOffset(Offset(100)), "record1", Seq.empty, None),
      RenderedRecord(Topic("topic2").withPartition(0).withOffset(Offset(50)), "record2", Seq.empty, None),
    )
    val currentCommitContext = HttpCommitContext(
      sinkName,
      Map(Topic("topic1").withPartition(0) -> Offset(50)),
      1L,
      10L,
      System.currentTimeMillis(),
      None,
      Map.empty,
    )

    val result = OffsetMergeUtils.createCommitContextForEvaluation(batch, currentCommitContext)

    result.committedOffsets shouldBe Map(
      Topic("topic1").withPartition(0) -> Offset(100),
      Topic("topic2").withPartition(0) -> Offset(50),
    )
    result.count shouldBe 2L
    result.fileSize shouldBe 14L
  }

  test("createCommitContextFromBatch should create a new HttpCommitContext when currentCommitContext is None") {
    val batch = Seq(
      RenderedRecord(Topic("topic1").withPartition(0).withOffset(Offset(100)), "record1", Seq.empty, None),
      RenderedRecord(Topic("topic2").withPartition(0).withOffset(Offset(50)), "record2", Seq.empty, None),
    )
    val currentCommitContext = HttpCommitContext.default("My Sink")

    val result = OffsetMergeUtils.createCommitContextForEvaluation(batch, currentCommitContext)

    result.committedOffsets shouldBe Map(
      Topic("topic1").withPartition(0) -> Offset(100),
      Topic("topic2").withPartition(0) -> Offset(50),
    )
    result.count shouldBe 2L
    result.fileSize shouldBe 14L
  }

  test("mergeOffsets should merge committed offsets correctly") {
    val committedOffsets = Map(
      Topic("topic1").withPartition(0) -> Offset(100),
      Topic("topic2").withPartition(0) -> Offset(50),
    )
    val topTpos = Map(
      Topic("topic2").withPartition(0) -> Offset(80),
      Topic("topic3").withPartition(0) -> Offset(70),
    )

    val result = OffsetMergeUtils.mergeOffsets(committedOffsets, topTpos)

    result shouldBe Map(
      Topic("topic1").withPartition(0) -> Offset(100),
      Topic("topic2").withPartition(0) -> Offset(80),
      Topic("topic3").withPartition(0) -> Offset(70),
    )
  }

  test("mergeOffsets should handle empty committed offsets") {
    val committedOffsets = Map.empty[TopicPartition, Offset]
    val topTpos = Map(
      Topic("topic2").withPartition(0) -> Offset(80),
      Topic("topic3").withPartition(0) -> Offset(70),
    )

    val result = OffsetMergeUtils.mergeOffsets(committedOffsets, topTpos)

    result shouldBe topTpos
  }
}
