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
package io.lenses.streamreactor.connect.http.sink

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.common.batch.BatchPolicy
import io.lenses.streamreactor.common.batch.Count
import io.lenses.streamreactor.common.batch.FileSize
import io.lenses.streamreactor.common.batch.HttpCommitContext
import io.lenses.streamreactor.common.batch.Interval
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.http.sink.tpl.RenderedRecord
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

class BatchAccumulatorTest extends AnyFunSuiteLike with Matchers with LazyLogging {

  private val defaultContext: HttpCommitContext = HttpCommitContext.default("My Sink")

  private val tp1: TopicPartition = Topic("myTopic").withPartition(1)
  private val tp2: TopicPartition = Topic("myTopic").withPartition(2)

  private val endpoint = "https://mytestendpoint.example.com"

  // "record1".length == 7
  private def rec(tp: TopicPartition, offset: Long, body: String = "record1"): RenderedRecord =
    RenderedRecord(tp.atOffset(offset), offset, body, Seq.empty, endpoint)

  private val record1 = rec(tp1, 100)
  private val record2 = rec(tp1, 101)

  private def fixedClock(atMillis: Long): Clock = new Clock {
    override def getZone:   ZoneId  = ZoneId.systemDefault()
    override def instant(): Instant = Instant.ofEpochMilli(atMillis)
    override def withZone(zone: ZoneId): Clock = this
  }

  test("count condition triggers when the configured count is reached") {
    val acc = new BatchAccumulator(BatchPolicy(logger, Count(2)), defaultContext)

    val r1 = acc.offer(record1)
    r1.fitsInBatch shouldBe true
    r1.triggerReached shouldBe false
    acc.size shouldBe 1

    val r2 = acc.offer(record2)
    r2.fitsInBatch shouldBe true
    r2.triggerReached shouldBe true
    r2.triggeredConditions shouldBe Set(Count(2))
    acc.flushTriggers shouldBe Set(Count(2))
    acc.size shouldBe 2
    acc.currentBatch.map(_.toSeq) shouldBe Some(Seq(record1, record2))
  }

  test("file size condition triggers when the accumulated size is reached") {
    // each record body is 7 bytes; 2 records == 14 bytes
    val acc = new BatchAccumulator(BatchPolicy(logger, FileSize(14L)), defaultContext)

    acc.offer(record1).triggerReached shouldBe false
    acc.offer(record2).triggerReached shouldBe true
    acc.currentBatch.map(_.toSeq) shouldBe Some(Seq(record1, record2))
  }

  test("interval condition greedily triggers once the interval has elapsed and still appends the record") {
    val created = 1_000_000L
    val ctx     = defaultContext.copy(createdTimestamp = created, lastFlushedTimestamp = None)
    // now is 3s after creation, interval is 1s => elapsed
    val acc =
      new BatchAccumulator(BatchPolicy(logger, Interval(Duration.ofSeconds(1), fixedClock(created + 3000))), ctx)

    val r1 = acc.offer(record1)
    r1.greedyTriggerReached shouldBe true
    r1.triggerReached shouldBe false
    // an elapsed interval is a flush trigger, not a capacity limit, so the record fits and is appended
    r1.fitsInBatch shouldBe true
    acc.size shouldBe 1
    acc.flushTriggers shouldBe r1.triggeredConditions
    acc.flushTriggers.exists(_.isInstanceOf[Interval]) shouldBe true
  }

  test("an interval-only policy packs every record into one batch once the interval has elapsed") {
    val created = 1_000_000L
    val ctx     = defaultContext.copy(createdTimestamp = created, lastFlushedTimestamp = None)
    // interval-only policy (no Count/FileSize), already elapsed: every offered record must fit so the
    // whole chunk packs into a single batch rather than being rejected one at a time.
    val acc =
      new BatchAccumulator(BatchPolicy(logger, Interval(Duration.ofSeconds(1), fixedClock(created + 3000))), ctx)

    val recA = rec(tp1, 100)
    val recB = rec(tp1, 101)
    val recC = rec(tp1, 102)
    List(recA, recB, recC).foreach { record =>
      val result = acc.offer(record)
      result.fitsInBatch shouldBe true
      result.greedyTriggerReached shouldBe true
    }

    acc.size shouldBe 3
    acc.currentBatch.map(_.toSeq) shouldBe Some(Seq(recA, recB, recC))
  }

  test("interval condition does not trigger before the interval has elapsed") {
    val created = 1_000_000L
    val ctx     = defaultContext.copy(createdTimestamp = created, lastFlushedTimestamp = None)
    // now is 0.5s after creation, interval is 1s => not elapsed
    val acc = new BatchAccumulator(BatchPolicy(logger, Interval(Duration.ofSeconds(1), fixedClock(created + 500))), ctx)

    val r1 = acc.offer(record1)
    r1.greedyTriggerReached shouldBe false
    r1.triggerReached shouldBe false
    r1.fitsInBatch shouldBe true
    acc.size shouldBe 1
  }

  test("flushed context keeps the per-partition maximum offset across records") {
    val acc = new BatchAccumulator(BatchPolicy(logger, Count(10)), defaultContext)

    val recA = rec(tp1, 100)
    val recB = rec(tp2, 50)
    val recC = rec(tp1, 99) // lower than recA, must not lower the max for tp1

    List(recA, recB, recC).foreach(acc.offer)

    val flushed = acc.flushedContext()
    flushed.committedOffsets shouldBe Map(
      tp1 -> Offset(100),
      tp2 -> Offset(50),
    )
    // post-commit counters are reset
    flushed.count shouldBe 0L
    flushed.fileSize shouldBe 0L
  }

  test("resetTo clears the buffer and adopts the given context") {
    val acc = new BatchAccumulator(BatchPolicy(logger, Count(10)), defaultContext)
    List(record1, record2).foreach(acc.offer)
    acc.size shouldBe 2

    val flushed = acc.flushedContext()
    acc.resetTo(flushed)

    acc.isEmpty shouldBe true
    acc.size shouldBe 0
    acc.currentBatch shouldBe None
  }

  test("nextFlushDeadlineMillis is defined only when an interval condition is present") {
    val created   = 5_000L
    val ctx       = defaultContext.copy(createdTimestamp = created, lastFlushedTimestamp = None)
    val countOnly = new BatchAccumulator(BatchPolicy(logger, Count(10)), ctx)
    val withInterval =
      new BatchAccumulator(BatchPolicy(logger, Interval(Duration.ofSeconds(2), fixedClock(created))), ctx)

    countOnly.nextFlushDeadlineMillis shouldBe None
    withInterval.nextFlushDeadlineMillis shouldBe Some(created + 2000L)
  }

  test("empty accumulator produces no batch") {
    val acc = new BatchAccumulator(BatchPolicy(logger, Count(2)), defaultContext)
    acc.currentBatch shouldBe None
    acc.isEmpty shouldBe true
  }
}
