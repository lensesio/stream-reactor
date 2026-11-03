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
package io.lenses.streamreactor.common.batch

import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.ConditionCommitResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset

class BatchPolicyTest extends AnyFlatSpec with Matchers {

  private def context(recordCount: Long, bytes: Long, lastModifiedMillis: Long): CommitContext =
    new CommitContext {
      override def count:                Long         = recordCount
      override def fileSize:             Long         = bytes
      override def createdTimestamp:     Long         = lastModifiedMillis
      override def lastFlushedTimestamp: Option[Long] = None
      override def generateLogLine(flushing: Boolean, result: Seq[ConditionCommitResult]): String = ""
    }

  // A clock reading well past the interval deadline, so an interval anchored at `lastModified = 0`
  // reads as elapsed (greedy).
  private val elapsedClock: Clock = Clock.fixed(Instant.ofEpochMilli(600_000L), ZoneOffset.UTC)

  "shouldBatch" should "produce the same result regardless of condition order (interval first vs last)" in {
    val count    = Count(1000)
    val interval = Interval(Duration.ofSeconds(1), elapsedClock)
    val ctx      = context(recordCount = 3, bytes = 21, lastModifiedMillis = 0L)

    val intervalFirst = BatchPolicy(interval, count).shouldBatch(ctx)
    val intervalLast  = BatchPolicy(count, interval).shouldBatch(ctx)

    val expected = BatchResult(fitsInBatch = true,
                               triggerReached       = false,
                               greedyTriggerReached = true,
                               triggeredConditions  = Set(interval),
    )
    intervalFirst should be(expected)
    intervalLast should be(expected)
  }

  it should "conjoin fitsInBatch across conditions so a size limit can reject an overshooting record" in {
    // Count(1000) fits (count is well below), but FileSize(10) rejects the 14-byte candidate and
    // triggers. The record must be reported as not fitting regardless of which condition is first.
    val count    = Count(1000)
    val fileSize = FileSize(10)
    val ctx      = context(recordCount = 1, bytes = 14, lastModifiedMillis = 0L)

    val expected = BatchResult(fitsInBatch = false,
                               triggerReached       = true,
                               greedyTriggerReached = false,
                               triggeredConditions  = Set(fileSize),
    )
    BatchPolicy(count, fileSize).shouldBatch(ctx) should be(expected)
    BatchPolicy(fileSize, count).shouldBatch(ctx) should be(expected)
  }
}
