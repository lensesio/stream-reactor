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
package io.lenses.streamreactor.connect.aws.s3.sink.commit

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.temporal.ChronoUnit
import java.time._

class IntervalTest extends AnyFlatSpec with Matchers with EitherValues with MockitoSugar with BeforeAndAfter {

  private val lastModifiedDate =
    ZonedDateTime.of(LocalDate.of(1939, 5, 1), LocalTime.of(12, 0, 1), ZoneId.of("GMT+1")).toInstant

  private val clock    = mock[Clock]
  private val interval = Interval(Duration.ofMinutes(10), clock)

  before {
    reset(clock)
  }
  "interval" should "return false when still on last modified date" in {

    when(clock.instant()).thenReturn(lastModifiedDate)
    interval.eval(commitContext(lastModifiedDate.toEpochMilli)) should
      be(
        ConditionCommitResult(
          commitTriggered = false,
        ),
      )
    interval
      .eval(commitContext(lastModifiedDate.toEpochMilli)) should
      be(ConditionCommitResult(commitTriggered = false))
  }

  "interval" should "return false when interval not reached yet" in {
    val lastModifiedPlus9 = lastModifiedDate.plus(9, ChronoUnit.MINUTES)

    when(clock.instant()).thenReturn(lastModifiedPlus9)
    interval.eval(commitContext(lastModifiedDate.toEpochMilli)) should
      be(
        ConditionCommitResult(
          commitTriggered = false,
        ),
      )
    interval.eval(commitContext(lastModifiedDate.toEpochMilli)) should
      be(ConditionCommitResult(commitTriggered = false))
  }

  "interval" should "return true when interval reached" in {
    val lastModifiedPlus10 = lastModifiedDate.plus(10, ChronoUnit.MINUTES)

    when(clock.instant()).thenReturn(lastModifiedPlus10)
    interval.eval(commitContext(lastModifiedDate.toEpochMilli)) should
      be(
        ConditionCommitResult(
          commitTriggered = true,
        ),
      )
    interval.eval(commitContext(lastModifiedDate.toEpochMilli)) should
      be(ConditionCommitResult(commitTriggered = true))
  }

  "interval" should "return true when interval exceeded" in {
    val lastModifiedPlus11 = lastModifiedDate.plus(10, ChronoUnit.MINUTES)
    when(clock.instant()).thenReturn(lastModifiedPlus11)

    interval.eval(commitContext(lastModifiedDate.toEpochMilli)) should
      be(
        ConditionCommitResult(
          commitTriggered = true,
        ),
      )
    interval.eval(commitContext(lastModifiedDate.toEpochMilli)) should
      be(ConditionCommitResult(commitTriggered = true))
  }

  private def commitContext(lastModified: Long): CommitContext = {
    val commitContext = mock[CommitContext]
    when(commitContext.lastModified).thenReturn(lastModified)
    commitContext
  }

}
