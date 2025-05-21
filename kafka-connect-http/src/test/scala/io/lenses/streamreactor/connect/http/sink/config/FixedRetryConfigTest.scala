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
package io.lenses.streamreactor.connect.http.sink.config

import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.DurationInt

class FixedRetryConfigTest extends AnyFunSuiteLike with Matchers {
  test("fixedInterval should return Some(interval) for the first attempt") {
    val interval    = 1.second
    val maxRetry    = 3
    val retryPolicy = FixedRetryConfig.fixedInterval(interval, maxRetry)
    retryPolicy(1) shouldBe Some(interval)
  }

  test("fixedInterval should return Some(interval) for attempt zero") {
    val interval    = 1.second
    val maxRetry    = 3
    val retryPolicy = FixedRetryConfig.fixedInterval(interval, maxRetry)
    retryPolicy(0) shouldBe Some(interval)
  }

  test("fixedInterval should return Some(interval) for all attempts up to maxRetry") {
    val interval    = 2.seconds
    val maxRetry    = 5
    val retryPolicy = FixedRetryConfig.fixedInterval(interval, maxRetry)

    (0 to maxRetry).foreach { attempt =>
      retryPolicy(attempt) shouldBe Some(interval)
    }
  }

  test("fixedInterval should return None for all attempts beyond maxRetry") {
    val interval    = 500.milliseconds
    val maxRetry    = 2
    val retryPolicy = FixedRetryConfig.fixedInterval(interval, maxRetry)

    (maxRetry + 1 to maxRetry + 3).foreach { attempt =>
      retryPolicy(attempt) shouldBe None
    }
  }

  test("fixedInterval should work with different interval durations") {
    val maxRetry     = 1
    val retryPolicy1 = FixedRetryConfig.fixedInterval(100.milliseconds, maxRetry)
    val retryPolicy2 = FixedRetryConfig.fixedInterval(1.minute, maxRetry)
    val retryPolicy3 = FixedRetryConfig.fixedInterval(2.hours, maxRetry)

    retryPolicy1(0) shouldBe Some(100.milliseconds)
    retryPolicy2(0) shouldBe Some(1.minute)
    retryPolicy3(0) shouldBe Some(2.hours)
  }

  test("fixedInterval should handle zero maxRetry") {
    val interval    = 1.second
    val maxRetry    = 0
    val retryPolicy = FixedRetryConfig.fixedInterval(interval, maxRetry)

    retryPolicy(0) shouldBe Some(interval)
    retryPolicy(1) shouldBe None
  }
}
