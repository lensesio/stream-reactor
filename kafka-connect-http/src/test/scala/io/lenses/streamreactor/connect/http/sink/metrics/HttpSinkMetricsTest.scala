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
package io.lenses.streamreactor.connect.http.sink.metrics
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class HttpSinkMetricsTest extends AnyFunSuite with Matchers {

  test("HttpSinkMetrics should calculate percentiles correctly") {
    val metrics = new HttpSinkMetrics()

    // Record a series of request times
    metrics.recordRequestTime(100)
    metrics.recordRequestTime(200)
    metrics.recordRequestTime(300)
    metrics.recordRequestTime(400)
    metrics.recordRequestTime(500)

    // Check percentiles
    metrics.getP50RequestTimeMs should be(300L +- 10L) // median should be around 300
    metrics.getP95RequestTimeMs should be(500L +- 10L) // 95th percentile should be around 500
    metrics.getP99RequestTimeMs should be(500L +- 10L) // 99th percentile should be around 500
  }

  test("HttpSinkMetrics should handle requests with the same time") {
    val metrics = new HttpSinkMetrics()

    // Record multiple requests with the same time
    metrics.recordRequestTime(200)
    metrics.recordRequestTime(200)
    metrics.recordRequestTime(200)

    // All percentiles should be exactly 200 since all values are the same
    metrics.getP50RequestTimeMs shouldBe 200L
    metrics.getP95RequestTimeMs shouldBe 200L
    metrics.getP99RequestTimeMs shouldBe 200L
  }

  test("HttpSinkMetrics should calculate average request time correctly") {
    val metrics = new HttpSinkMetrics()

    metrics.recordRequestTime(100)
    metrics.recordRequestTime(200)
    metrics.recordRequestTime(300)

  }

  test("HttpSinkMetrics should handle zero requests") {
    val metrics = new HttpSinkMetrics()

    metrics.getP50RequestTimeMs shouldBe 0L
    metrics.getP95RequestTimeMs shouldBe 0L
    metrics.getP99RequestTimeMs shouldBe 0L
  }

  test("HttpSinkMetrics should track HTTP status counts correctly") {
    val metrics = new HttpSinkMetrics()

    // Record some success and error counts
    metrics.increment2xxCount()
    metrics.increment2xxCount()
    metrics.increment4xxCount()
    metrics.increment5xxCount()
    metrics.increment5xxCount()
    metrics.increment5xxCount()
    metrics.incrementOtherErrorsCount()

    metrics.get2xxCount shouldBe 2L
    metrics.get4xxCount shouldBe 1L
    metrics.get5xxCount shouldBe 3L
    metrics.getOtherErrorsCount shouldBe 1L
  }

  test("HttpSinkMetrics should handle large request times") {
    val metrics = new HttpSinkMetrics()

    // Record some large values
    metrics.recordRequestTime(3600000L) // 1 hour
    metrics.recordRequestTime(7200000L) // 2 hours
    metrics.recordRequestTime(1800000L) // 30 minutes

    // Values should be recorded accurately even for large numbers
    metrics.getP50RequestTimeMs should be(3600000L +- 36000L) // 1% tolerance for large numbers
    metrics.getP95RequestTimeMs should be(7200000L +- 72000L)
    metrics.getP99RequestTimeMs should be(7200000L +- 72000L)
  }

  test("HttpSinkMetrics should handle mixed request times") {
    val metrics = new HttpSinkMetrics()

    // Mix of small and large values
    metrics.recordRequestTime(100)     // 100ms
    metrics.recordRequestTime(500)     // 500ms
    metrics.recordRequestTime(1000)    // 1s
    metrics.recordRequestTime(60000)   // 1m
    metrics.recordRequestTime(3600000) // 1h

    // Check if percentiles handle mixed scales correctly
    metrics.getP50RequestTimeMs should be(1000L +- 100L)
    metrics.getP95RequestTimeMs should be(3600000L +- 36000L)
    metrics.getP99RequestTimeMs should be(3600000L +- 36000L)
  }

  test("HttpSinkMetrics should handle very small request times") {
    val metrics = new HttpSinkMetrics()

    // Record some very small values
    metrics.recordRequestTime(1) // 1ms
    metrics.recordRequestTime(2) // 2ms
    metrics.recordRequestTime(5) // 5ms

    // Check precision for small values
    metrics.getP50RequestTimeMs shouldBe 2L
    metrics.getP95RequestTimeMs shouldBe 5L
    metrics.getP99RequestTimeMs shouldBe 5L
  }

  test("HttpSinkMetrics should handle values exceeding maximum histogram value") {
    val metrics            = new HttpSinkMetrics()
    val fourHoursInMillis  = 4 * 60 * 60 * 1000L // 4 hours = 14,400,000 ms
    val threeHoursInMillis = 3 * 60 * 60 * 1000L // 3 hours = 10,800,000 ms

    // Record a mix of values including one exceeding maximum
    metrics.recordRequestTime(1000)              // 1 second
    metrics.recordRequestTime(fourHoursInMillis) // 4 hours (exceeds max)
    metrics.recordRequestTime(60000)             // 1 minute

    // Verify the percentile values are capped at 3 hours
    metrics.getP50RequestTimeMs should be(60000L +- 100L)
    metrics.getP95RequestTimeMs shouldBe threeHoursInMillis +- 10000
    metrics.getP99RequestTimeMs shouldBe threeHoursInMillis +- 10000

  }

  test("HttpSinkMetrics should handle multiple values near and exceeding maximum") {
    val metrics            = new HttpSinkMetrics()
    val threeHoursInMillis = 3 * 60 * 60 * 1000L // 3 hours

    metrics.recordRequestTime(threeHoursInMillis - 1000) // Just under max
    metrics.recordRequestTime(threeHoursInMillis)        // At max
    metrics.recordRequestTime(threeHoursInMillis + 1000) // Just over max

    // All percentiles should be at or very close to 3 hours
    metrics.getP50RequestTimeMs shouldBe threeHoursInMillis +- 10000
    metrics.getP95RequestTimeMs shouldBe threeHoursInMillis +- 10000
    metrics.getP99RequestTimeMs shouldBe threeHoursInMillis +- 10000
  }
}
