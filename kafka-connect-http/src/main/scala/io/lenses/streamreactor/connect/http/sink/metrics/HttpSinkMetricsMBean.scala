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
package io.lenses.streamreactor.connect.http.sink.metrics

import org.HdrHistogram.Recorder

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

trait HttpSinkMetricsMBean {
  def get2xxCount:         Long
  def get4xxCount:         Long
  def get5xxCount:         Long
  def getOtherErrorsCount: Long

  /**
   * Record the time taken to process a request
   */
  def recordRequestTime(time: Long): Unit

  /**
   * Reset the request time histogram based on a time window interval and calculate the percentiles
   */
  def resetRequestTime(): Unit

  /**
   * Update the percentiles based on the current histogram
   */
  def updatePercentiles(): Unit

  def increment2xxCount():         Unit
  def increment4xxCount():         Unit
  def increment5xxCount():         Unit
  def incrementOtherErrorsCount(): Unit
  def getP50RequestTimeMs:         Long
  def getP95RequestTimeMs:         Long
  def getP99RequestTimeMs:         Long
}

class HttpSinkMetrics extends HttpSinkMetricsMBean {
  private val successCount     = new LongAdder()
  private val error4xxCount    = new LongAdder()
  private val error5xxCount    = new LongAdder()
  private val otherErrorsCount = new LongAdder()

  private val p50RequestTimeMs = new AtomicLong(0L)
  private val p95RequestTimeMs = new AtomicLong(0L)
  private val p99RequestTimeMs = new AtomicLong(0L)

  // Sets the maximum value to record in the histogram
  private val MaxValueMillis = 3 * 60 * 60 * 1000L // 3 hours; a more than 3hs HTTP request is considered an error
  private val recorder       = new Recorder(MaxValueMillis, 3)

  def increment2xxCount(): Unit = successCount.increment()
  def increment4xxCount(): Unit = error4xxCount.increment()
  def increment5xxCount(): Unit = error5xxCount.increment()

  def recordRequestTime(millis: Long): Unit =
    // Record handles concurrent recording. It's the reset that will extract the percentiles and that call is also synchronized
    // That means the percentiles will be calculated based on the last reset
    // So if the time window is 1h, the percentiles will be calculated based on the last hour at the end of the hour
    recorder.recordValue(math.min(millis, MaxValueMillis))

  def resetRequestTime(): Unit = {
    updatePercentiles()
    recorder.reset()
  }

  def updatePercentiles(): Unit = {
    val histogram = recorder.getIntervalHistogram()
    p50RequestTimeMs.set(histogram.getValueAtPercentile(50.0))
    p95RequestTimeMs.set(histogram.getValueAtPercentile(95.0))
    p99RequestTimeMs.set(histogram.getValueAtPercentile(99.0))
  }

  override def get2xxCount:                 Long = successCount.sum()
  override def get4xxCount:                 Long = error4xxCount.sum()
  override def get5xxCount:                 Long = error5xxCount.sum()
  override def getOtherErrorsCount:         Long = otherErrorsCount.sum()
  override def incrementOtherErrorsCount(): Unit = otherErrorsCount.increment()

  override def getP50RequestTimeMs: Long = p50RequestTimeMs.get()
  override def getP95RequestTimeMs: Long =
    p95RequestTimeMs.get()
  override def getP99RequestTimeMs: Long =
    p99RequestTimeMs.get()
}
