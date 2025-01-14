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

import org.HdrHistogram.Histogram

import java.util.concurrent.atomic.LongAdder

trait HttpSinkMetricsMBean {
  def get2xxCount:         Long
  def get4xxCount:         Long
  def get5xxCount:         Long
  def getOtherErrorsCount: Long
  def recordRequestTime(time: Long): Unit
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

  // Sets the maximum value to record in the histogram
  private val MaxValueMillis = 3 * 60 * 60 * 1000L // 3 hours
  private val histogram      = new Histogram(MaxValueMillis, 3)

  def increment2xxCount(): Unit = successCount.increment()
  def increment4xxCount(): Unit = error4xxCount.increment()
  def increment5xxCount(): Unit = error5xxCount.increment()

  def recordRequestTime(millis: Long): Unit =
    histogram.recordValue(math.min(millis, MaxValueMillis))

  override def get2xxCount:                 Long = successCount.sum()
  override def get4xxCount:                 Long = error4xxCount.sum()
  override def get5xxCount:                 Long = error5xxCount.sum()
  override def getOtherErrorsCount:         Long = otherErrorsCount.sum()
  override def incrementOtherErrorsCount(): Unit = otherErrorsCount.increment()

  override def getP50RequestTimeMs: Long =
    histogram.getValueAtPercentile(50.0)
  override def getP95RequestTimeMs: Long =
    histogram.getValueAtPercentile(95.0)
  override def getP99RequestTimeMs: Long =
    histogram.getValueAtPercentile(99.0)
}
