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

  /** Cumulative final delivery-endpoint response counts after body decoding. */
  def getHttp200Count:             Long
  def getHttp201Count:             Long
  def getHttp202Count:             Long
  def getHttp204Count:             Long
  def getHttp206Count:             Long
  def getHttp300Count:             Long
  def getHttp301Count:             Long
  def getHttp302Count:             Long
  def getHttp303Count:             Long
  def getHttp304Count:             Long
  def getHttp307Count:             Long
  def getHttp308Count:             Long
  def getHttp400Count:             Long
  def getHttp401Count:             Long
  def getHttp403Count:             Long
  def getHttp404Count:             Long
  def getHttp405Count:             Long
  def getHttp408Count:             Long
  def getHttp409Count:             Long
  def getHttp410Count:             Long
  def getHttp412Count:             Long
  def getHttp413Count:             Long
  def getHttp415Count:             Long
  def getHttp422Count:             Long
  def getHttp429Count:             Long
  def getHttp500Count:             Long
  def getHttp501Count:             Long
  def getHttp502Count:             Long
  def getHttp503Count:             Long
  def getHttp504Count:             Long
  def getHttp507Count:             Long
  def getHttp508Count:             Long
  def getHttp510Count:             Long
  def getHttp511Count:             Long
  def getOtherHttpStatusCodeCount: Long

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
  def recordHttpResponseStatus(statusCode: Int): Unit
  def getP50RequestTimeMs: Long
  def getP95RequestTimeMs: Long
  def getP99RequestTimeMs: Long
}

class HttpSinkMetrics extends HttpSinkMetricsMBean {
  private val successCount             = new LongAdder()
  private val error4xxCount            = new LongAdder()
  private val error5xxCount            = new LongAdder()
  private val otherErrorsCount         = new LongAdder()
  private val responseCodeCounts       = HttpSinkMetrics.TrackedResponseCodes.map(_ -> new LongAdder()).toMap
  private val otherHttpStatusCodeCount = new LongAdder()

  private val p50RequestTimeMs = new AtomicLong(0L)
  private val p95RequestTimeMs = new AtomicLong(0L)
  private val p99RequestTimeMs = new AtomicLong(0L)

  // Sets the maximum value to record in the histogram
  private val MaxValueMillis = 3 * 60 * 60 * 1000L // 3 hours; a more than 3hs HTTP request is considered an error
  private val recorder       = new Recorder(MaxValueMillis, 3)

  def increment2xxCount(): Unit = successCount.increment()
  def increment4xxCount(): Unit = error4xxCount.increment()
  def increment5xxCount(): Unit = error5xxCount.increment()

  def recordHttpResponseStatus(statusCode: Int): Unit = {
    if (statusCode >= 200 && statusCode < 300) increment2xxCount()
    else if (statusCode >= 400 && statusCode < 500) increment4xxCount()
    else if (statusCode >= 500 && statusCode < 600) increment5xxCount()
    else incrementOtherErrorsCount()

    recordStatusCode(responseCodeCounts, otherHttpStatusCodeCount, statusCode)
  }

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
  override def getHttp200Count:             Long = codeCount(responseCodeCounts, 200)
  override def getHttp201Count:             Long = codeCount(responseCodeCounts, 201)
  override def getHttp202Count:             Long = codeCount(responseCodeCounts, 202)
  override def getHttp204Count:             Long = codeCount(responseCodeCounts, 204)
  override def getHttp206Count:             Long = codeCount(responseCodeCounts, 206)
  override def getHttp300Count:             Long = codeCount(responseCodeCounts, 300)
  override def getHttp301Count:             Long = codeCount(responseCodeCounts, 301)
  override def getHttp302Count:             Long = codeCount(responseCodeCounts, 302)
  override def getHttp303Count:             Long = codeCount(responseCodeCounts, 303)
  override def getHttp304Count:             Long = codeCount(responseCodeCounts, 304)
  override def getHttp307Count:             Long = codeCount(responseCodeCounts, 307)
  override def getHttp308Count:             Long = codeCount(responseCodeCounts, 308)
  override def getHttp400Count:             Long = codeCount(responseCodeCounts, 400)
  override def getHttp401Count:             Long = codeCount(responseCodeCounts, 401)
  override def getHttp403Count:             Long = codeCount(responseCodeCounts, 403)
  override def getHttp404Count:             Long = codeCount(responseCodeCounts, 404)
  override def getHttp405Count:             Long = codeCount(responseCodeCounts, 405)
  override def getHttp408Count:             Long = codeCount(responseCodeCounts, 408)
  override def getHttp409Count:             Long = codeCount(responseCodeCounts, 409)
  override def getHttp410Count:             Long = codeCount(responseCodeCounts, 410)
  override def getHttp412Count:             Long = codeCount(responseCodeCounts, 412)
  override def getHttp413Count:             Long = codeCount(responseCodeCounts, 413)
  override def getHttp415Count:             Long = codeCount(responseCodeCounts, 415)
  override def getHttp422Count:             Long = codeCount(responseCodeCounts, 422)
  override def getHttp429Count:             Long = codeCount(responseCodeCounts, 429)
  override def getHttp500Count:             Long = codeCount(responseCodeCounts, 500)
  override def getHttp501Count:             Long = codeCount(responseCodeCounts, 501)
  override def getHttp502Count:             Long = codeCount(responseCodeCounts, 502)
  override def getHttp503Count:             Long = codeCount(responseCodeCounts, 503)
  override def getHttp504Count:             Long = codeCount(responseCodeCounts, 504)
  override def getHttp507Count:             Long = codeCount(responseCodeCounts, 507)
  override def getHttp508Count:             Long = codeCount(responseCodeCounts, 508)
  override def getHttp510Count:             Long = codeCount(responseCodeCounts, 510)
  override def getHttp511Count:             Long = codeCount(responseCodeCounts, 511)
  override def getOtherHttpStatusCodeCount: Long = otherHttpStatusCodeCount.sum()
  override def incrementOtherErrorsCount(): Unit = otherErrorsCount.increment()

  override def getP50RequestTimeMs: Long = p50RequestTimeMs.get()
  override def getP95RequestTimeMs: Long =
    p95RequestTimeMs.get()
  override def getP99RequestTimeMs: Long =
    p99RequestTimeMs.get()
  private def recordStatusCode(
    counts: Map[Int, LongAdder],
    other:  LongAdder,
    code:   Int,
  ): Unit =
    counts.get(code).fold(other.increment())(_.increment())

  private def codeCount(counts: Map[Int, LongAdder], code: Int): Long =
    counts.get(code).fold(0L)(_.sum())
}

object HttpSinkMetrics {
  val TrackedResponseCodes: Set[Int] = Set(
    200, 201, 202, 204, 206, 300, 301, 302, 303, 304, 307, 308, 400, 401, 403, 404, 405, 408, 409, 410, 412, 413, 415,
    422, 429, 500, 501, 502, 503, 504, 507, 508, 510, 511,
  )
}
