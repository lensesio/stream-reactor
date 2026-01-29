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

import cats.Applicative
import cats.effect.Clock
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import scala.concurrent.duration._

/**
  * A fixed Clock that always returns the same Instant.
  */
class CustomFixedClock(private val fixedInstant: Instant) extends Clock[IO] {
  override def realTime: IO[FiniteDuration] =
    IO.pure(FiniteDuration(fixedInstant.toEpochMilli, MILLISECONDS))

  override def monotonic: IO[FiniteDuration] =
    IO.pure(FiniteDuration(0, NANOSECONDS)) // Not relevant for this test

  override def realTimeInstant: IO[Instant] =
    IO.pure(fixedInstant)

  override def applicative: Applicative[IO] = ???
}
class MetricsResetterSpec extends AnyFunSuite with Matchers {

  // Mock implementation of HttpSinkMetricsMBean for testing
  class MockHttpSinkMetrics extends HttpSinkMetricsMBean {
    var resetCalled:  Int = 0
    var updateCalled: Int = 0

    override def resetRequestTime(): Unit =
      resetCalled += 1

    override def updatePercentiles(): Unit =
      updateCalled += 1

    override def get2xxCount: Long = ???

    override def get4xxCount: Long = ???

    override def get5xxCount: Long = ???

    override def getOtherErrorsCount: Long = ???

    /**
      * Record the time taken to process a request
      */
    override def recordRequestTime(time: Long): Unit = ???

    override def increment2xxCount(): Unit = ???

    override def increment4xxCount(): Unit = ???

    override def increment5xxCount(): Unit = ???

    override def incrementOtherErrorsCount(): Unit = ???

    override def getP50RequestTimeMs: Long = ???

    override def getP95RequestTimeMs: Long = ???

    override def getP99RequestTimeMs: Long = ???
  }

  // Import or define the calculateInitialDelay method here
  def calculateInitialDelay(interval: FiniteDuration)(implicit clock: Clock[IO]): IO[FiniteDuration] =
    clock.realTimeInstant.flatMap { nowInstant =>
      val now             = LocalDateTime.ofInstant(nowInstant, java.time.ZoneId.systemDefault())
      val intervalMinutes = interval.toMinutes

      val currentMinute = now.getMinute
      val currentSecond = now.getSecond

      val minutesUntilNext    = intervalMinutes - (currentMinute % intervalMinutes)
      val initialDelaySeconds = minutesUntilNext * 60 - currentSecond

      IO.pure(FiniteDuration(initialDelaySeconds, SECONDS))
    }

  test("calculateInitialDelay should compute correct initial delay") {
    // Define the fixed current time: 10:05:30
    val fixedLocalDateTime = LocalDateTime.of(2025, 1, 14, 10, 5, 30)
    val fixedInstant = fixedLocalDateTime
      .atZone(ZoneId.systemDefault())
      .toInstant

    // Create a custom fixed clock
    implicit val fixedClock: Clock[IO] = new CustomFixedClock(fixedInstant)

    val resetInterval = 5.minutes

    // Expected next reset at 10:10:00
    // Current time: 10:05:30
    // Delay should be 4 minutes 30 seconds = 270 seconds
    val expectedDelay = 270.seconds

    val calculatedDelay = calculateInitialDelay(resetInterval).unsafeRunSync()

    calculatedDelay shouldBe expectedDelay
  }

  test("calculateInitialDelay should compute zero delay when exactly at interval") {
    // Define the fixed current time: 10:10:00
    val fixedLocalDateTime = LocalDateTime.of(2025, 1, 14, 10, 10, 0)
    val fixedInstant = fixedLocalDateTime
      .atZone(ZoneId.systemDefault())
      .toInstant

    // Create a custom fixed clock
    implicit val fixedClock: Clock[IO] = new CustomFixedClock(fixedInstant)

    val resetInterval = 5.minutes

    // Expected next reset at 10:15:00
    // Current time: 10:10:00
    // Delay should be 5 minutes = 300 seconds
    val expectedDelay = 300.seconds

    val calculatedDelay = calculateInitialDelay(resetInterval).unsafeRunSync()

    calculatedDelay shouldBe expectedDelay
  }

  test("calculateInitialDelay should compute correct delay across hour boundaries") {
    // Define the fixed current time: 10:59:50
    val fixedLocalDateTime = LocalDateTime.of(2025, 1, 14, 10, 59, 50)
    val fixedInstant = fixedLocalDateTime
      .atZone(ZoneId.systemDefault())
      .toInstant

    // Create a custom fixed clock
    implicit val fixedClock: Clock[IO] = new CustomFixedClock(fixedInstant)

    val resetInterval = 5.minutes

    // Expected next reset at 11:00:00
    // Current time: 10:59:50
    // Delay should be 10 seconds
    val expectedDelay = 10.seconds

    val calculatedDelay = calculateInitialDelay(resetInterval).unsafeRunSync()

    calculatedDelay shouldBe expectedDelay
  }

  test("calculateInitialDelay should handle intervals longer than an hour") {
    // Define the fixed current time: 10:20:15
    val fixedLocalDateTime = LocalDateTime.of(2025, 1, 14, 10, 20, 15)
    val fixedInstant = fixedLocalDateTime
      .atZone(ZoneId.systemDefault())
      .toInstant

    // Create a custom fixed clock
    implicit val fixedClock: Clock[IO] = new CustomFixedClock(fixedInstant)

    val resetInterval   = 90.minutes // 1 hour 30 minutes
    val calculatedDelay = calculateInitialDelay(resetInterval).unsafeRunSync()

    // Expected next reset at 11:30:00
    // Current time: 10:20:15
    // Delay should be 1 hour 9 minutes 45 seconds = 4185 seconds
    val expectedDelay = 4185.seconds
    calculatedDelay shouldBe expectedDelay
  }

  test("calculateInitialDelay should handle interval not aligned with minutes") {
    // Define the fixed current time: 10:05:30
    val fixedLocalDateTime = LocalDateTime.of(2025, 1, 14, 10, 5, 30)
    val fixedInstant = fixedLocalDateTime
      .atZone(ZoneId.systemDefault())
      .toInstant

    // Create a custom fixed clock
    implicit val fixedClock: Clock[IO] = new CustomFixedClock(fixedInstant)

    val resetInterval = 7.minutes

    // Expected next reset at 10:07:00
    // Current time: 10:05:30
    // Delay should be 1 minute 30 seconds = 90 seconds
    val expectedDelay = 90.seconds

    val calculatedDelay = calculateInitialDelay(resetInterval).unsafeRunSync()

    calculatedDelay shouldBe expectedDelay
  }
}
