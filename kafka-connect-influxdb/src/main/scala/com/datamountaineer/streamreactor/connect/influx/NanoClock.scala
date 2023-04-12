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
package com.datamountaineer.streamreactor.connect.influx

import java.time.temporal.ChronoField
import java.time.Clock
import java.time.Instant
import java.time.ZoneId

/**
  * Trying to work against JVM time precision where Nano support is not provided until Java 9.
  * @param clock
  */
class NanoClock(val clock: Clock) {
  private val initialNanos = getSystemNanos
  private val initialInstant: Instant = clock.instant()

  def this() = {
    this(Clock.systemUTC)
  }

  def instant = initialInstant.plusNanos(getSystemNanos - initialNanos)

  def withZone(zone: ZoneId) = new NanoClock(clock.withZone(zone))

  def getEpochNanos: Long = {
    val now = instant
    now.toEpochMilli * 1000000L + (now.getLong(ChronoField.NANO_OF_SECOND) % 1000000)
  }
  private def getSystemNanos = System.nanoTime
}
