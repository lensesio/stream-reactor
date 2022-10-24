package com.datamountaineer.streamreactor.connect.influx2

import java.time.{Clock, Instant}
import java.time.temporal.ChronoField

/**
  * Trying to work against JVM time precision where Nano support is not provided until Java 9.
  * @param clock
  */
class NanoClock(val clock: Clock = Clock.systemUTC()) {

  private val initialNanos = getSystemNanos

  private val initialInstant: Instant = clock.instant()

  private def instant = initialInstant.plusNanos(getSystemNanos - initialNanos)

  def getEpochNanos: Long = {
    val now = instant
    now.toEpochMilli * 1000000L + (now.getLong(ChronoField.NANO_OF_SECOND) % 1000000)
  }
  private def getSystemNanos: Long = System.nanoTime
}
