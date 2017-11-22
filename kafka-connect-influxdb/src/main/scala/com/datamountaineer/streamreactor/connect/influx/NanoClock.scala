package com.datamountaineer.streamreactor.connect.influx

import java.time.{Clock, Instant, ZoneId}
import java.time.temporal.ChronoField

/**
  * Trying to work against JVM time precision where Nano support is not provided until Java 9.
  * @param clock
  */
class NanoClock(val clock: Clock){
  private var initialNanos = getSystemNanos
  private var initialInstant:Instant = clock.instant()

  def this() = {
    this(Clock.systemUTC)
  }

  def getZone = clock.getZone

  def instant = initialInstant.plusNanos(getSystemNanos - initialNanos)

  def withZone(zone: ZoneId) = new NanoClock(clock.withZone(zone))

  def getEpochNanos = {
    val now = instant
    now.toEpochMilli * 1000000l + (now.getLong(ChronoField.NANO_OF_SECOND)%1000000)
  }
  private def getSystemNanos = System.nanoTime
}