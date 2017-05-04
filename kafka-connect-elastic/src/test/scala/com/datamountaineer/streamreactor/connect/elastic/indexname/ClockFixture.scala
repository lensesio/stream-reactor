package com.datamountaineer.streamreactor.connect.elastic.indexname

import java.time.{Clock, Instant, ZoneOffset}

trait ClockFixture {
  val TestClock = Clock.fixed(Instant.parse("2016-10-02T14:00:00.00Z"), ZoneOffset.UTC)
}
