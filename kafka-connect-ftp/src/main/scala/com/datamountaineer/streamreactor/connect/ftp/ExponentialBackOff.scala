package com.datamountaineer.streamreactor.connect.ftp

import java.time.{Duration, Instant}

class ExponentialBackOff(step: Duration, cap: Duration, iteration: Int = 0) {
  val endTime = Instant.now.plus(interval(iteration))

  private def interval(i: Int) = Duration.ofMillis(
    Math.min(
      cap.toMillis,
      step.toMillis * Math.pow(2, i).toLong
    )
  )

  def remaining: Duration = Duration.between(Instant.now, endTime)

  def passed: Boolean = Instant.now.isAfter(this.endTime)

  def nextSuccess: ExponentialBackOff = new ExponentialBackOff(step, cap)

  def nextFailure: ExponentialBackOff = new ExponentialBackOff(step, cap, iteration + 1)
}