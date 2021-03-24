/*
 *
 *  * Copyright 2020 Lenses.io.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.datamountaineer.streamreactor.common.source

/**
  * Created by andrew@datamountaineer.com on 03/03/2017. 
  * kafka-connect-common
  */

import java.time.Clock
import java.time.Duration
import java.time.Instant

class ExponentialBackOff(step: Duration, cap: Duration, iteration: Int = 0, clock: Clock = Clock.systemUTC(), first: Boolean = true) {
  def now: Instant = Instant.now(clock)
  val endTime: Instant = now.plus(exponentialInterval(iteration))

  def remaining: Duration = Duration.between(now, endTime)

  def passed: Boolean = now.isAfter(this.endTime)

  def nextSuccess(): ExponentialBackOff = {
    new ExponentialBackOff(step, cap, 0, clock, false)
  }

  def nextFailure(): ExponentialBackOff = {
    new ExponentialBackOff(step, cap, iteration + 1, clock, false)
  }

  private def exponentialInterval(i: Int): Duration = {
    if (first) Duration.ofMillis(-1) else Duration.ofMillis(Math.min(cap.toMillis, step.toMillis * Math.pow(2, i).toLong))
  }
}

