/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.ftp.source

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

  def nextSuccess(): ExponentialBackOff = new ExponentialBackOff(step, cap)

  def nextFailure(): ExponentialBackOff = new ExponentialBackOff(step, cap, iteration + 1)
}