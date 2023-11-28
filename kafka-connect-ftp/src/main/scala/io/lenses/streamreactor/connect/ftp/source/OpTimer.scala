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
package io.lenses.streamreactor.connect.ftp.source

import com.typesafe.scalalogging.LazyLogging

object OpTimer {
  def apply(timerName: String = "noName"): OpTimerTiming = {
    val stack = new IllegalArgumentException().getStackTrace
    OpTimerTiming(timerName, stack, System.currentTimeMillis())
  }

  def profile[T](lineName: String, f: => T): T = {
    val timer = OpTimer(lineName)
    val res: T = f
    timer.complete()
    res
  }

}

case class OpTimerTiming(timerName: String, startStackTrace: Array[StackTraceElement], startTime: Long) {
  def complete(): OpTimerComplete =
    OpTimerComplete(timerName, startStackTrace, startTime, System.currentTimeMillis())
}

case class OpTimerComplete(timerName: String, startStackTrace: Array[StackTraceElement], startTime: Long, endTime: Long)
    extends LazyLogging {
  val time = endTime - startTime
  if (time > 100) {
    //logger.error("OpTimer {}: {} StackTrace: {}", timerName, time, startStackTrace)
  } else {
    //logger.info("OpTimer {}: {}", timerName, time)
  }
}
