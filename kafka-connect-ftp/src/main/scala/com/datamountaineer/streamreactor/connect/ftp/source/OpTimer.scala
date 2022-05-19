package com.datamountaineer.streamreactor.connect.ftp.source

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
