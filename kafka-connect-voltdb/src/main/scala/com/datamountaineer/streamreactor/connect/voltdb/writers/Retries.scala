package com.datamountaineer.streamreactor.connect.voltdb.writers

import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.util.Try

trait Retries extends StrictLogging {
  def withRetries[T](retries: Int, retryInterval: Long, errorMessage: Option[String])(thunk: => T): T = {
    try {
      thunk
    }
    catch {
      case t: Throwable =>
        errorMessage.foreach(m => logger.error(m, t))
        if (retries - 1 <= 0) throw t
        Try(Thread.sleep(retryInterval))
        withRetries(retries - 1, retryInterval, errorMessage)(thunk)
    }
  }
}
