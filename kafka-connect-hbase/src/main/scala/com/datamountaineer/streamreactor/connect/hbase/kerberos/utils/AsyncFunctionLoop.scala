package com.datamountaineer.streamreactor.connect.hbase.kerberos.utils

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.Duration

class AsyncFunctionLoop(interval: Duration, description: String)(thunk: => Unit)
  extends AutoCloseable
    with StrictLogging {

  private val running = new AtomicBoolean(false)
  private val executorService = Executors.newSingleThreadExecutor

  def start(): Unit = {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException(s"$description already running.")
    }
    logger.info(s"Starting $description loop with an interval of ${interval.toMillis}ms.")
    executorService.submit(new Runnable {
      override def run(): Unit = {
        while (running.get()) {
          try {
            Thread.sleep(interval.toMillis)
            thunk
          }
          catch {
            case _: InterruptedException =>
            case t: Throwable =>
              logger.warn("Failed to renew the Kerberos ticket", t)
          }
        }
      }
    })
  }

  override def close(): Unit = {
    if (running.compareAndSet(true, false)) {
      executorService.shutdownNow()
      executorService.awaitTermination(10000, TimeUnit.MILLISECONDS)
    }
  }
}
