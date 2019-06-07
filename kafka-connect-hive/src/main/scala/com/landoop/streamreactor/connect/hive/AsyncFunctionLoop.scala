package com.landoop.streamreactor.connect.hive

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.slf4j.StrictLogging

import scala.concurrent.duration.Duration

class AsyncFunctionLoop(interval: Duration, description: String)(thunk: => Unit)
  extends AutoCloseable
    with StrictLogging {

  @volatile private var running = false
  private val executorService = Executors.newFixedThreadPool(1)

  def start(): Unit = {
    if (running) {
      throw new IllegalStateException(s"$description already running.")
    }
    logger.info(s"Starting $description loop with an interval of ${interval.toMillis}ms.")
    running = true
    executorService.submit(new Runnable {
      override def run(): Unit = {
        while (running) {
          try {
            Thread.sleep(interval.toMillis)
            thunk
          }
          catch {
            case _: InterruptedException =>
            case t: Throwable =>
          }
        }
      }
    })

    executorService.shutdown()
  }

  override def close(): Unit = {
    if (running) {
      running = false
      executorService.shutdownNow()
      executorService.awaitTermination(60000, TimeUnit.MILLISECONDS)
    }
  }
}
