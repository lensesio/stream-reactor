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
package io.lenses.streamreactor.connect.hbase.kerberos.utils

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.duration.Duration

class AsyncFunctionLoop(interval: Duration, description: String)(thunk: => Unit)
    extends AutoCloseable
    with StrictLogging {

  private val running         = new AtomicBoolean(false)
  private val executorService = Executors.newSingleThreadExecutor

  def start(): Unit = {
    if (!running.compareAndSet(false, true)) {
      throw new IllegalStateException(s"$description already running.")
    }
    logger.info(s"Starting $description loop with an interval of ${interval.toMillis}ms.")
    executorService.submit(
      new Runnable {
        override def run(): Unit =
          while (running.get()) {
            try {
              Thread.sleep(interval.toMillis)
              thunk
            } catch {
              case _: InterruptedException =>
              case t: Throwable =>
                logger.warn("Failed to renew the Kerberos ticket", t)
            }
          }
      },
    )
    ()
  }

  override def close(): Unit = {
    if (running.compareAndSet(true, false)) {
      executorService.shutdownNow()
      executorService.awaitTermination(10000, TimeUnit.MILLISECONDS)
    }
    ()
  }
}
