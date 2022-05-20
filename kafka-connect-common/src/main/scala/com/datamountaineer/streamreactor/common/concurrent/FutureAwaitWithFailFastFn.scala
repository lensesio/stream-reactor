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

package com.datamountaineer.streamreactor.common.concurrent

import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

import com.typesafe.scalalogging.StrictLogging

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Failure

object FutureAwaitWithFailFastFn extends StrictLogging {

  def apply(executorService: ExecutorService, futures: Seq[Future[Unit]], duration: Duration): Unit = {
    //make sure we ask the executor to shutdown to ensure the process exits
    executorService.shutdown()

    val promise = Promise[Boolean]()

    //stop on the first failure
    futures.foreach { f =>
      f.failed.foreach { t =>
        if (promise.tryFailure(t)) {
          executorService.shutdownNow()
        }
      }
    }

    val fut = Future.sequence(futures)
    fut.foreach { _ =>
      if (promise.trySuccess(true)) {
        val failed = executorService.shutdownNow()
        if (failed.size() > 0) {
          logger.error(s"${failed.size()} task have failed.")
        }
      }
    }

    Await.ready(promise.future, duration).value match {
      case Some(Failure(t)) =>
        executorService.awaitTermination(1, TimeUnit.MINUTES)
        //throw the underlying error
        throw t

      case _ =>
        val _ = executorService.awaitTermination(1, TimeUnit.MINUTES)
    }
  }

  def apply[T](executorService: ExecutorService, futures: Seq[Future[T]], duration: Duration = 1.hours): Seq[T] = {
    //make sure we ask the executor to shutdown to ensure the process exits
    executorService.shutdown()

    val promise = Promise[Boolean]()

    //stop on the first failure
    futures.foreach { f =>
      f.failed.foreach { t =>
        if (promise.tryFailure(t)) {
          executorService.shutdownNow()
        }
      }
    }

    val fut = Future.sequence(futures)
    fut.foreach { _ =>
      if (promise.trySuccess(true)) {
        val failed = executorService.shutdownNow()
        if (failed.size() > 0) {
          logger.error(s"${failed.size()} task have failed.")
        }
      }
    }

    Await.ready(promise.future, duration).value match {
      case Some(Failure(t)) =>
        executorService.awaitTermination(1, TimeUnit.MINUTES)
        //throw the underlying error
        throw t

      case _ =>
        executorService.awaitTermination(1, TimeUnit.MINUTES)
        //return the result from each of the futures
        Await.result(Future.sequence(futures), 1.minute)
    }
  }
}
