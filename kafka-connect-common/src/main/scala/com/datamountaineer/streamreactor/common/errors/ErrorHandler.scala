/*
 *  Copyright 2017 Datamountaineer.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.datamountaineer.streamreactor.common.errors

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.scalalogging.StrictLogging

import scala.util.{Failure, Success, Try}

/**
  * Created by andrew@datamountaineer.com on 29/05/16. 
  * stream-reactor-maven
  */
trait ErrorHandler extends StrictLogging {
  var errorTracker: Option[ErrorTracker] = None
  private val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'")

  def initialize(maxRetries: Int, errorPolicy: ErrorPolicy): Unit = {
    errorTracker = Some(ErrorTracker(maxRetries, maxRetries, "", new Date(), errorPolicy))
  }

  def getErrorTrackerRetries() : Int = {
    errorTracker.get.retries
  }

  def errored() : Boolean = {
    errorTracker.get.retries != errorTracker.get.maxRetries
  }

  def handleTry[A](t : Try[A]) : Option[A] = {
    require(errorTracker.isDefined, "ErrorTracker is not set call. Initialize.")
    t
    match {
      case Success(s) => {
        //success, check if we had previous errors.
        if (errorTracker.get.retries != errorTracker.get.maxRetries) {
          logger.info(s"Recovered from error [${errorTracker.get.lastErrorMessage}] at " +
            s"${dateFormatter.format(errorTracker.get.lastErrorTimestamp)}")
        }
        //cleared error
        resetErrorTracker()
        Some(s)
      }
      case Failure(f) =>
        //decrement the retry count
        logger.error(s"Encountered error [${f.getMessage}]", f)
        this.errorTracker = Some(decrementErrorTracker(errorTracker.get, f.getMessage))
        handleError(f, errorTracker.get.retries, errorTracker.get.policy)
        None
    }
  }

  def resetErrorTracker() = {
    errorTracker = Some(ErrorTracker(errorTracker.get.maxRetries, errorTracker.get.maxRetries, "", new Date(),
      errorTracker.get.policy))
  }

  private def decrementErrorTracker(errorTracker: ErrorTracker, msg: String): ErrorTracker = {
    if (errorTracker.maxRetries == -1) {
      ErrorTracker(errorTracker.retries, errorTracker.maxRetries, msg, new Date(), errorTracker.policy)
    } else {
      ErrorTracker(errorTracker.retries - 1, errorTracker.maxRetries, msg, new Date(), errorTracker.policy)
    }
  }

  private def handleError(f: Throwable, retries: Int, policy: ErrorPolicy): Unit = {
    policy.handle(f, true, retries)
  }
}
