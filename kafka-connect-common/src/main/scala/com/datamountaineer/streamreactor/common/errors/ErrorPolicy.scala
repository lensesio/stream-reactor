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

import java.util.Date

import com.datamountaineer.streamreactor.common.errors.ErrorPolicyEnum.ErrorPolicyEnum
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.RetriableException

/**
  * Created by andrew@datamountaineer.com on 19/05/16. 
  * kafka-connect-common
  */
object ErrorPolicyEnum extends Enumeration {
  type ErrorPolicyEnum = Value
  val NOOP, THROW, RETRY = Value
}

case class ErrorTracker(retries: Int, maxRetries: Int, lastErrorMessage: String, lastErrorTimestamp: Date, policy: ErrorPolicy)

trait ErrorPolicy extends StrictLogging {
  def handle(error: Throwable, sink: Boolean = true, retryCount: Int = 0)
}

object ErrorPolicy extends StrictLogging {
  def apply(policy: ErrorPolicyEnum): ErrorPolicy = {
    policy match {
      case ErrorPolicyEnum.NOOP => NoopErrorPolicy()
      case ErrorPolicyEnum.THROW => ThrowErrorPolicy()
      case ErrorPolicyEnum.RETRY => RetryErrorPolicy()
    }
  }
}

case class NoopErrorPolicy() extends ErrorPolicy {
  override def handle(error: Throwable, sink: Boolean = true, retryCount: Int = 0){
    logger.warn(s"Error policy NOOP: [${error.getMessage}]. Processing continuing.")
  }
}

case class ThrowErrorPolicy() extends ErrorPolicy {
  override def handle(error: Throwable, sink: Boolean = true, retryCount: Int = 0){
    throw new RuntimeException(error)
  }
}

case class RetryErrorPolicy() extends ErrorPolicy {

  override def handle(error: Throwable, sink: Boolean = true, retryCount: Int) = {
    if (retryCount == 0) {
      throw new RuntimeException(error)
    }
    else {
      logger.warn(s"Error policy set to RETRY. Remaining attempts [$retryCount]")
      throw new RetriableException(error)
    }
  }
}