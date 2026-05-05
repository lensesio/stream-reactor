/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.common.errors

import java.util.Date
import ErrorPolicyEnum.ErrorPolicyEnum
import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.RetriableException

/**
 * Created by andrew@datamountaineer.com on 19/05/16.
 * kafka-connect-common
 */
object ErrorPolicyEnum extends Enumeration {
  type ErrorPolicyEnum = Value
  val NOOP, THROW, RETRY = Value
}

case class ErrorTracker(
  retries:            Int,
  maxRetries:         Int,
  lastErrorMessage:   String,
  lastErrorTimestamp: Date,
  policy:             ErrorPolicy,
)

trait ErrorPolicy extends StrictLogging {
  def handle(error: Throwable, sink: Boolean = true, retryCount: Int = 0): Unit
}

object ErrorPolicy extends StrictLogging {
  def apply(policy: ErrorPolicyEnum): ErrorPolicy =
    policy match {
      case ErrorPolicyEnum.NOOP  => NoopErrorPolicy()
      case ErrorPolicyEnum.THROW => ThrowErrorPolicy()
      case ErrorPolicyEnum.RETRY => RetryErrorPolicy()
    }
}

case class NoopErrorPolicy() extends ErrorPolicy {
  override def handle(error: Throwable, sink: Boolean = true, retryCount: Int = 0): Unit =
    error match {
      case fatal: FatalConnectException =>
        // Fail-fast: NOOP must NOT swallow data-integrity violations.
        throw fatal
      case _ =>
        logger.warn(s"Error policy NOOP: [${error.getMessage}]. Processing continuing.")
    }
}

case class ThrowErrorPolicy() extends ErrorPolicy {
  override def handle(error: Throwable, sink: Boolean = true, retryCount: Int = 0): Unit =
    error match {
      case fatal: FatalConnectException =>
        // Preserve the marker subclass at the top of the propagated exception so
        // downstream observability filters that distinguish fatal-marker vs plain
        // ConnectException work uniformly across all error.policy configurations.
        throw fatal
      case _ =>
        throw new ConnectException(error)
    }
}

case class RetryErrorPolicy() extends ErrorPolicy {

  override def handle(error: Throwable, sink: Boolean = true, retryCount: Int): Unit =
    error match {
      // RetriableIntegrityException MUST be checked before FatalConnectException because it
      // is a subclass.  Under RETRY we re-deliver via RetriableException while retries remain;
      // on exhaustion we rethrow as-is so NOOP-style integrity guarantees are still upheld.
      case ri: RetriableIntegrityException if retryCount > 0 =>
        logger.warn(s"Error policy set to RETRY (integrity). Remaining attempts [$retryCount]")
        throw new RetriableException(ri)
      case ri: RetriableIntegrityException =>
        throw ri
      case fatal: FatalConnectException =>
        // Fail-fast: never wrap in RetriableException; never schedule an in-process
        // retry attempt. Note: ErrorHandler.handleTry decrements the in-memory retry
        // tracker once BEFORE invoking this policy, so the tracker DOES decrement on
        // the fatal path; that decrement is harmless because the rethrow propagates
        // out of handleTry, Connect kills the task before any further put() runs,
        // and the tracker resets on the new task instance.
        throw fatal
      case _ if retryCount == 0 =>
        throw new ConnectException(error)
      case _ =>
        logger.warn(s"Error policy set to RETRY. Remaining attempts [$retryCount]")
        throw new RetriableException(error)
    }
}
