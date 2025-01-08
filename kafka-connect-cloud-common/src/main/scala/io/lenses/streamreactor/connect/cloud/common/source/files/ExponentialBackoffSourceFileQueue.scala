/*
 * Copyright 2017-2025 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.cloud.common.source.files

import cats.implicits.toShow
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.Duration

trait TimeProvider {
  def nanoTime(): Long
}

object SystemTimeProvider extends TimeProvider {
  override def nanoTime(): Long = System.nanoTime()
}

object ExponentialBackoffSourceFileQueue {
  def apply(
    delegate:        SourceFileQueue,
    maxTimeout:      Duration,
    initialDelay:    Duration,
    backoffFactor:   Double = 2.0,
    timeProvider:    TimeProvider,
    connectorTaskId: ConnectorTaskId,
  ): Either[IllegalArgumentException, ExponentialBackoffSourceFileQueue] =
    if (maxTimeout <= Duration.Zero) {
      Left(new IllegalArgumentException("maxTimeout must be positive"))
    } else if (initialDelay <= Duration.Zero) {
      Left(new IllegalArgumentException("initialDelay must be positive"))
    } else if (backoffFactor <= 1.0) {
      Left(new IllegalArgumentException("backoffFactor must be greater than 1"))
    } else {
      Right(new ExponentialBackoffSourceFileQueue(delegate,
                                                  maxTimeout,
                                                  initialDelay,
                                                  backoffFactor,
                                                  timeProvider,
                                                  connectorTaskId,
      ))
    }
}
class ExponentialBackoffSourceFileQueue private (
  delegate:        SourceFileQueue,
  maxTimeout:      Duration,
  initialDelay:    Duration,
  backoffFactor:   Double,
  timeProvider:    TimeProvider,
  connectorTaskId: ConnectorTaskId,
) extends SourceFileQueue
    with StrictLogging {

  // Atomic state to ensure thread safety
  private case class BackoffState(
    delay:           Duration,
    nextAttemptTime: Long,
    isFirstBackoff:  Boolean,
  )

  private val stateRef = new AtomicReference[BackoffState](
    BackoffState(
      delay           = initialDelay,
      nextAttemptTime = 0L,
      isFirstBackoff  = true,
    ),
  )

  override def next(): Either[FileListError, Option[CloudLocation]] = {
    val currentTime  = timeProvider.nanoTime()
    val currentState = stateRef.get()

    if (currentTime < currentState.nextAttemptTime) {
      logBackoffAttempt(currentState)
      Right(None)
    } else {
      delegate.next() match {
        case right @ Right(Some(_)) =>
          resetSuccessState()
          right

        case right @ Right(None) =>
          val newDelay = calculateBackoffDelay(currentState)
          updateRetryState(currentTime, newDelay)
          right

        case left @ Left(_) => left
      }
    }
  }

  private def resetSuccessState(): Unit = {
    val successState = stateRef.get().copy(
      delay           = initialDelay,
      nextAttemptTime = 0L,
      isFirstBackoff  = true,
    )
    stateRef.set(successState)
    logger.debug(s"[${connectorTaskId.show}] Found file; resetting backoff state")
  }

  private def calculateBackoffDelay(currentState: BackoffState): Duration = {
    val newDelay = if (currentState.isFirstBackoff) {
      logger.info(s"[${connectorTaskId.show}] No files found; retrying in ${currentState.delay.toMillis} milliseconds")
      currentState.delay
    } else {
      val exponentialDelay = (currentState.delay * backoffFactor) min maxTimeout
      logger.info(s"[${connectorTaskId.show}] No files found; retrying in ${exponentialDelay.toMillis} milliseconds")
      exponentialDelay
    }
    newDelay
  }

  private def updateRetryState(currentTime: Long, newDelay: Duration): Unit = {
    val retryState = stateRef.get().copy(
      delay           = newDelay,
      nextAttemptTime = currentTime + newDelay.toNanos,
      isFirstBackoff  = false,
    )
    stateRef.set(retryState)
  }

  private def logBackoffAttempt(currentState: BackoffState): Unit =
    logger.debug(
      s"[${connectorTaskId.show}] Backing off; next attempt at ${DateTimeFormatter.ISO_INSTANT.format(Instant.ofEpochMilli(currentState.nextAttemptTime / 1000000))}",
    )
}
