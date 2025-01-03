/*
 * Copyright 2017-2024 Lenses.io Ltd
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
import cats.data.Validated
import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration._

class ExponentialBackoffSourceFileQueueTest extends AnyFunSuite with Matchers with EitherValues {

  implicit val cloudLocationValidator: CloudLocationValidator = (location: CloudLocation) => Validated.valid(location)
  private val connectorTaskId = ConnectorTaskId("demo", 3, 1)
  // Mock TimeProvider for testing
  class MockTimeProvider(private var currentTime: Long) extends TimeProvider {
    override def nanoTime(): Long = currentTime
    def advance(duration: Duration): Unit =
      currentTime += duration.toNanos
  }

  test("Backoff logic when delegate returns Right(None) repeatedly") {
    class DelayedSourceFileQueue(attemptsBeforeSuccess: Int) extends SourceFileQueue {
      private val attemptRef = new AtomicInteger(0)

      override def next(): Either[FileListError, Option[CloudLocation]] = {
        val attempt = attemptRef.incrementAndGet()
        if (attempt >= attemptsBeforeSuccess) {
          Right(Some(CloudLocation(s"/path/to/file$attempt")))
        } else {
          Right(None)
        }
      }
    }

    val mockTimeProvider = new MockTimeProvider(0L)
    val delegateQueue    = new DelayedSourceFileQueue(attemptsBeforeSuccess = 3)
    val backoffQueue = ExponentialBackoffSourceFileQueue(
      delegate      = delegateQueue,
      maxTimeout    = 1.second,
      initialDelay  = 100.millis,
      backoffFactor = 2.0,
      timeProvider  = mockTimeProvider,
      connectorTaskId,
    ).value

    // First call should attempt delegate and get Right(None)
    backoffQueue.next() shouldEqual Right(None)
    // Advance time by initialDelay
    mockTimeProvider.advance(100.millis)

    // Second call should attempt delegate and get Right(None), delay should double
    backoffQueue.next() shouldEqual Right(None)
    // Advance time by 200 ms (delay doubled)
    mockTimeProvider.advance(200.millis)

    // Third call should attempt delegate and get Right(Some(value)), backoff should reset
    backoffQueue.next().value shouldBe CloudLocation("/path/to/file3").some

    // Backoff state should be reset; next call should attempt immediately
    backoffQueue.next().value shouldBe CloudLocation("/path/to/file4").some
  }

  test("Backoff state resets after success") {
    class FlakySourceFileQueue extends SourceFileQueue {
      private val responses = Iterator(
        Right(None),
        Right(None),
        Right(Some(CloudLocation("/path/to/file1"))),
        Right(None),
        Right(Some(CloudLocation("/path/to/file2"))),
      )

      override def next(): Either[FileListError, Option[CloudLocation]] =
        if (responses.hasNext) responses.next()
        else Right(None)
    }

    val mockTimeProvider = new MockTimeProvider(0L)
    val delegateQueue    = new FlakySourceFileQueue
    val backoffQueue = ExponentialBackoffSourceFileQueue(
      delegate      = delegateQueue,
      maxTimeout    = 1.second,
      initialDelay  = 50.millis,
      backoffFactor = 2.0,
      timeProvider  = mockTimeProvider,
      connectorTaskId,
    ).value

    // First attempt: Right(None), backoff applies
    backoffQueue.next() shouldEqual Right(None)
    mockTimeProvider.advance(50.millis)

    // Second attempt: Right(None), delay doubles
    backoffQueue.next() shouldEqual Right(None)
    mockTimeProvider.advance(100.millis)

    // Third attempt: Right(Some(value)), backoff resets
    backoffQueue.next().value shouldBe CloudLocation("/path/to/file1").some

    // Backoff state reset; next attempt should not be delayed
    backoffQueue.next() shouldEqual Right(None)
    mockTimeProvider.advance(50.millis)

    // Next attempt: Right(Some(value))
    backoffQueue.next().value shouldBe CloudLocation("/path/to/file2").some
  }

  test("Errors are returned immediately without backoff") {
    class ErrorProneSourceFileQueue extends SourceFileQueue {
      override def next(): Either[FileListError, Option[CloudLocation]] =
        Left(FileListError(new RuntimeException("Simulated error"), "bucket", None))
    }

    val mockTimeProvider = new MockTimeProvider(0L)
    val delegateQueue    = new ErrorProneSourceFileQueue
    val backoffQueue = ExponentialBackoffSourceFileQueue(
      delegate      = delegateQueue,
      maxTimeout    = 1.second,
      initialDelay  = 100.millis,
      backoffFactor = 2.0,
      timeProvider  = mockTimeProvider,
      connectorTaskId,
    ).value

    backoffQueue.next().left.value.exception.getMessage shouldEqual "Simulated error"
    backoffQueue.next().left.value.exception.getMessage shouldEqual "Simulated error"
  }

  test("Parameter validation") {
    val mockDelegate = new SourceFileQueue {
      override def next(): Either[FileListError, Option[CloudLocation]] = Right(None)
    }

    ExponentialBackoffSourceFileQueue(
      delegate      = mockDelegate,
      maxTimeout    = Duration.Zero,
      initialDelay  = 100.millis,
      backoffFactor = 2.0,
      timeProvider  = new MockTimeProvider(0L),
      connectorTaskId,
    ).left.value should (be(an[IllegalArgumentException]) and have message "maxTimeout must be positive")

    ExponentialBackoffSourceFileQueue(
      delegate      = mockDelegate,
      maxTimeout    = 1.second,
      initialDelay  = Duration.Zero,
      backoffFactor = 2.0,
      timeProvider  = new MockTimeProvider(0L),
      connectorTaskId,
    ).left.value should (be(an[IllegalArgumentException]) and have message "initialDelay must be positive")

    ExponentialBackoffSourceFileQueue(
      delegate      = mockDelegate,
      maxTimeout    = 1.second,
      initialDelay  = 100.millis,
      backoffFactor = 1.0,
      timeProvider  = new MockTimeProvider(0L),
      connectorTaskId,
    ).left.value should (be(an[IllegalArgumentException]) and have message "backoffFactor must be greater than 1")
  }

  test("Backoff delay does not exceed maxTimeout") {
    class PersistentNoneSourceFileQueue extends SourceFileQueue {
      override def next(): Either[FileListError, Option[CloudLocation]] =
        Right(None)
    }

    val mockTimeProvider = new MockTimeProvider(0L)
    val delegateQueue    = new PersistentNoneSourceFileQueue
    val backoffQueue = ExponentialBackoffSourceFileQueue(
      delegate      = delegateQueue,
      maxTimeout    = 400.millis,
      initialDelay  = 50.millis,
      backoffFactor = 2.0,
      timeProvider  = mockTimeProvider,
      connectorTaskId,
    ).value

    // First attempt: delay is 50 ms
    backoffQueue.next() shouldEqual Right(None)
    mockTimeProvider.advance(50.millis)

    // Second attempt: delay is 100 ms
    backoffQueue.next() shouldEqual Right(None)
    mockTimeProvider.advance(100.millis)

    // Third attempt: delay is 200 ms
    backoffQueue.next() shouldEqual Right(None)
    mockTimeProvider.advance(200.millis)

    // Fourth attempt: delay should be capped at maxTimeout (400 ms)
    backoffQueue.next() shouldEqual Right(None)
    mockTimeProvider.advance(400.millis)

    val delay: Duration = useReflectionToRetrieveDelayDuration(backoffQueue)

    delay shouldEqual 400.millis
  }

  private def useReflectionToRetrieveDelayDuration(backoffQueue: ExponentialBackoffSourceFileQueue) = {
    // Ensure delay does not exceed maxTimeout
    // We can check the internal delay by accessing the private state using reflection
    val stateField = backoffQueue.getClass.getDeclaredField("stateRef")
    stateField.setAccessible(true)
    val stateRef   = stateField.get(backoffQueue).asInstanceOf[AtomicReference[_]]
    val state      = stateRef.get()
    val delayField = state.getClass.getDeclaredField("delay")
    delayField.setAccessible(true)
    delayField.get(state).asInstanceOf[Duration]
  }
}
