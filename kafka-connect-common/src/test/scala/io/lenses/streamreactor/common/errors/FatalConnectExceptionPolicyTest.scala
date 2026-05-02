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

import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.errors.RetriableException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Asserts the canonical "all three policies symmetrically rethrow `FatalConnectException` as-is"
 * contract.
 *
 * The contract prevents (a) `RetryErrorPolicy` from wrapping the marker in
 * `RetriableException` (which would let Connect retry an unrecoverable error in-process),
 * (b) `NoopErrorPolicy` from silently swallowing the marker (which would defeat the
 * fail-fast guarantee for users running NOOP), and (c) `ThrowErrorPolicy` from wrapping
 * the marker in plain `ConnectException` (which would hide the marker subclass under an
 * extra cause-chain layer and break observability filters that distinguish fatal-marker
 * vs plain `ConnectException`).
 */
class FatalConnectExceptionPolicyTest extends AnyFunSuite with Matchers {

  private val cause      = new IllegalStateException("/tmp/staging/missing.tmp")
  private val fatal      = new FatalConnectException("staging file disappeared", cause)
  private val transient  = new RuntimeException("transient")

  test("RetryErrorPolicy rethrows FatalConnectException as-is (regardless of remaining retries)") {
    val policy = RetryErrorPolicy()
    val thrown = intercept[FatalConnectException] {
      policy.handle(fatal, sink = true, retryCount = 5)
    }
    (thrown should be theSameInstanceAs fatal)
    thrown.getCause should be theSameInstanceAs cause
    thrown shouldNot be(a[RetriableException])
  }

  test("RetryErrorPolicy rethrows FatalConnectException as-is when retries are exhausted") {
    val policy = RetryErrorPolicy()
    val thrown = intercept[FatalConnectException] {
      policy.handle(fatal, sink = true, retryCount = 0)
    }
    (thrown should be theSameInstanceAs fatal)
  }

  test("RetryErrorPolicy still wraps non-fatal errors in RetriableException when retries remain") {
    val policy = RetryErrorPolicy()
    intercept[RetriableException] {
      policy.handle(transient, sink = true, retryCount = 3)
    }
  }

  test("RetryErrorPolicy wraps non-fatal errors in plain ConnectException when retries are exhausted") {
    val policy = RetryErrorPolicy()
    val thrown = intercept[ConnectException] {
      policy.handle(transient, sink = true, retryCount = 0)
    }
    thrown shouldNot be(a[RetriableException])
    thrown shouldNot be(a[FatalConnectException])
  }

  test("NoopErrorPolicy rethrows FatalConnectException as-is (does NOT silently swallow)") {
    val policy = NoopErrorPolicy()
    val thrown = intercept[FatalConnectException] {
      policy.handle(fatal)
    }
    (thrown should be theSameInstanceAs fatal)
    thrown.getCause should be theSameInstanceAs cause
  }

  test("NoopErrorPolicy continues silently for non-fatal errors") {
    val policy = NoopErrorPolicy()
    noException should be thrownBy policy.handle(transient)
  }

  test("ThrowErrorPolicy rethrows FatalConnectException as-is (NOT wrapped in plain ConnectException)") {
    val policy = ThrowErrorPolicy()
    val thrown = intercept[FatalConnectException] {
      policy.handle(fatal)
    }
    (thrown should be theSameInstanceAs fatal)
    thrown.getCause should be theSameInstanceAs cause
  }

  test("ThrowErrorPolicy wraps non-fatal errors in plain ConnectException") {
    val policy = ThrowErrorPolicy()
    val thrown = intercept[ConnectException] {
      policy.handle(transient)
    }
    thrown shouldNot be(a[FatalConnectException])
    thrown.getCause should be theSameInstanceAs transient
  }
}
