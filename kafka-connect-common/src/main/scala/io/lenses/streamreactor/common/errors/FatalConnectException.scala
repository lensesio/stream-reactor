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

/**
 * Marker subclass of `ConnectException` that signals an unrecoverable error.
 *
 * ALL `ErrorPolicy` implementations MUST rethrow this as-is:
 *   - `RetryErrorPolicy` MUST NOT wrap it in `RetriableException` (regardless of remaining retries).
 *   - `NoopErrorPolicy` MUST NOT swallow it with a warning log.
 *   - `ThrowErrorPolicy` MUST NOT wrap it in a plain `ConnectException`.
 *
 * Used by sinks to fail-fast on tampering / data-integrity violations where retrying or
 * ignoring the error is guaranteed to lose data.
 */
class FatalConnectException(message: String, cause: Throwable) extends ConnectException(message, cause) {
  def this(message: String) = this(message, null)
}

/**
 * Marker subclass of `FatalConnectException` for integrity-sensitive transient errors.
 *
 * Unlike `FatalConnectException`, this class signals that Kafka Connect SHOULD re-deliver
 * the same batch (via `RetriableException`) under `error.policy=RETRY` — the operation is
 * safe to retry because no partial state was cached. Under `error.policy=NOOP` and
 * `error.policy=THROW` it behaves identically to `FatalConnectException` (fail-fast),
 * because silently advancing past the error would produce data loss.
 *
 * Concrete example: a transient cloud read-timeout while loading a granular lock prevents
 * the connector from knowing the correct deduplication floor for the current partition key.
 * Retrying the same `put()` is safe; swallowing the error and continuing is not.
 *
 * Policy handling:
 *   - `RetryErrorPolicy`: wraps in `RetriableException` while retries remain; rethrows as-is when exhausted.
 *   - `NoopErrorPolicy`: rethrows as-is (via the `FatalConnectException` case — no change needed).
 *   - `ThrowErrorPolicy`: rethrows as-is (via the `FatalConnectException` case — no change needed).
 */
class RetriableIntegrityException(message: String, cause: Throwable) extends FatalConnectException(message, cause) {
  def this(message: String) = this(message, null)
}
