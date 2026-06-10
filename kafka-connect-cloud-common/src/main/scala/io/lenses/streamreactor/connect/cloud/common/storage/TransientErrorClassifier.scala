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
package io.lenses.streamreactor.connect.cloud.common.storage

import java.io.EOFException
import java.io.InterruptedIOException
import java.net.SocketException
import java.net.SocketTimeoutException
import javax.net.ssl.SSLException

/**
 * Classifies a `Throwable` as transient (safe to retry) or permanent (fail immediately).
 *
 * Implementations MUST be allowlist-based: return `true` only for exceptions
 * known to be transient. Unknown exceptions MUST be classified as permanent so
 * that fencing errors (e.g. `412` precondition failures) are never retried.
 */
trait TransientErrorClassifier {
  def isTransient(t: Throwable): Boolean
}

/**
 * Default classifier covering the JDK network exception hierarchy plus a small
 * set of recognisable message substrings.
 *
 * Walks the cause chain so that SDK exceptions that wrap JDK network errors
 * (e.g. `com.google.cloud.storage.StorageException` wrapping `EOFException`)
 * are also classified as transient when the root cause is transient.
 *
 * Allowlist-by-default: any exception type not in this list is classified as
 * permanent. This is intentional — `412` precondition failures (the zombie-
 * fencing signal) must NOT be retried.
 */
object DefaultTransientErrorClassifier extends TransientErrorClassifier {

  private val TransientMessageSubstrings: Seq[String] = Seq(
    "unexpected end of file",
    "connection reset",
    "broken pipe",
    "connection closed",
    "connection timed out",
    "network is unreachable",
    "no route to host",
    "too many open files",
  )

  override def isTransient(t: Throwable): Boolean = {
    var current = t
    while (current != null) {
      if (isDirectlyTransient(current)) return true
      current = current.getCause
    }
    false
  }

  private def isDirectlyTransient(t: Throwable): Boolean =
    t match {
      case _: SocketException        => true
      case _: SocketTimeoutException => true
      case _: SSLException           => true
      case _: EOFException           => true
      case _: InterruptedIOException => true
      case other =>
        val msg = Option(other.getMessage).map(_.toLowerCase).getOrElse("")
        TransientMessageSubstrings.exists(msg.contains)
    }
}
