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
package io.lenses.streamreactor.connect.gcp.storage.storage

import com.google.cloud.storage.StorageException
import io.lenses.streamreactor.connect.cloud.common.storage.DefaultTransientErrorClassifier
import io.lenses.streamreactor.connect.cloud.common.storage.TransientErrorClassifier

/**
 * GCS-specific transient-error classifier.
 *
 * Extends [[DefaultTransientErrorClassifier]] (JDK network exceptions) with
 * inspection of [[StorageException]] HTTP status codes:
 *
 * Transient (retryable):
 *   - Code 0  — network-layer error (no HTTP response; wraps an IOException)
 *   - Code 429 — Too Many Requests (throttling)
 *   - 500–599  — GCS server-side errors
 *
 * Permanent (never retried):
 *   - Code 412 — Precondition Failed; this is the zombie-fencing signal and
 *                MUST remain fatal.
 *   - Code 409 — Conflict (concurrent create race on NoOverwrite paths)
 *   - Code 404 — Not Found
 *   - Any other 4xx — client errors that will not resolve on retry
 *
 * The cause chain is walked so that a `FileMoveError` wrapping a
 * `StorageException` wrapping an `IOException` is correctly classified.
 */
object GCPTransientErrorClassifier extends TransientErrorClassifier {

  override def isTransient(t: Throwable): Boolean = {
    var current = t
    while (current != null) {
      current match {
        case se: StorageException =>
          return isStorageExceptionTransient(se)
        case _ =>
          current = current.getCause
      }
    }
    // No StorageException in the chain; fall back to the JDK classifier.
    DefaultTransientErrorClassifier.isTransient(t)
  }

  private def isStorageExceptionTransient(se: StorageException): Boolean = {
    val code = se.getCode
    if (code == 0 || code == 429 || (code >= 500 && code <= 599)) {
      true
    } else if (code == 412 || code == 409 || (code >= 400 && code <= 499)) {
      false
    } else {
      // Unknown code: fall back to inspecting the cause chain with the JDK classifier.
      Option(se.getCause).exists(DefaultTransientErrorClassifier.isTransient)
    }
  }
}
