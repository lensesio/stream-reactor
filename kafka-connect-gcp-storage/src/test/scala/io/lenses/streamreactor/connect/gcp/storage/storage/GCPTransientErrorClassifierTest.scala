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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.EOFException
import java.net.SocketException

class GCPTransientErrorClassifierTest extends AnyFlatSpec with Matchers {

  private val classifier = GCPTransientErrorClassifier

  // ---- StorageException-based cases ----

  "GCPTransientErrorClassifier" should "classify StorageException code 0 (network error) as transient" in {
    classifier.isTransient(new StorageException(0, "network error")) shouldBe true
  }

  it should "classify StorageException code 429 (throttling) as transient" in {
    classifier.isTransient(new StorageException(429, "Too Many Requests")) shouldBe true
  }

  it should "classify StorageException code 500 as transient" in {
    classifier.isTransient(new StorageException(500, "Internal Server Error")) shouldBe true
  }

  it should "classify StorageException code 503 as transient" in {
    classifier.isTransient(new StorageException(503, "Service Unavailable")) shouldBe true
  }

  it should "classify StorageException code 412 (precondition failed / fencing signal) as permanent" in {
    classifier.isTransient(new StorageException(412, "Precondition Failed")) shouldBe false
  }

  it should "classify StorageException code 409 (conflict) as permanent" in {
    classifier.isTransient(new StorageException(409, "Conflict")) shouldBe false
  }

  it should "classify StorageException code 404 (not found) as permanent" in {
    classifier.isTransient(new StorageException(404, "Not Found")) shouldBe false
  }

  it should "classify StorageException code 403 (forbidden) as permanent" in {
    classifier.isTransient(new StorageException(403, "Forbidden")) shouldBe false
  }

  it should "classify StorageException code 400 (bad request) as permanent" in {
    classifier.isTransient(new StorageException(400, "Bad Request")) shouldBe false
  }

  // ---- Cause-chain cases ----

  it should "classify RuntimeException wrapping a StorageException(0) as transient (cause chain)" in {
    val storageEx = new StorageException(0, "network")
    val wrapped   = new RuntimeException("outer", storageEx)
    classifier.isTransient(wrapped) shouldBe true
  }

  it should "classify RuntimeException wrapping a StorageException(412) as permanent" in {
    val storageEx = new StorageException(412, "Precondition Failed")
    val wrapped   = new RuntimeException("outer", storageEx)
    classifier.isTransient(wrapped) shouldBe false
  }

  // ---- JDK network exception fallback (no StorageException in chain) ----

  it should "classify SocketException (no StorageException in chain) as transient via JDK fallback" in {
    classifier.isTransient(new SocketException("Connection reset")) shouldBe true
  }

  it should "classify EOFException (no StorageException in chain) as transient via JDK fallback" in {
    classifier.isTransient(new EOFException("Unexpected end of file from server")) shouldBe true
  }

  it should "classify a plain RuntimeException with no transient context as permanent" in {
    classifier.isTransient(new RuntimeException("some unrelated error")) shouldBe false
  }

  // ---- The specific reported failure mode ----

  it should "classify 'Unexpected end of file from server' wrapped in a StorageException(0) as transient" in {
    val cause     = new EOFException("Unexpected end of file from server")
    val storageEx = new StorageException(0, "unexpected end of file", cause)
    classifier.isTransient(storageEx) shouldBe true
  }
}
