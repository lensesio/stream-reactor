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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.EOFException
import java.io.InterruptedIOException
import java.net.SocketException
import java.net.SocketTimeoutException
import javax.net.ssl.SSLException

class TransientErrorClassifierTest extends AnyFlatSpec with Matchers {

  private val classifier = DefaultTransientErrorClassifier

  // ---- transient cases ----

  "DefaultTransientErrorClassifier" should "classify SocketException as transient" in {
    classifier.isTransient(new SocketException("Connection reset")) shouldBe true
  }

  it should "classify SocketTimeoutException as transient" in {
    classifier.isTransient(new SocketTimeoutException("Read timed out")) shouldBe true
  }

  it should "classify SSLException as transient" in {
    classifier.isTransient(new SSLException("Unexpected end of file")) shouldBe true
  }

  it should "classify EOFException as transient" in {
    classifier.isTransient(new EOFException("Unexpected end of file from server")) shouldBe true
  }

  it should "classify InterruptedIOException as transient" in {
    classifier.isTransient(new InterruptedIOException("interrupted")) shouldBe true
  }

  it should "classify exception with 'unexpected end of file' message as transient" in {
    classifier.isTransient(new RuntimeException("Unexpected end of file from server")) shouldBe true
  }

  it should "classify exception with 'connection reset' message as transient" in {
    classifier.isTransient(new RuntimeException("Connection reset by peer")) shouldBe true
  }

  it should "classify exception with 'broken pipe' message as transient" in {
    classifier.isTransient(new RuntimeException("Broken pipe")) shouldBe true
  }

  it should "classify exception with 'connection closed' message as transient" in {
    classifier.isTransient(new RuntimeException("connection closed")) shouldBe true
  }

  it should "walk the cause chain and classify as transient when cause is a SocketException" in {
    val cause   = new SocketException("Connection reset")
    val wrapped = new RuntimeException("outer wrapper", cause)
    classifier.isTransient(wrapped) shouldBe true
  }

  it should "walk multi-level cause chain and classify as transient" in {
    val root   = new EOFException("Unexpected end of file from server")
    val middle = new RuntimeException("middle", root)
    val outer  = new RuntimeException("outer", middle)
    classifier.isTransient(outer) shouldBe true
  }

  // ---- permanent cases ----

  it should "classify a plain RuntimeException with non-transient message as permanent" in {
    classifier.isTransient(new RuntimeException("some other error")) shouldBe false
  }

  it should "classify IllegalArgumentException as permanent" in {
    classifier.isTransient(new IllegalArgumentException("bad argument")) shouldBe false
  }

  it should "classify a null-cause exception with no matching message as permanent" in {
    classifier.isTransient(new Exception("412 Precondition Failed")) shouldBe false
  }

  it should "classify an exception with null message and no transient cause as permanent" in {
    classifier.isTransient(new RuntimeException(null.asInstanceOf[String])) shouldBe false
  }
}
