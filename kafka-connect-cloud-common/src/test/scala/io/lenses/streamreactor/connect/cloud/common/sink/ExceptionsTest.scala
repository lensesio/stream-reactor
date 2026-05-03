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
package io.lenses.streamreactor.connect.cloud.common.sink

import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

/**
 * Pins the contracts that `CloudSinkTask.handleErrors` depends on:
 *  - `BatchCloudSinkError.rollBack()` is true iff `fatal.nonEmpty` (so successful
 *    TPs are NOT rolled back when only some TPs failed).
 *  - `BatchCloudSinkError.topicPartitions()` returns ONLY the fatal TPs (so
 *    `cleanUp(tp)` runs ONLY for the failing TPs).
 *  - `BatchCloudSinkError.exception()` is fully deterministic across runs even
 *    when the underlying `Set` would otherwise have hash-iteration-order results
 *    (so the propagated cause is stable for observability filters / metrics).
 */
class ExceptionsTest extends AnyFunSuiteLike with Matchers {

  private val tpA = TopicPartition(Topic("topic-a"), 0)
  private val tpB = TopicPartition(Topic("topic-b"), 0)

  test("BatchCloudSinkError.rollBack() is true iff fatal.nonEmpty (fatal-only)") {
    val fatal = FatalCloudSinkError("fatal", tpA)
    BatchCloudSinkError(fatal = Set(fatal), nonFatal = Set.empty).rollBack() shouldBe true
  }

  test("BatchCloudSinkError.rollBack() is false when only nonFatal errors are present") {
    val nonFatal = NonFatalCloudSinkError("non-fatal")
    BatchCloudSinkError(fatal = Set.empty, nonFatal = Set(nonFatal)).rollBack() shouldBe false
  }

  test("BatchCloudSinkError.rollBack() is true when both fatal and nonFatal are present") {
    val fatal    = FatalCloudSinkError("fatal", tpA)
    val nonFatal = NonFatalCloudSinkError("non-fatal")
    BatchCloudSinkError(fatal = Set(fatal), nonFatal = Set(nonFatal)).rollBack() shouldBe true
  }

  test("BatchCloudSinkError.topicPartitions() returns ONLY the fatal TPs") {
    val fatalA   = FatalCloudSinkError("fatal-a", tpA)
    val nonFatal = NonFatalCloudSinkError("non-fatal", None)
    val batch    = BatchCloudSinkError(fatal = Set(fatalA), nonFatal = Set(nonFatal))
    batch.topicPartitions() shouldBe Set(tpA)
  }

  test("BatchCloudSinkError.exception() prefers a fatal cause when fatal.nonEmpty") {
    val fatalCause    = new RuntimeException("fatal-cause")
    val nonFatalCause = new RuntimeException("non-fatal-cause")
    val fatal         = FatalCloudSinkError("fatal-msg", Some(fatalCause), tpA)
    val nonFatal      = NonFatalCloudSinkError("non-fatal-msg", Some(nonFatalCause))
    val batch         = BatchCloudSinkError(fatal = Set(fatal), nonFatal = Set(nonFatal))
    batch.exception() shouldBe Some(fatalCause)
  }

  test("BatchCloudSinkError.exception() falls back to a nonFatal cause when fatal is empty") {
    val nonFatalCause = new RuntimeException("non-fatal-cause")
    val nonFatal      = NonFatalCloudSinkError("non-fatal-msg", Some(nonFatalCause))
    val batch         = BatchCloudSinkError(fatal = Set.empty, nonFatal = Set(nonFatal))
    batch.exception() shouldBe Some(nonFatalCause)
  }

  test("BatchCloudSinkError.exception() is deterministic for multi-fatal batches (sort by (topic, partition))") {
    // Sort keys: ("topic-a", 0) < ("topic-x", 1) < ("topic-x", 5)
    val causeA = new RuntimeException("cause-a")
    val causeB = new RuntimeException("cause-b")
    val causeC = new RuntimeException("cause-c")
    val fatalA = FatalCloudSinkError("a-msg", Some(causeA), TopicPartition(Topic("topic-x"), 5))
    val fatalB = FatalCloudSinkError("b-msg", Some(causeB), TopicPartition(Topic("topic-x"), 1))
    val fatalC = FatalCloudSinkError("c-msg", Some(causeC), TopicPartition(Topic("topic-a"), 0))

    (1 to 100).foreach { _ =>
      val batch = BatchCloudSinkError(fatal = Set(fatalA, fatalB, fatalC), nonFatal = Set.empty)
      batch.exception() shouldBe Some(causeC)
    }
  }

  test("BatchCloudSinkError.exception() honours secondary partition sort key when topics tie") {
    val cause7  = new RuntimeException("p-7")
    val cause2  = new RuntimeException("p-2")
    val cause13 = new RuntimeException("p-13")
    val fatal7  = FatalCloudSinkError("p7", Some(cause7), TopicPartition(Topic("same-topic"), 7))
    val fatal2  = FatalCloudSinkError("p2", Some(cause2), TopicPartition(Topic("same-topic"), 2))
    val fatal13 = FatalCloudSinkError("p13", Some(cause13), TopicPartition(Topic("same-topic"), 13))

    (1 to 100).foreach { _ =>
      val batch = BatchCloudSinkError(fatal = Set(fatal7, fatal2, fatal13), nonFatal = Set.empty)
      batch.exception() shouldBe Some(cause2)
    }
  }

  test("BatchCloudSinkError.exception() is deterministic for multi-nonFatal batches (sort by message)") {
    val causeA = new RuntimeException("nf-a")
    val causeB = new RuntimeException("nf-b")
    val causeC = new RuntimeException("nf-c")
    val nfA    = NonFatalCloudSinkError("aaa", Some(causeA))
    val nfB    = NonFatalCloudSinkError("bbb", Some(causeB))
    val nfC    = NonFatalCloudSinkError("ccc", Some(causeC))

    (1 to 100).foreach { _ =>
      val batch = BatchCloudSinkError(fatal = Set.empty, nonFatal = Set(nfA, nfB, nfC))
      batch.exception() shouldBe Some(causeA)
    }
  }

  test(
    "BatchCloudSinkError.exception() honours secondary cause-toString sort key when nonFatal messages tie",
  ) {
    // All three entries share the same `message` value; the tiebreaker is cause.toString
    // (class + message), so the lexicographically-first cause must always win regardless
    // of how the JVM happens to iterate the Set.
    //
    // toString values (lexicographic order):
    //   "java.lang.IllegalArgumentException: cause-b"   ← SMALLEST → expected winner
    //   "java.lang.IllegalStateException: cause-a"
    //   "java.lang.RuntimeException: cause-c"
    val causeC = new RuntimeException("cause-c")
    val causeA = new IllegalStateException("cause-a")
    val causeB = new IllegalArgumentException("cause-b")
    val nfC    = NonFatalCloudSinkError("shared-message", Some(causeC))
    val nfA    = NonFatalCloudSinkError("shared-message", Some(causeA))
    val nfB    = NonFatalCloudSinkError("shared-message", Some(causeB))

    (1 to 100).foreach { _ =>
      val batch = BatchCloudSinkError(fatal = Set.empty, nonFatal = Set(nfC, nfA, nfB))
      batch.exception() shouldBe Some(causeB)
    }
  }

  test("BatchCloudSinkError.topicPartitions() also fatal-only when both fatal and nonFatal present") {
    // NonFatalCloudSinkError.topicPartitions() returns Set.empty by design,
    // so this test pins the documented contract: rollback is scoped to fatal TPs ONLY.
    val fatalA   = FatalCloudSinkError("fatal-a", tpA)
    val nonFatal = NonFatalCloudSinkError("non-fatal", None)
    val batch    = BatchCloudSinkError(fatal = Set(fatalA), nonFatal = Set(nonFatal))
    batch.topicPartitions() shouldBe Set(tpA)
    batch.topicPartitions() should not contain tpB
  }
}
