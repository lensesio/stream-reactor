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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.data.NonEmptyList
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailMoveAt
import org.scalatest.Assertion
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * A new owner that loads a granular lock written under an older ownership epoch rewrites it with
 * its own epoch before caching it or driving its pending chain, so every token an older instance
 * still holds for the key stops being valid, and a reload by that instance is fenced.
 */
class GranularFenceTest extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private val tp = Topic("topic").withPartition(0)
  private val pk = "date=2024-01-01"

  private def lockPath(h: TwoInstanceHarness): String = h.granularLockPath(tp, pk)

  private def stored(h: TwoInstanceHarness): ObjectWithETag[IndexFile] = h.readLock(lockPath(h)).value

  private def refusedWith(result: Either[SinkError, Any], text: String): Assertion =
    result.left.value.message() should include(text)

  /** A writes `pk` once under its own epoch, so it holds a valid token for the key. */
  private def zombieHoldsToken(h: TwoInstanceHarness, offset: Long): Unit = {
    h.a.open(Set(tp)).value
    h.a.ensureGranularLock(tp, pk).value
    val _ = h.a.updateForPartitionKey(tp, pk, Some(Offset(offset)), None).value
  }

  private def stageTemp(h: TwoInstanceHarness, name: String): (String, String) = {
    val temp = s"data/topic/0/.temp-upload/$name"
    h.storage.writeStringToFile(h.bucket, temp, UploadableString(name)).value
    temp -> h.storage.getBlobAsStringAndEtag(h.bucket, temp).value._2
  }

  // ---- clean locks --------------------------------------------------------------------

  test("a zombie holding a token for a key the new owner has loaded cannot commit it") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      zombieHoldsToken(h, offset = 5) // A: epoch 1000
      h.b.open(Set(tp)).value // B: epoch 1001

      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
      stored(h).wrappedObject shouldBe IndexFile(h.lockUuid(1), Some(Offset(5)), None, Some(1001L))

      h.a.updateForPartitionKey(tp, pk, Some(Offset(7)), None).isLeft shouldBe true
      stored(h).wrappedObject shouldBe IndexFile(h.lockUuid(1), Some(Offset(5)), None, Some(1001L))

      h.b.updateForPartitionKey(tp, pk, Some(Offset(9)), None).value shouldBe Some(Offset(9))
      stored(h).wrappedObject shouldBe IndexFile(h.lockUuid(1), Some(Offset(9)), None, Some(1001L))
    } finally h.close()
  }

  test("a key the new owner loads through ensureGranularLock is fenced against the zombie's token") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      zombieHoldsToken(h, offset = 5)
      h.b.open(Set(tp)).value

      h.b.ensureGranularLock(tp, pk).value
      stored(h).wrappedObject shouldBe IndexFile(h.lockUuid(1), Some(Offset(5)), None, Some(1001L))
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))

      h.a.updateForPartitionKey(tp, pk, Some(Offset(7)), None).isLeft shouldBe true
      h.b.updateForPartitionKey(tp, pk, Some(Offset(9)), None).value shouldBe Some(Offset(9))
    } finally h.close()
  }

  test("reloading a key this instance wrote under its own epoch makes no write") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.b.open(Set(tp)).value
      h.b.ensureGranularLock(tp, pk).value
      h.b.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value
      val before = stored(h)

      h.b.evictGranularLock(tp, pk)
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
      h.b.evictGranularLock(tp, pk)
      h.b.ensureGranularLock(tp, pk).value

      stored(h) shouldBe before
      h.b.updateForPartitionKey(tp, pk, Some(Offset(6)), None).value shouldBe Some(Offset(6))
    } finally h.close()
  }

  test("a zombie commit before the new owner loads the key is adopted, and the zombie is fenced after the load") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      zombieHoldsToken(h, offset = 5)
      h.b.open(Set(tp)).value

      // B has not loaded the key yet: A's commit still lands (durable, CAS-protected).
      h.a.updateForPartitionKey(tp, pk, Some(Offset(7)), None).value shouldBe Some(Offset(7))

      // B adopts A's committed offset, so the replayed records up to 7 are skipped.
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(7))
      stored(h).wrappedObject shouldBe IndexFile(h.lockUuid(1), Some(Offset(7)), None, Some(1001L))

      h.a.updateForPartitionKey(tp, pk, Some(Offset(9)), None).isLeft shouldBe true
      stored(h).wrappedObject.committedOffset shouldBe Some(Offset(7))
    } finally h.close()
  }

  test("after a new owner loads a key an intermediate owner never touched, that owner's reload is fenced") {
    val h = new TwoInstanceHarness(instanceCount = 3)
    try {
      val x0 = h.instances(0)
      val x1 = h.instances(1)
      val b  = h.instances(2)
      h.clock.set(1000L)
      x0.open(Set(tp)).value // 1000
      x0.ensureGranularLock(tp, pk).value
      x0.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value
      x1.open(Set(tp)).value // 1001, never loads the key
      b.open(Set(tp)).value // 1002

      b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
      val bLock = stored(h)
      bLock.wrappedObject.epoch shouldBe Some(1002L)

      refusedWith(x1.getSeekedOffsetForPartitionKey(tp, pk), "epoch 1002 > held epoch 1001")
      x1.fencedPartitions should contain(tp)
      stored(h) shouldBe bLock
    } finally h.close()
  }

  // ---- pending chains -----------------------------------------------------------------

  test("a new owner bumps an older-epoch pending lock before its chain, so the zombie's token is dead even if the chain fails") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      zombieHoldsToken(h, offset = 3)
      val (temp, tempETag) = stageTemp(h, "in-flight")
      val pending =
        PendingState(Offset(8), NonEmptyList.one(CopyOperation(h.bucket, temp, "data/topic/0/final", tempETag)))
      // A's in-flight commit: the pending checkpoint, under A's epoch and with A holding its token.
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), Some(pending)).value
      h.b.open(Set(tp)).value

      // B's chain fails on its first (and only) data step, so the chain itself writes no lock.
      val _ = h.storage.arm(FailMoveAt(h.bucket, temp))
      h.b.getSeekedOffsetForPartitionKey(tp, pk).isLeft shouldBe true

      stored(h).wrappedObject shouldBe IndexFile(h.lockUuid(1), Some(Offset(3)), Some(pending), Some(1001L))
      // A's next chain step, with the token it held before B's load, is refused.
      h.a.updateForPartitionKey(tp, pk, Some(Offset(8)), None).isLeft shouldBe true
      stored(h).wrappedObject.epoch shouldBe Some(1001L)
      h.storage.pathExists(h.bucket, "data/topic/0/final").value shouldBe false

      // B's retry resolves the chain from its own bumped lock.
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(8))
      stored(h).wrappedObject shouldBe IndexFile(h.lockUuid(1), Some(Offset(8)), None, Some(1001L))
      h.storage.pathExists(h.bucket, "data/topic/0/final").value shouldBe true
    } finally h.close()
  }
}
