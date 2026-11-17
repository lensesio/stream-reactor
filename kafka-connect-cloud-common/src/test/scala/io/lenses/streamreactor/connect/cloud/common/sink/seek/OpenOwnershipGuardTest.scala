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
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.OnWriteAt
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/**
 * The master-lock retry inside `open()`: a lost write is retried only while the re-read lock still
 * carries the ownership epoch this open first observed. A different epoch means another instance
 * took the partition after our read, so the open fails instead of taking the lock back.
 */
class OpenOwnershipGuardTest extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private val tp = Topic("topic").withPartition(0)

  private def seedMaster(h: TwoInstanceHarness, idx: IndexFile): Unit = {
    val _ = h.storage.writeBlobToFile(h.bucket, h.masterLockPath(tp), NoOverwriteExistingObject(idx)).value
  }

  private def storedMaster(h: TwoInstanceHarness): IndexFile =
    h.readLock(h.masterLockPath(tp)).value.wrappedObject

  private def masterWrites(h: TwoInstanceHarness): Int = h.storage.writeCount(h.bucket, h.masterLockPath(tp))

  /**
   * Runs `action` just before the next master-lock write by instance `instance` is applied, so
   * another instance's step lands between that instance's read and its conditional write. The
   * action runs on its own thread (it may itself call `open()`, which runs its own IO).
   */
  private def beforeMasterWriteBy[A](h: TwoInstanceHarness, instance: Int)(action: => A): AtomicReference[A] = {
    val result = new AtomicReference[A]()
    val owner  = h.lockUuid(instance)
    val _ = h.storage.arm(
      OnWriteAt(
        h.bucket,
        h.masterLockPath(tp),
        () => result.set(Await.result(Future(action)(scala.concurrent.ExecutionContext.global), 30.seconds)),
        { case idx: IndexFile => idx.owner == owner; case _ => false },
      ),
    )
    result
  }

  private def ownershipLost(result: Either[SinkError, Any]): Unit = {
    result.left.value shouldBe a[FatalCloudSinkError]
    result.left.value.message() should include("taken over during open")
    ()
  }

  test("an open whose bump loses to a newer owner's open fails and leaves the newer owner's lock alone") {
    val h = new TwoInstanceHarness()
    try {
      seedMaster(h, IndexFile("prev-owner", Some(Offset(41)), None, Some(500L)))
      h.clock.set(1000L)
      val writesAfterB = new AtomicReference[Int]()
      val bOpen = beforeMasterWriteBy(h, instance = 0) {
        val r = h.b.open(Set(tp)) // B: reads A's M0 too, bumps it first (epoch 501 -> 1000)
        writesAfterB.set(masterWrites(h))
        r
      }

      val aOpen = h.a.open(Set(tp))

      bOpen.get().value shouldBe Map(tp -> Some(Offset(41)))
      ownershipLost(aOpen)
      // A's failed bump is its only write (already counted when B's open ran inside it): A neither
      // re-bumps B's lock nor writes anything else.
      masterWrites(h) shouldBe writesAfterB.get()
      storedMaster(h) shouldBe IndexFile(h.lockUuid(1), Some(Offset(41)), None, Some(1000L))
      h.a.heldEpochOf(tp) shouldBe None
      h.a.topicPartitionToETags.get(tp) shouldBe None
      h.metrics(0).getMasterLockOpenOwnershipLost shouldBe 1L
      h.metrics(1).getMasterLockOpenOwnershipLost shouldBe 0L

      // B keeps the partition: its next floor write succeeds.
      h.b.updateMasterLock(tp, Offset(50)).value
      storedMaster(h).owner shouldBe h.lockUuid(1)
    } finally h.close()
  }

  test("a floor write by the previous owner between our read and bump keeps its epoch, so the bump is retried") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.b.open(Set(tp)).value // B: the previous owner, epoch 1000
      // B advances its floor (same epoch) while A is between its read and its bump.
      val floorWrite = beforeMasterWriteBy(h, instance = 0)(h.b.updateMasterLock(tp, Offset(10)))

      h.a.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(9)))

      floorWrite.get().value
      storedMaster(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(9)), None, Some(1001L))
      h.a.heldEpochOf(tp) shouldBe Some(1001L)
      h.metrics(0).getMasterLockOpenOwnershipLost shouldBe 0L
      // The previous owner is fenced by A's bump.
      h.b.updateMasterLock(tp, Offset(11)).isLeft shouldBe true
      h.b.confirmMasterFenced(tp) shouldBe true
      h.a.updateMasterLock(tp, Offset(11)).value
    } finally h.close()
  }

  test("a lost create race against a newer owner fails the loser and leaves the winner's lock alone") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      // Both read the lock as absent; B's create lands first.
      val bOpen = beforeMasterWriteBy(h, instance = 0)(h.b.open(Set(tp)))

      val aOpen = h.a.open(Set(tp))

      bOpen.get().value shouldBe Map(tp -> None)
      ownershipLost(aOpen)
      storedMaster(h) shouldBe IndexFile(h.lockUuid(1), None, None, Some(1000L))
      h.a.heldEpochOf(tp) shouldBe None
      h.metrics(0).getMasterLockOpenOwnershipLost shouldBe 1L
      h.b.updateMasterLock(tp, Offset(5)).value
    } finally h.close()
  }

  test("a pending-state bump that loses to a newer owner fails without re-bumping or driving the chain") {
    val h = new TwoInstanceHarness()
    try {
      val (chain, temp, finalPath) = copyChain(h)
      seedMaster(h, IndexFile("crashed-owner", Some(Offset(41)), Some(chain), Some(500L)))
      h.clock.set(1000L)
      val writesAfterB = new AtomicReference[Int]()
      val bOpen = beforeMasterWriteBy(h, instance = 0) {
        val r = h.b.open(Set(tp)) // B bumps, then drives the chain to completion
        writesAfterB.set(masterWrites(h))
        r
      }

      val aOpen = h.a.open(Set(tp))

      bOpen.get().value shouldBe Map(tp -> Some(Offset(50)))
      ownershipLost(aOpen)
      masterWrites(h) shouldBe writesAfterB.get() // A's failed bump, counted before B's open ran
      storedMaster(h) shouldBe IndexFile(h.lockUuid(1), Some(Offset(50)), None, Some(1000L))
      h.storage.pathExists(h.bucket, finalPath).value shouldBe true
      h.storage.pathExists(h.bucket, temp).value shouldBe false
      h.a.heldEpochOf(tp) shouldBe None
      h.metrics(0).getMasterLockOpenOwnershipLost shouldBe 1L
    } finally h.close()
  }

  /** Stages one temp object and returns a Copy + Delete chain moving it to its final path. */
  private def copyChain(h: TwoInstanceHarness): (PendingState, String, String) = {
    val temp      = "data/topic/0/.temp-upload/staged"
    val finalPath = "data/topic/0/000000000050.avro"
    h.storage.writeStringToFile(h.bucket, temp, UploadableString("record")).value
    val eTag = h.storage.getBlobAsStringAndEtag(h.bucket, temp).value._2
    val chain = PendingState(
      Offset(50),
      NonEmptyList.of(CopyOperation(h.bucket, temp, finalPath, eTag), DeleteOperation(h.bucket, temp, eTag)),
    )
    (chain, temp, finalPath)
  }
}
