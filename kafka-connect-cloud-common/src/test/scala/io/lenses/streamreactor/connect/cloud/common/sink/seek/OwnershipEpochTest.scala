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
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** LC-350 spec ownership-epoch: epoch assignment on open (R2). */
class OwnershipEpochTest extends AnyFunSuite with Matchers with EitherValues {

  private val tp = Topic("topic").withPartition(0)

  private def seedMaster(h: TwoInstanceHarness, idx: IndexFile): Unit = {
    val _ = h.storage.writeBlobToFile(h.bucket, h.masterLockPath(tp), NoOverwriteExistingObject(idx)).value
  }

  private def storedMaster(h: TwoInstanceHarness): IndexFile =
    h.readLock(h.masterLockPath(tp)).value.wrappedObject

  test("opening a brand-new partition creates the master lock with epoch = clock") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value

      storedMaster(h).epoch shouldBe Some(1000L)
      h.a.tpEpoch.get(tp) shouldBe Some(1000L)
    } finally h.close()
  }

  test("opening over a pre-epoch clean lock takes epoch = clock (old version dropped the field)") {
    val h = new TwoInstanceHarness()
    try {
      seedMaster(h, IndexFile("old-owner", Some(Offset(41)), None))
      h.clock.set(1000L)

      h.a.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(41)))

      storedMaster(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(41)), None, Some(1000L))
      h.a.tpEpoch.get(tp) shouldBe Some(1000L)
    } finally h.close()
  }

  test("opening over a clean lock whose epoch is ahead of the clock takes previous + 1") {
    val h = new TwoInstanceHarness()
    try {
      seedMaster(h, IndexFile("prev-owner", Some(Offset(41)), None, Some(5000L)))
      h.clock.set(1000L)

      h.a.open(Set(tp)).value

      storedMaster(h).epoch shouldBe Some(5001L)
      h.a.tpEpoch.get(tp) shouldBe Some(5001L)
    } finally h.close()
  }

  test("opening over a 0-byte poison master lock takes epoch = clock") {
    val h = new TwoInstanceHarness()
    try {
      h.storage.putRaw(h.bucket, h.masterLockPath(tp), Array.emptyByteArray)
      h.clock.set(1000L)

      h.a.open(Set(tp)).value

      storedMaster(h).epoch shouldBe Some(1000L)
      h.a.tpEpoch.get(tp) shouldBe Some(1000L)
    } finally h.close()
  }

  test("opening over a master lock with a PendingState bumps the epoch before recovery") {
    val h = new TwoInstanceHarness()
    try {
      val pending = PendingState(
        Offset(50),
        NonEmptyList.one(DeleteOperation(h.bucket, "data/topic/0/.temp-upload/never-existed", "temp-etag")),
      )
      seedMaster(h, IndexFile("crashed-owner", Some(Offset(41)), Some(pending), Some(7000L)))
      h.clock.set(1000L)

      h.a.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(50)))

      h.a.tpEpoch.get(tp) shouldBe Some(7001L)
    } finally h.close()
  }

  test("successive owners get strictly increasing epochs even when the clock is frozen") {
    val h = new TwoInstanceHarness(instanceCount = 3)
    try {
      h.clock.set(1000L)
      h.instances(0).open(Set(tp)).value
      h.instances(1).open(Set(tp)).value
      h.instances(2).open(Set(tp)).value

      h.instances(0).tpEpoch.get(tp) shouldBe Some(1000L)
      h.instances(1).tpEpoch.get(tp) shouldBe Some(1001L)
      h.instances(2).tpEpoch.get(tp) shouldBe Some(1002L)
      storedMaster(h).epoch shouldBe Some(1002L)
    } finally h.close()
  }

  // ---- R3: every lock write stamps the held epoch -------------------------------------------

  private val pk = "date=2024-01-01"

  private def storedGranular(h: TwoInstanceHarness): IndexFile =
    h.readLock(h.granularLockPath(tp, pk)).value.wrappedObject

  test("a new granular lock is created with the held epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value

      storedGranular(h).epoch shouldBe h.a.tpEpoch.get(tp)
    } finally h.close()
  }

  test("a 0-byte poison granular lock is overwritten with the held epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.storage.putRaw(h.bucket, h.granularLockPath(tp, pk), Array.emptyByteArray)

      h.a.ensureGranularLock(tp, pk).value

      storedGranular(h).epoch shouldBe h.a.tpEpoch.get(tp)
    } finally h.close()
  }

  test("a granular commit (updateForPartitionKey) keeps the held epoch on the lock") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value

      storedGranular(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(5)), None, h.a.tpEpoch.get(tp))
    } finally h.close()
  }

  test("a master floor write (updateMasterLock) keeps the held epoch on the master lock") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(10)).value

      storedMaster(h).epoch shouldBe h.a.tpEpoch.get(tp)
      storedMaster(h).committedOffset shouldBe Some(Offset(9))
    } finally h.close()
  }

  test("a master commit (update, used by batch and non-PARTITIONBY commits) keeps the held epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.update(tp, Some(Offset(12)), None).value

      storedMaster(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(12)), None, h.a.tpEpoch.get(tp))
    } finally h.close()
  }

  test("master PendingState recovery on open leaves the master lock stamped with the new epoch") {
    val h = new TwoInstanceHarness()
    try {
      val pending = PendingState(
        Offset(50),
        NonEmptyList.one(DeleteOperation(h.bucket, "data/topic/0/.temp-upload/never-existed", "temp-etag")),
      )
      seedMaster(h, IndexFile("crashed-owner", Some(Offset(41)), Some(pending), Some(7000L)))
      h.clock.set(1000L)

      h.a.open(Set(tp)).value

      storedMaster(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(50)), None, Some(7001L))
    } finally h.close()
  }

  test("granular PendingState recovery on load leaves the granular lock stamped with the held epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      val pending = PendingState(
        Offset(8),
        NonEmptyList.one(DeleteOperation(h.bucket, "data/topic/0/.temp-upload/never-existed", "temp-etag")),
      )
      val _ = h.storage
        .writeBlobToFile(h.bucket,
                         h.granularLockPath(tp, pk),
                         NoOverwriteExistingObject(IndexFile("crashed", Some(Offset(3)), Some(pending))),
        )
        .value

      h.a.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(8))

      storedGranular(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(8)), None, h.a.tpEpoch.get(tp))
    } finally h.close()
  }

  test("a lock write without a held epoch fails fatally and writes nothing") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      val masterBefore   = h.readLock(h.masterLockPath(tp)).value
      val granularBefore = h.readLock(h.granularLockPath(tp, pk)).value
      val _              = h.a.tpEpoch.remove(tp)

      h.a.updateMasterLock(tp, Offset(10)).left.value shouldBe a[
        io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError,
      ]
      h.a.update(tp, Some(Offset(12)), None).left.value shouldBe a[
        io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError,
      ]
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).left.value shouldBe a[
        io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError,
      ]

      h.readLock(h.masterLockPath(tp)).value shouldBe masterBefore
      h.readLock(h.granularLockPath(tp, pk)).value shouldBe granularBefore
    } finally h.close()
  }

  // ---- R4/R5: a lock with a higher epoch than ours means we are fenced -----------------------

  test("LC-350 T1: a zombie that idle-evicts and reloads a key B has committed is fenced, not handed B's token") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value // A: epoch 1000
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value

      h.b.open(Set(tp)).value // B: epoch 1001, the legitimate owner
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
      h.b.updateForPartitionKey(tp, pk, Some(Offset(9)), None).value // K now carries epoch 1001
      val bLock = h.readLock(h.granularLockPath(tp, pk)).value

      // Zombie A: its idle writer for K is evicted, then a new record for K arrives.
      h.a.evictGranularLock(tp, pk)
      h.a.getSeekedOffsetForPartitionKey(tp, pk).left.value shouldBe a[
        io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError,
      ]
      h.a.fencedPartitions.contains(tp) shouldBe true

      // A cached nothing, so it holds no token to write with, and K is untouched.
      h.a.updateForPartitionKey(tp, pk, Some(Offset(7)), None).isLeft shouldBe true
      h.readLock(h.granularLockPath(tp, pk)).value shouldBe bLock

      // B is unaffected.
      h.b.updateForPartitionKey(tp, pk, Some(Offset(12)), None).value shouldBe Some(Offset(12))
      h.b.fencedPartitions.contains(tp) shouldBe false
    } finally h.close()
  }

  test("a zombie's ensureGranularLock over a key B has committed is fenced") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.b.open(Set(tp)).value
      h.b.ensureGranularLock(tp, pk).value // created with B's epoch

      h.a.ensureGranularLock(tp, pk).left.value shouldBe a[
        io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError,
      ]
      h.a.fencedPartitions.contains(tp) shouldBe true
    } finally h.close()
  }

  test("a zombie never drives a pending chain on a lock written under a newer epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value // epoch 1000
      h.b.open(Set(tp)).value // epoch 1001
      val pending = PendingState(
        Offset(8),
        NonEmptyList.one(DeleteOperation(h.bucket, "data/topic/0/.temp-upload/in-flight", "temp-etag")),
      )
      val _ = h.storage
        .writeBlobToFile(
          h.bucket,
          h.granularLockPath(tp, pk),
          NoOverwriteExistingObject(IndexFile(h.lockUuid(1), Some(Offset(3)), Some(pending), Some(1001L))),
        )
        .value
      val before = h.readLock(h.granularLockPath(tp, pk)).value

      h.a.getSeekedOffsetForPartitionKey(tp, pk).isLeft shouldBe true

      h.readLock(h.granularLockPath(tp, pk)).value shouldBe before
    } finally h.close()
  }

  test("locks at or below our epoch load as before (no fence)") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value
      h.b.open(Set(tp)).value // newer owner loads the older-epoch lock

      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
      h.b.fencedPartitions.contains(tp) shouldBe false

      h.a.evictGranularLock(tp, pk) // ... and so does a reload of our own lock
      h.b.evictGranularLock(tp, pk)
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
    } finally h.close()
  }

  test("clock skew after an old-version reset: the owner is fenced, a restart past the lock's epoch succeeds") {
    val h = new TwoInstanceHarness(instanceCount = 3)
    try {
      // An earlier new-version owner with a fast clock stamped K at 5000; an old version then
      // rewrote the master without the field.
      seedMaster(h, IndexFile("old-version-owner", Some(Offset(2)), None))
      val _ = h.storage
        .writeBlobToFile(
          h.bucket,
          h.granularLockPath(tp, pk),
          NoOverwriteExistingObject(IndexFile("fast-clock-owner", Some(Offset(4)), None, Some(5000L))),
        )
        .value

      h.clock.set(1000L) // this worker's clock is behind
      h.instances(1).open(Set(tp)).value
      h.instances(1).getSeekedOffsetForPartitionKey(tp, pk).isLeft shouldBe true

      h.clock.set(6000L) // restart (new instance) once the clock has passed the lock's epoch
      h.instances(2).open(Set(tp)).value
      h.instances(2).getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(4))
    } finally h.close()
  }

  // ---- R6: a failed master write is checked for fencing -------------------------------------

  test("LC-350 T2: after a slow-first-read zombie takes the master, the owner's failed write confirms it is fenced") {
    val h = new TwoInstanceHarness(instanceCount = 3)
    try {
      h.clock.set(1000L)
      h.b.open(Set(tp)).value // B: the legitimate owner, epoch 1000
      h.a.open(Set(tp)).value // A: a slow first read lands after B's bump, epoch 1001

      h.b.updateMasterLock(tp, Offset(10)).isLeft shouldBe true
      h.b.confirmMasterFenced(tp) shouldBe true
      h.b.fencedPartitions should contain(tp)

      // B's task fails and restarts as a new instance, which re-opens above A and fences A.
      val bRestarted = h.instances(2)
      bRestarted.open(Set(tp)).value
      bRestarted.tpEpoch.get(tp) shouldBe Some(1002L)
      h.a.updateMasterLock(tp, Offset(11)).isLeft shouldBe true
      h.a.confirmMasterFenced(tp) shouldBe true
      bRestarted.updateMasterLock(tp, Offset(11)).value
    } finally h.close()
  }

  test("LC-350 T3: a transient master-write failure is not mistaken for fencing") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.storage.arm(
        io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailWriteAt(
          h.bucket,
          h.masterLockPath(tp),
        ),
      )

      h.a.updateMasterLock(tp, Offset(10)).isLeft shouldBe true
      h.a.confirmMasterFenced(tp) shouldBe false
      h.a.fencedPartitions shouldBe empty

      h.a.updateMasterLock(tp, Offset(10)).value
    } finally h.close()
  }

  test("an unreadable master during the fencing check is treated as transient, not fenced") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      val _ = h.storage.deleteFile(h.bucket, h.masterLockPath(tp), "")

      h.a.confirmMasterFenced(tp) shouldBe false
      h.a.fencedPartitions shouldBe empty
    } finally h.close()
  }

  test("clearing the partition state (revoke / failed open) drops the held epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.tpEpoch.get(tp) shouldBe defined

      h.a.clearTopicPartitionState(tp)

      h.a.tpEpoch.get(tp) shouldBe None
    } finally h.close()
  }
}
