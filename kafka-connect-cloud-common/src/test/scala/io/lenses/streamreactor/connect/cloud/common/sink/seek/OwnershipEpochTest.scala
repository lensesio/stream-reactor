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
import org.scalatest.Assertion
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Ownership epoch: assigned on open, stamped on every lock write, and enforced on every load. */
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

  // ---- every lock write stamps the held epoch -------------------------------------------

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

  test("with no held epoch, loading a key refuses it: no pending chain is driven and nothing is cached") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      val temp = "data/topic/0/.temp-upload/in-flight"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      val tempETag = h.storage.getBlobAsStringAndEtag(h.bucket, temp).value._2
      val pending  = PendingState(Offset(4), NonEmptyList.one(DeleteOperation(h.bucket, temp, tempETag)))
      val _ = h.storage
        .writeBlobToFile(h.bucket,
                         h.granularLockPath(tp, pk),
                         NoOverwriteExistingObject(IndexFile("old-owner", Some(Offset(2)), Some(pending))),
        )
        .value
      val before = h.readLock(h.granularLockPath(tp, pk)).value
      // The partition's state is gone (closed, or a failed open rolled back): it is not ours.
      h.a.clearTopicPartitionState(tp)

      h.a.getSeekedOffsetForPartitionKey(tp, pk).left.value shouldBe a[
        io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError,
      ]
      // An instance that never opened the partition refuses it the same way.
      h.b.getSeekedOffsetForPartitionKey(tp, pk).isLeft shouldBe true

      h.storage.pathExists(h.bucket, temp).value shouldBe true
      h.a.granularCacheSize shouldBe 0
      h.b.granularCacheSize shouldBe 0
      h.readLock(h.granularLockPath(tp, pk)).value shouldBe before
    } finally h.close()
  }

  // ---- a lock with a higher epoch than ours means we are fenced -----------------------

  test("a zombie that idle-evicts and reloads a key B has committed is fenced, not handed B's token") {
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
      // Nothing was cached by the rejected load: a second lookup is rejected again, not served.
      h.a.granularCacheSize shouldBe 0
      h.a.getSeekedOffsetForPartitionKey(tp, pk).isLeft shouldBe true

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

  // ---- a failed master write is checked for fencing -------------------------------------

  test("after a sequential takeover by another instance, the owner's failed master write confirms it is fenced") {
    val h = new TwoInstanceHarness(instanceCount = 3)
    try {
      h.clock.set(1000L)
      h.b.open(Set(tp)).value // B: the legitimate owner, epoch 1000
      h.a.open(Set(tp)).value // A: opens after B (a sequential takeover), epoch 1001

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

  test("a transient master-write failure is not mistaken for fencing") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.storage.arm(
        io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailWriteAt(
          h.bucket,
          h.masterLockPath(tp),
        ),
      )

      val cachedBefore = h.a.topicPartitionToETags.get(tp)

      h.a.updateMasterLock(tp, Offset(10)).isLeft shouldBe true
      h.a.confirmMasterFenced(tp) shouldBe false
      h.a.fencedPartitions shouldBe empty
      h.a.topicPartitionToETags.get(tp) shouldBe cachedBefore
      Some(h.readLock(h.masterLockPath(tp)).value.eTag) shouldBe cachedBefore

      h.a.updateMasterLock(tp, Offset(10)).value
    } finally h.close()
  }

  test("a master write that landed but lost its response is adopted on the fencing check, and later writes succeed") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.storage.arm(
        io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.LoseWriteResponseAt(
          h.bucket,
          h.masterLockPath(tp),
        ),
      )

      h.a.updateMasterLock(tp, Offset(10)).isLeft shouldBe true
      storedMaster(h).committedOffset shouldBe Some(Offset(9)) // the write did land

      h.a.confirmMasterFenced(tp) shouldBe false
      h.a.fencedPartitions shouldBe empty
      h.a.topicPartitionToETags.get(tp) shouldBe Some(h.readLock(h.masterLockPath(tp)).value.eTag)
      h.a.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(9))

      // Three more floor-write cycles, each re-checked on failure as preCommit does.
      val written = (1 to 3).map { i =>
        val result = h.a.updateMasterLock(tp, Offset(10L + i * 10L))
        if (result.isLeft) h.a.confirmMasterFenced(tp) shouldBe false
        result.isRight
      }
      written shouldBe Seq(true, true, true)
      storedMaster(h).committedOffset shouldBe Some(Offset(39))
      h.a.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(39))
    } finally h.close()
  }

  test("a newer owner that opens after our lost-response master write still fences us") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.storage.arm(
        io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.LoseWriteResponseAt(
          h.bucket,
          h.masterLockPath(tp),
        ),
      )
      h.a.updateMasterLock(tp, Offset(10)).isLeft shouldBe true
      h.b.open(Set(tp)).value

      h.a.confirmMasterFenced(tp) shouldBe true
      h.a.fencedPartitions should contain(tp)
      h.a.updateMasterLock(tp, Offset(20)).isLeft shouldBe true
      storedMaster(h).owner shouldBe h.lockUuid(1)
    } finally h.close()
  }

  test("a master lock with our owner but a lower epoch is not adopted and does not fence") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      val cachedBefore = h.a.topicPartitionToETags.get(tp)
      val current      = h.readLock(h.masterLockPath(tp)).value
      val _ = h.storage.writeBlobToFile(
        h.bucket,
        h.masterLockPath(tp),
        ObjectWithETag(IndexFile(h.lockUuid(0), Some(Offset(99)), None, Some(999L)), current.eTag),
      ).value

      h.a.confirmMasterFenced(tp) shouldBe false
      h.a.fencedPartitions shouldBe empty
      h.a.topicPartitionToETags.get(tp) shouldBe cachedBefore
      h.a.getSeekedOffsetForTopicPartition(tp) should not be Some(Offset(99))
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

  test("clearing the partition state of a revoked fenced partition drops the fence") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.b.open(Set(tp)).value
      h.b.ensureGranularLock(tp, pk).value
      h.a.ensureGranularLock(tp, pk).isLeft shouldBe true
      h.a.fencedPartitions should contain(tp)

      h.a.clearTopicPartitionState(tp)

      h.a.fencedPartitions shouldBe empty
      h.a.fencingReason(tp) shouldBe None
    } finally h.close()
  }

  // ---- close(partitions) is the only revoke; open() only takes ownership ------------------

  private val other = Topic("topic").withPartition(1)

  test("opening newly assigned partitions leaves a retained partition's ownership and cache untouched") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      // Nothing committed to the master yet, so no seeked offset is held for the retained partition.
      h.a.getSeekedOffsetForTopicPartition(tp) shouldBe None
      val epoch = h.a.tpEpoch.get(tp)
      val eTag  = h.a.topicPartitionToETags.get(tp)

      // A cooperative rebalance adds `other` and keeps `tp`: open() receives only the added partition.
      h.a.open(Set(other)).value

      h.a.tpEpoch.get(tp) shouldBe epoch
      h.a.topicPartitionToETags.get(tp) shouldBe eTag
      h.a.granularCacheSize shouldBe 1
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value shouldBe Some(Offset(5))
      h.a.updateMasterLock(tp, Offset(6)).value
    } finally h.close()
  }

  test("closing a revoked fenced partition clears its fence and state and leaves the other partitions working") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp, other)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value
      h.b.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(6)).isLeft shouldBe true
      h.a.confirmMasterFenced(tp) shouldBe true
      // The master floor was never written, so no seeked offset is held for the partition.
      h.a.getSeekedOffsetForTopicPartition(tp) shouldBe None

      h.a.closePartitions(Set(tp))

      h.a.fencedPartitions shouldBe empty
      h.a.fencingReason(tp) shouldBe None
      h.a.tpEpoch.get(tp) shouldBe None
      h.a.topicPartitionToETags.get(tp) shouldBe None
      h.a.granularCacheSize shouldBe 0
      h.a.tpEpoch.get(other) shouldBe defined
      h.a.ensureGranularLock(other, pk).value
      h.a.updateForPartitionKey(other, pk, Some(Offset(1)), None).value shouldBe Some(Offset(1))
      h.a.updateMasterLock(other, Offset(2)).value
    } finally h.close()
  }

  test("an eager rebalance (close all, then open all) re-takes ownership of every partition with a new epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp, other)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(4)).value
      val before = h.a.tpEpoch(tp)

      h.a.closePartitions(Set(tp, other))
      h.a.tpEpoch shouldBe empty
      h.a.topicPartitionToETags shouldBe empty
      h.a.getSeekedOffsetForTopicPartition(tp) shouldBe None

      h.a.open(Set(tp, other)).value shouldBe Map(tp -> Some(Offset(3)), other -> None)
      h.a.tpEpoch(tp) should be > before
      storedMaster(h).epoch shouldBe h.a.tpEpoch.get(tp)
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(7)), None).value shouldBe Some(Offset(7))
      h.a.updateMasterLock(tp, Offset(8)).value
    } finally h.close()
  }

  test("GC deletes queued before a close are drained by the close while the partition is still ours") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value

      h.a.closePartitions(Set(tp))

      h.readLock(h.granularLockPath(tp, pk)).isLeft shouldBe true
      h.a.gcQueueSize shouldBe 0
    } finally h.close()
  }

  test("GC deletes queued before a handover this instance never noticed are dropped by its close, not drained") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value // queues the key's lock

      // The partitions were lost: B took the partition over before this instance's close ran.
      h.b.open(Set(tp)).value
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value
      h.b.updateForPartitionKey(tp, pk, Some(Offset(12)), None).value
      val bLock = h.readLock(h.granularLockPath(tp, pk)).value
      h.a.fencedPartitions shouldBe empty

      h.a.closePartitions(Set(tp))

      h.readLock(h.granularLockPath(tp, pk)).value shouldBe bLock
      h.a.gcQueueSize shouldBe 0
    } finally h.close()
  }

  test("a fenced instance whose partition is re-assigned to it re-takes ownership, drops the fence and writes again") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value // epoch 1000
      h.b.open(Set(tp)).value // epoch 1001
      h.b.ensureGranularLock(tp, pk).value
      h.a.ensureGranularLock(tp, pk).isLeft shouldBe true
      h.a.fencedPartitions should contain(tp)

      // A rebalance re-assigns the partition to this instance (close + open, same instance).
      h.a.evictAllGranularLocks(tp)
      h.a.open(Set(tp)).value
      h.a.tpEpoch.get(tp) shouldBe Some(1002L)

      h.a.fencedPartitions shouldBe empty
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(4)), None).value shouldBe Some(Offset(4))
      h.a.updateMasterLock(tp, Offset(4)).value
      // The previous owner is now the fenced one.
      h.b.updateMasterLock(tp, Offset(5)).isLeft shouldBe true
      h.b.confirmMasterFenced(tp) shouldBe true
    } finally h.close()
  }

  test("a fenced instance's GC drain does not delete a lock the newer owner now holds") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      // Below the floor and no active writer: queued for deletion.
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value

      h.b.open(Set(tp)).value
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value
      h.b.updateForPartitionKey(tp, pk, Some(Offset(12)), None).value
      val bLock = h.readLock(h.granularLockPath(tp, pk)).value
      h.a.updateMasterLock(tp, Offset(11)).isLeft shouldBe true
      h.a.confirmMasterFenced(tp) shouldBe true

      h.a.drainGcQueue()

      h.readLock(h.granularLockPath(tp, pk)).value shouldBe bLock
    } finally h.close()
  }

  // ---- once fenced, nothing more is written or cached --------------------------------------

  test("once fenced on one key, a commit to another cached key the new owner has not touched is refused") {
    val h = new TwoInstanceHarness()
    try {
      val other = "date=2024-01-02"
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.ensureGranularLock(tp, other).value
      h.a.updateForPartitionKey(tp, other, Some(Offset(3)), None).value

      h.b.open(Set(tp)).value
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value
      h.b.updateForPartitionKey(tp, pk, Some(Offset(9)), None).value

      h.a.evictGranularLock(tp, pk)
      h.a.getSeekedOffsetForPartitionKey(tp, pk).isLeft shouldBe true
      h.a.fencedPartitions should contain(tp)
      val otherBefore = h.readLock(h.granularLockPath(tp, other)).value

      h.a.updateForPartitionKey(tp, other, Some(Offset(4)), None).left.value shouldBe a[
        io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError,
      ]
      h.a.updateMasterLock(tp, Offset(4)).isLeft shouldBe true
      h.readLock(h.granularLockPath(tp, other)).value shouldBe otherBefore
    } finally h.close()
  }

  /**
   * Fences A on `pk` (B has written it), then seeds `other` with a lock carrying A's own epoch and
   * a pending Delete of a real temp object, so only the partition-wide fence can refuse it.
   */
  private def fencedWithOwnEpochPendingKey(h: TwoInstanceHarness, other: String, temp: String): Assertion = {
    h.clock.set(1000L)
    h.a.open(Set(tp)).value
    h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
    val tempETag = h.storage.getBlobAsStringAndEtag(h.bucket, temp).value._2
    val pending  = PendingState(Offset(8), NonEmptyList.one(DeleteOperation(h.bucket, temp, tempETag)))
    val _ = h.storage
      .writeBlobToFile(
        h.bucket,
        h.granularLockPath(tp, other),
        NoOverwriteExistingObject(IndexFile(h.lockUuid(0), Some(Offset(3)), Some(pending), h.a.tpEpoch.get(tp))),
      )
      .value
    h.b.open(Set(tp)).value
    h.b.ensureGranularLock(tp, pk).value
    h.a.ensureGranularLock(tp, pk).isLeft shouldBe true
    h.a.fencedPartitions should contain(tp)
  }

  test("once fenced, loading another key refuses its pending chain and leaves its temp object in place") {
    val h = new TwoInstanceHarness()
    try {
      val other = "date=2024-01-02"
      val temp  = "data/topic/0/.temp-upload/own-epoch-pending"
      fencedWithOwnEpochPendingKey(h, other, temp)
      val before = h.readLock(h.granularLockPath(tp, other)).value

      h.a.getSeekedOffsetForPartitionKey(tp, other).left.value shouldBe a[
        io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError,
      ]

      h.storage.pathExists(h.bucket, temp).value shouldBe true
      h.readLock(h.granularLockPath(tp, other)).value shouldBe before
      h.a.granularCacheSize shouldBe 0
    } finally h.close()
  }

  test("once fenced, ensureGranularLock on another key refuses its pending chain and leaves its temp object") {
    val h = new TwoInstanceHarness()
    try {
      val other = "date=2024-01-02"
      val temp  = "data/topic/0/.temp-upload/own-epoch-pending"
      fencedWithOwnEpochPendingKey(h, other, temp)
      val before = h.readLock(h.granularLockPath(tp, other)).value

      h.a.ensureGranularLock(tp, other).left.value shouldBe a[
        io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError,
      ]

      h.storage.pathExists(h.bucket, temp).value shouldBe true
      h.readLock(h.granularLockPath(tp, other)).value shouldBe before
      h.a.granularCacheSize shouldBe 0
    } finally h.close()
  }

  test("once fenced, a key cached before the fence is no longer served") {
    val h = new TwoInstanceHarness()
    try {
      val other = "date=2024-01-02"
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, other).value
      h.a.updateForPartitionKey(tp, other, Some(Offset(3)), None).value
      h.b.open(Set(tp)).value
      h.b.ensureGranularLock(tp, pk).value
      h.a.ensureGranularLock(tp, pk).isLeft shouldBe true
      h.a.fencedPartitions should contain(tp)

      h.a.getSeekedOffsetForPartitionKey(tp, other).isLeft shouldBe true
      h.a.ensureGranularLock(tp, other).isLeft shouldBe true
    } finally h.close()
  }

  test("a fenced partition reports the epochs involved") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value // epoch 1000
      h.b.open(Set(tp)).value // epoch 1001
      h.b.ensureGranularLock(tp, pk).value

      h.a.ensureGranularLock(tp, pk).isLeft shouldBe true

      h.a.fencingReason(tp).getOrElse("") should (include("1001") and include("1000"))
      h.b.fencingReason(tp) shouldBe None
    } finally h.close()
  }

  test("a batch open whose legacy-lock snapshot reads a newer-epoch lock is fenced and neither bumps nor purges it") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch)
    try {
      h.clock.set(1000L)
      // A granular lock written by a newer owner after this instance's ownership write.
      val _ = h.storage
        .writeBlobToFile(
          h.bucket,
          h.granularLockPath(tp, pk),
          NoOverwriteExistingObject(IndexFile(h.lockUuid(1), Some(Offset(9)), None, Some(5000L))),
        )
        .value
      val before = h.readLock(h.granularLockPath(tp, pk)).value

      h.a.open(Set(tp)).value

      h.a.fencedPartitions should contain(tp)
      h.readLock(h.granularLockPath(tp, pk)).value shouldBe before
    } finally h.close()
  }

  test("GC deletes queued before a fence do not survive a re-open that re-takes the partition") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value // queues the key's lock

      h.b.open(Set(tp)).value
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value
      h.b.updateForPartitionKey(tp, pk, Some(Offset(12)), None).value
      h.a.updateMasterLock(tp, Offset(11)).isLeft shouldBe true
      h.a.confirmMasterFenced(tp) shouldBe true

      // A rebalance re-assigns the partition to the fenced instance before its GC drains.
      h.a.evictAllGranularLocks(tp)
      h.a.open(Set(tp)).value
      val lockAfterReopen = h.readLock(h.granularLockPath(tp, pk)).value
      lockAfterReopen.wrappedObject.committedOffset shouldBe Some(Offset(12))

      h.a.drainGcQueue()

      h.readLock(h.granularLockPath(tp, pk)).value shouldBe lockAfterReopen
    } finally h.close()
  }

  test("GC deletes queued before an unnoticed handover do not survive the re-open that takes the partition back") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value // queues the key's lock

      // The partition moves to B and back while this instance reads and writes nothing.
      h.b.open(Set(tp)).value
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value
      h.b.updateForPartitionKey(tp, pk, Some(Offset(12)), None).value
      h.a.fencedPartitions shouldBe empty

      h.a.evictAllGranularLocks(tp)
      h.a.open(Set(tp)).value
      val lockAfterReopen = h.readLock(h.granularLockPath(tp, pk)).value

      h.a.drainGcQueue()

      h.readLock(h.granularLockPath(tp, pk)).value shouldBe lockAfterReopen
    } finally h.close()
  }

  test("GC deletes queued before a re-open by the same owner without a close are still drained") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value

      h.a.evictAllGranularLocks(tp)
      h.a.open(Set(tp)).value
      h.a.drainGcQueue()

      h.readLock(h.granularLockPath(tp, pk)).isLeft shouldBe true
    } finally h.close()
  }

  test("once a legacy snapshot read fences the partition, no later legacy lock's pending chain is driven") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch)
    try {
      h.clock.set(1000L)
      val later = "date=2024-01-02" // listed after `pk`
      val temp  = "data/topic/0/.temp-upload/in-flight"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      val tempETag = h.storage.getBlobAsStringAndEtag(h.bucket, temp).value._2
      val seed = (key: String, idx: IndexFile) =>
        h.storage.writeBlobToFile(h.bucket, h.granularLockPath(tp, key), NoOverwriteExistingObject(idx)).value
      val _ = seed(pk, IndexFile(h.lockUuid(1), Some(Offset(9)), None, Some(5000L)))
      val _ = seed(
        later,
        IndexFile("old-owner",
                  Some(Offset(2)),
                  Some(PendingState(Offset(4), NonEmptyList.one(DeleteOperation(h.bucket, temp, tempETag)))),
        ),
      )
      val laterBefore = h.readLock(h.granularLockPath(tp, later)).value
      val laterReads  = h.storage.readCount(h.bucket, h.granularLockPath(tp, later))

      h.a.open(Set(tp)).value

      h.a.fencedPartitions should contain(tp)
      // The snapshot stops at the fencing lock: the later key's lock is not even read.
      h.storage.readCount(h.bucket, h.granularLockPath(tp, later)) shouldBe laterReads
      h.storage.pathExists(h.bucket, temp).value shouldBe true
      h.readLock(h.granularLockPath(tp, later)).value shouldBe laterBefore
    } finally h.close()
  }

  // ---- a fenced partition is never swept -------------------------------------------------

  private val longAgo = java.time.Instant.now().minusSeconds(7200L)

  test("the granular orphan sweep skips a fenced partition: nothing is queued and no sweep marker is written") {
    val h = new TwoInstanceHarness(sweepEnabled = true, sweepSeconds = 3600)
    try {
      val orphan = "date=2024-01-02"
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, orphan).value
      h.a.updateForPartitionKey(tp, orphan, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      // The key's writer went idle: below the floor, uncached and aged, so it reads as an orphan.
      h.a.evictGranularLock(tp, orphan)
      h.storage.setLastModified(h.bucket, h.granularLockPath(tp, orphan), longAgo) shouldBe true

      h.b.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(11)).isLeft shouldBe true
      h.a.confirmMasterFenced(tp) shouldBe true
      val lockBefore = h.readLock(h.granularLockPath(tp, orphan)).value

      h.a.sweepOrphanedLocks()

      h.a.gcQueueSize shouldBe 0
      h.storage.pathExists(h.bucket, h.sweepMarkerPath(tp)).value shouldBe false
      h.a.drainGcQueue()
      h.readLock(h.granularLockPath(tp, orphan)).value shouldBe lockBefore
    } finally h.close()
  }

  test("the periodic batch temp sweep skips a fenced partition") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(9)), None))
      h.a.open(Set(tp)).value
      h.b.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(10)).isLeft shouldBe true
      h.a.confirmMasterFenced(tp) shouldBe true
      // An aged, unreferenced temp staged after both opens: an orphan to any unfenced sweep.
      val temp = h.batchTempPrefix(tp) + "batch-uuid/data/topic/0/staged.json"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      h.storage.setLastModified(h.bucket, temp, longAgo) shouldBe true

      h.a.sweepAllBatchTemps()

      h.storage.pathExists(h.bucket, temp).value shouldBe true
    } finally h.close()
  }

  test("a periodic batch temp sweep that reads a newer owner's master lock is fenced and deletes nothing") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(9)), None))
      h.a.open(Set(tp)).value
      // B takes the partition over; A has not noticed yet.
      h.b.open(Set(tp)).value
      h.a.fencedPartitions shouldBe empty
      // An aged temp under the shared prefix: B's upload that its master lock does not reference yet.
      val temp = h.batchTempPrefix(tp) + "batch-uuid/data/topic/0/staged.json"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      h.storage.setLastModified(h.bucket, temp, longAgo) shouldBe true
      val masterReads = h.storage.readCount(h.bucket, h.masterLockPath(tp))

      h.a.sweepAllBatchTemps()

      h.storage.pathExists(h.bucket, temp).value shouldBe true
      h.a.fencedPartitions should contain(tp)
      // The sweep's own master read decided it: no extra GET.
      h.storage.readCount(h.bucket, h.masterLockPath(tp)) shouldBe masterReads + 1
      h.a.update(tp, Some(Offset(10)), None).isLeft shouldBe true
    } finally h.close()
  }

  test("the periodic batch temp sweep skips a partition fenced while its master lock is still ours") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(9)), None))
      // A granular lock written by a newer owner fences this open's legacy snapshot; the master
      // lock still carries this instance's own bump, so only the fence keeps the sweep out.
      val _ = h.storage
        .writeBlobToFile(
          h.bucket,
          h.granularLockPath(tp, pk),
          NoOverwriteExistingObject(IndexFile(h.lockUuid(1), Some(Offset(9)), None, Some(5000L))),
        )
        .value
      h.a.open(Set(tp)).value
      h.a.fencedPartitions should contain(tp)
      storedMaster(h).owner shouldBe h.lockUuid(0)
      h.a.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(9))
      val temp = h.batchTempPrefix(tp) + "batch-uuid/data/topic/0/staged.json"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      h.storage.setLastModified(h.bucket, temp, longAgo) shouldBe true

      h.a.sweepAllBatchTemps()

      h.storage.pathExists(h.bucket, temp).value shouldBe true
    } finally h.close()
  }

  test("the batch temp sweep at the end of open skips a partition the open found itself fenced on") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      // A granular lock written by a newer owner fences this open's legacy snapshot.
      val _ = h.storage
        .writeBlobToFile(
          h.bucket,
          h.granularLockPath(tp, pk),
          NoOverwriteExistingObject(IndexFile(h.lockUuid(1), Some(Offset(9)), None, Some(5000L))),
        )
        .value
      val temp = h.batchTempPrefix(tp) + "batch-uuid/data/topic/0/staged.json"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      h.storage.setLastModified(h.bucket, temp, longAgo) shouldBe true

      h.a.open(Set(tp)).value

      h.a.fencedPartitions should contain(tp)
      h.storage.pathExists(h.bucket, temp).value shouldBe true
    } finally h.close()
  }
}
