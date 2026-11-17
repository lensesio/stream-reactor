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
import cats.data.Validated
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.AfterReadAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailDeleteAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailMoveAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.LoseWriteResponseAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.OnReadAt
import org.scalatest.Assertion
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Ownership epoch: assigned on open, stamped on every lock write, and enforced on every load. */
class OwnershipEpochTest extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private val tp = Topic("topic").withPartition(0)

  private def seedMaster(h: TwoInstanceHarness, idx: IndexFile): Unit = {
    val _ = h.storage.writeBlobToFile(h.bucket, h.masterLockPath(tp), NoOverwriteExistingObject(idx)).value
  }

  private def storedMaster(h: TwoInstanceHarness): IndexFile =
    h.readLock(h.masterLockPath(tp)).value.wrappedObject

  /** Asserts `result` is a refusal whose message carries `text`. */
  private def refusedWith(result: Either[SinkError, Any], text: String): Assertion =
    result.left.value.message() should include(text)

  /** Overwrites the master lock with `idx` (conditional on its current eTag), as another write would. */
  private def overwriteMaster(h: TwoInstanceHarness, idx: IndexFile): ObjectWithETag[IndexFile] = {
    val current = h.readLock(h.masterLockPath(tp)).value
    h.storage.writeBlobToFile(h.bucket, h.masterLockPath(tp), ObjectWithETag(idx, current.eTag)).value
  }

  test("opening a brand-new partition creates the master lock with epoch = clock") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value

      storedMaster(h).epoch shouldBe Some(1000L)
      h.a.heldEpochOf(tp) shouldBe Some(1000L)
    } finally h.close()
  }

  test("opening over a pre-epoch clean lock takes epoch = clock (old version dropped the field)") {
    val h = new TwoInstanceHarness()
    try {
      seedMaster(h, IndexFile("old-owner", Some(Offset(41)), None))
      h.clock.set(1000L)

      h.a.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(41)))

      storedMaster(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(41)), None, Some(1000L))
      h.a.heldEpochOf(tp) shouldBe Some(1000L)
    } finally h.close()
  }

  test("opening over a clean lock whose epoch is ahead of the clock takes previous + 1") {
    val h = new TwoInstanceHarness()
    try {
      seedMaster(h, IndexFile("prev-owner", Some(Offset(41)), None, Some(5000L)))
      h.clock.set(1000L)

      h.a.open(Set(tp)).value

      storedMaster(h).epoch shouldBe Some(5001L)
      h.a.heldEpochOf(tp) shouldBe Some(5001L)
    } finally h.close()
  }

  test("opening over a 0-byte poison master lock takes epoch = clock") {
    val h = new TwoInstanceHarness()
    try {
      h.storage.putRaw(h.bucket, h.masterLockPath(tp), Array.emptyByteArray)
      h.clock.set(1000L)

      h.a.open(Set(tp)).value

      storedMaster(h).epoch shouldBe Some(1000L)
      h.a.heldEpochOf(tp) shouldBe Some(1000L)
    } finally h.close()
  }

  test("opening over a master lock with a PendingState bumps the epoch before recovery") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      val (chain, first, _) = twoCopyChain(h, pendingOffset = 50L)
      seedMaster(h, IndexFile("crashed-owner", Some(Offset(41)), Some(chain), Some(7000L)))
      // Recovery fails on its very first step, so no chain checkpoint (update) ever runs: whatever
      // is stored afterwards is the ownership bump's own write.
      val _ = h.storage.arm(FailMoveAt(h.bucket, first))

      h.a.open(Set(tp)).isLeft shouldBe true

      storedMaster(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(41)), Some(chain), Some(7001L))
    } finally h.close()
  }

  test("successive owners get strictly increasing epochs even when the clock is frozen") {
    val h = new TwoInstanceHarness(instanceCount = 3)
    try {
      h.clock.set(1000L)
      h.instances(0).open(Set(tp)).value
      h.instances(1).open(Set(tp)).value
      h.instances(2).open(Set(tp)).value

      h.instances(0).heldEpochOf(tp) shouldBe Some(1000L)
      h.instances(1).heldEpochOf(tp) shouldBe Some(1001L)
      h.instances(2).heldEpochOf(tp) shouldBe Some(1002L)
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

      storedGranular(h).epoch shouldBe h.a.heldEpochOf(tp)
    } finally h.close()
  }

  test("a 0-byte poison granular lock is overwritten with the held epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.storage.putRaw(h.bucket, h.granularLockPath(tp, pk), Array.emptyByteArray)

      h.a.ensureGranularLock(tp, pk).value

      storedGranular(h).epoch shouldBe h.a.heldEpochOf(tp)
    } finally h.close()
  }

  test("a granular commit (updateForPartitionKey) keeps the held epoch on the lock") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value

      storedGranular(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(5)), None, h.a.heldEpochOf(tp))
    } finally h.close()
  }

  test("a master floor write (updateMasterLock) keeps the held epoch on the master lock") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(10)).value

      storedMaster(h).epoch shouldBe h.a.heldEpochOf(tp)
      storedMaster(h).committedOffset shouldBe Some(Offset(9))
    } finally h.close()
  }

  test("a master commit (update, used by batch and non-PARTITIONBY commits) keeps the held epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.update(tp, Some(Offset(12)), None).value

      storedMaster(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(12)), None, h.a.heldEpochOf(tp))
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

      storedGranular(h) shouldBe IndexFile(h.lockUuid(0), Some(Offset(8)), None, h.a.heldEpochOf(tp))
    } finally h.close()
  }

  /**
   * Stages two temp objects and returns a two-Copy chain over them, with the two temp paths. When
   * the second Copy is made to fail, the chain stops after its first checkpoint, which leaves the
   * intermediate PendingState write (the remaining Copy) persisted in the lock.
   */
  private def twoCopyChain(h: TwoInstanceHarness, pendingOffset: Long): (PendingState, String, String) = {
    def stage(name: String): (String, String) = {
      val temp = s"data/topic/0/.temp-upload/$name"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString(name)).value
      temp -> h.storage.getBlobAsStringAndEtag(h.bucket, temp).value._2
    }
    val (t1, e1) = stage("first")
    val (t2, e2) = stage("second")
    val chain = PendingState(
      Offset(pendingOffset),
      NonEmptyList.of(CopyOperation(h.bucket, t1, "data/topic/0/first", e1),
                      CopyOperation(h.bucket, t2, "data/topic/0/second", e2),
      ),
    )
    (chain, t1, t2)
  }

  private def remainingOps(idx: IndexFile): List[FileOperation] =
    idx.pendingState.toList.flatMap(_.pendingOperations.toList)

  test("an intermediate master PendingState checkpoint (update) carries the held epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      val (chain, _, second) = twoCopyChain(h, pendingOffset = 50L)
      seedMaster(h, IndexFile("crashed-owner", Some(Offset(41)), Some(chain), Some(7000L)))
      val _ = h.storage.arm(FailMoveAt(h.bucket, second))

      h.a.open(Set(tp)).isLeft shouldBe true

      // Interrupted after the first Copy: the stored lock is the checkpoint holding the second one.
      val stored = storedMaster(h)
      remainingOps(stored).map { case c: CopyOperation => c.source; case other => other.toString } shouldBe
        List(second)
      stored.owner shouldBe h.lockUuid(0)
      stored.epoch shouldBe Some(7001L)
    } finally h.close()
  }

  test("an intermediate granular PendingState checkpoint (updateForPartitionKey) carries the held epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      val (chain, _, second) = twoCopyChain(h, pendingOffset = 8L)
      val _ = h.storage
        .writeBlobToFile(h.bucket,
                         h.granularLockPath(tp, pk),
                         NoOverwriteExistingObject(IndexFile("crashed", Some(Offset(3)), Some(chain))),
        )
        .value
      val _ = h.storage.arm(FailMoveAt(h.bucket, second))

      h.a.getSeekedOffsetForPartitionKey(tp, pk).isLeft shouldBe true

      val stored = storedGranular(h)
      remainingOps(stored).map { case c: CopyOperation => c.source; case other => other.toString } shouldBe
        List(second)
      stored.owner shouldBe h.lockUuid(0)
      stored.epoch shouldBe h.a.heldEpochOf(tp)
      stored.epoch shouldBe defined
    } finally h.close()
  }

  test("a lock write without a held epoch fails fatally and writes nothing") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      val masterBefore   = h.readLock(h.masterLockPath(tp)).value
      val granularBefore = h.readLock(h.granularLockPath(tp, pk)).value
      // No production path clears the epoch while the master eTag stays cached (both are set by the
      // ownership write and cleared together), so the master writes' epoch check is reachable only
      // through the bug it guards against; the hook drops the epoch alone to stand in for it. The
      // realistic path, state cleared under a cached granular key, is covered by the next test.
      IndexManagerV2.IndexManagerV2TestHooks.dropHeldEpoch(h.a, tp)

      refusedWith(h.a.updateMasterLock(tp, Offset(10)), "No ownership epoch held")
      refusedWith(h.a.update(tp, Some(Offset(12)), None), "No ownership epoch held")
      refusedWith(h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None), "No ownership epoch held")

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

      refusedWith(h.a.getSeekedOffsetForPartitionKey(tp, pk), "No ownership epoch held")
      // An instance that never opened the partition refuses it the same way.
      refusedWith(h.b.getSeekedOffsetForPartitionKey(tp, pk), "No ownership epoch held")

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
      refusedWith(h.a.getSeekedOffsetForPartitionKey(tp, pk), "epoch 1001 > held epoch 1000")
      h.a.fencedPartitions.contains(tp) shouldBe true
      // Nothing was cached by the rejected load: a second lookup is rejected again, not served.
      h.a.granularCacheSize shouldBe 0
      refusedWith(h.a.getSeekedOffsetForPartitionKey(tp, pk), "Fenced on")

      // A cached nothing, so it holds no token to write with, and K is untouched.
      refusedWith(h.a.updateForPartitionKey(tp, pk, Some(Offset(7)), None), "not in cache")
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

      refusedWith(h.a.ensureGranularLock(tp, pk), "epoch 1001 > held epoch 1000")
      h.a.fencedPartitions.contains(tp) shouldBe true
    } finally h.close()
  }

  test("a zombie never drives a pending chain on a lock written under a newer epoch") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value // epoch 1000
      h.b.open(Set(tp)).value // epoch 1001
      val temp = "data/topic/0/.temp-upload/in-flight"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      val tempETag = h.storage.getBlobAsStringAndEtag(h.bucket, temp).value._2
      val pending  = PendingState(Offset(8), NonEmptyList.one(DeleteOperation(h.bucket, temp, tempETag)))
      val _ = h.storage
        .writeBlobToFile(
          h.bucket,
          h.granularLockPath(tp, pk),
          NoOverwriteExistingObject(IndexFile(h.lockUuid(1), Some(Offset(3)), Some(pending), Some(1001L))),
        )
        .value
      val before = h.readLock(h.granularLockPath(tp, pk)).value

      refusedWith(h.a.getSeekedOffsetForPartitionKey(tp, pk), "epoch 1001 > held epoch 1000")

      h.storage.pathExists(h.bucket, temp).value shouldBe true
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
      h.b.open(Set(tp)).value // epoch 1001: the newer owner loads the lower-epoch lock

      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
      h.b.fencedPartitions.contains(tp) shouldBe false

      // B writes the key itself, so the lock now carries B's own epoch; an idle eviction and reload
      // of a lock at exactly the held epoch is not a fence either.
      h.b.updateForPartitionKey(tp, pk, Some(Offset(7)), None).value
      h.readLock(h.granularLockPath(tp, pk)).value.wrappedObject.epoch shouldBe h.b.heldEpochOf(tp)
      h.b.evictGranularLock(tp, pk)
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(7))
      h.b.fencedPartitions.contains(tp) shouldBe false
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
      refusedWith(h.instances(1).getSeekedOffsetForPartitionKey(tp, pk), "epoch 5000 > held epoch 1000")

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
      bRestarted.heldEpochOf(tp) shouldBe Some(1002L)
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
      refusedWith(h.a.updateMasterLock(tp, Offset(20)), "Fenced on")
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

  test("a master lock with our owner and epoch but a pending state is not adopted") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(10)).value
      val cachedBefore = h.a.topicPartitionToETags.get(tp)
      val pending = PendingState(
        Offset(99),
        NonEmptyList.one(DeleteOperation(h.bucket, "data/topic/0/.temp-upload/in-flight", "temp-etag")),
      )
      // Our lockUuid and epoch, a new version, but a pending chain: not the shape of a floor write.
      val stored = overwriteMaster(h, IndexFile(h.lockUuid(0), Some(Offset(99)), Some(pending), Some(1000L)))
      Some(stored.eTag) should not be cachedBefore

      h.a.confirmMasterFenced(tp) shouldBe false

      h.a.fencedPartitions shouldBe empty
      h.a.topicPartitionToETags.get(tp) shouldBe cachedBefore
      h.a.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(9))
    } finally h.close()
  }

  test("adopting our own landed master write moves the seeked offset forward only, never back") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(41)), None))
      h.a.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(41)))
      // Our lockUuid and epoch, no pending state, a new version, but a floor below the seeked offset.
      val stored = overwriteMaster(h, IndexFile(h.lockUuid(0), Some(Offset(20)), None, Some(1000L)))

      h.a.confirmMasterFenced(tp) shouldBe false

      h.a.fencedPartitions shouldBe empty
      h.a.topicPartitionToETags.get(tp) shouldBe Some(stored.eTag)
      h.a.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(41))
    } finally h.close()
  }

  test("a master lock with our owner but a higher epoch fences us") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      val _ = overwriteMaster(h, IndexFile(h.lockUuid(0), Some(Offset(9)), None, Some(1001L)))

      h.a.confirmMasterFenced(tp) shouldBe true

      h.a.fencedPartitions should contain(tp)
      h.a.fencingReason(tp).getOrElse("") should include("epoch 1001, held epoch 1000")
      refusedWith(h.a.updateMasterLock(tp, Offset(20)), "Fenced on")
    } finally h.close()
  }

  test("a lost-response master write whose bytes equal the stored lock leaves the cached eTag valid") {
    // Content-derived eTags (S3's single-part PUT): rewriting identical bytes keeps the eTag, so the
    // write that landed is indistinguishable from the version already cached and nothing is adopted.
    val h = new TwoInstanceHarness(contentETags = true)
    try {
      h.a.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(10)).value
      val cached = h.a.topicPartitionToETags.get(tp)
      h.storage.arm(LoseWriteResponseAt(h.bucket, h.masterLockPath(tp)))

      h.a.updateMasterLock(tp, Offset(10)).isLeft shouldBe true
      Some(h.readLock(h.masterLockPath(tp)).value.eTag) shouldBe cached

      h.a.confirmMasterFenced(tp) shouldBe false
      h.a.fencedPartitions shouldBe empty
      h.a.topicPartitionToETags.get(tp) shouldBe cached
      h.a.updateMasterLock(tp, Offset(20)).value
      storedMaster(h).committedOffset shouldBe Some(Offset(19))
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

  test("clearing the partition state drops the held epoch: a key still cached can no longer be written") {
    val h = new TwoInstanceHarness()
    try {
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value
      val before = h.readLock(h.granularLockPath(tp, pk)).value

      // Only the partition state goes; the key's cached eTag is still there to write with.
      h.a.clearTopicPartitionState(tp)

      refusedWith(h.a.updateForPartitionKey(tp, pk, Some(Offset(7)), None), "No ownership epoch held")
      h.readLock(h.granularLockPath(tp, pk)).value shouldBe before
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
      val epoch = h.a.heldEpochOf(tp)
      val eTag  = h.a.topicPartitionToETags.get(tp)

      // A cooperative rebalance adds `other` and keeps `tp`: open() receives only the added partition.
      h.a.open(Set(other)).value

      h.a.heldEpochOf(tp) shouldBe epoch
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

      h.a.closePartitions(Set(tp))

      // Not this task's to fail on any more, and nothing of it is served or written.
      h.a.fencedPartitions shouldBe empty
      h.a.fencingReason(tp) shouldBe None
      refusedWith(h.a.getSeekedOffsetForPartitionKey(tp, pk), "No ownership epoch held")
      refusedWith(h.a.updateMasterLock(tp, Offset(7)), "Master index not found")
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
      val before = h.a.heldEpochOf(tp).value

      h.a.closePartitions(Set(tp, other))
      h.a.heldEpochOf(tp) shouldBe None
      h.a.heldEpochOf(other) shouldBe None
      h.a.topicPartitionToETags shouldBe empty
      h.a.getSeekedOffsetForTopicPartition(tp) shouldBe None

      h.a.open(Set(tp, other)).value shouldBe Map(tp -> Some(Offset(3)), other -> None)
      h.a.heldEpochOf(tp).value should be > before
      storedMaster(h).epoch shouldBe h.a.heldEpochOf(tp)
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(7)), None).value shouldBe Some(Offset(7))
      h.a.updateMasterLock(tp, Offset(8)).value
    } finally h.close()
  }

  // Shutdown/revoke cleanup promise: closing a partition no longer deletes its queued obsolete
  // locks. The queued deletes are by path, and the writer close that runs first evicts the granular
  // cache, which is the only thing telling the drain that a queued key has since been reclaimed and
  // rewritten. The obsolete locks a close drops stay in storage for the orphan sweep.

  test("closing a partition drops its queued GC deletes: the obsolete lock is left for the orphan sweep") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value
      h.a.gcQueueSize shouldBe 1

      h.a.closePartitions(Set(tp))

      h.readLock(h.granularLockPath(tp, pk)).value.wrappedObject.committedOffset shouldBe Some(Offset(3))
      h.a.gcQueueSize shouldBe 0
      h.metrics(0).getGcLocksSkippedRevoked shouldBe 1L
      h.metrics(0).getGcLocksDeleted shouldBe 0L
    } finally h.close()
  }

  test("a reclaimed key's lock rewritten above the master survives the writer close and the partition close") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(9)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(9), Set.empty).value // queues the key's lock path
      // The key comes back before the drain: a new writer reclaims it and commits up to 12, above
      // the master, so the lock is now the key's only dedup floor for offsets 9..12.
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(12)), None).value
      val current = h.readLock(h.granularLockPath(tp, pk)).value
      current.wrappedObject.committedOffset shouldBe Some(Offset(12))

      // CloudSinkTask.close(partitions): the writer close evicts the key's cache entry first.
      h.a.evictAllGranularLocks(tp)
      h.a.closePartitions(Set(tp))

      h.readLock(h.granularLockPath(tp, pk)).value shouldBe current
      h.a.gcQueueSize shouldBe 0
    } finally h.close()
  }

  test("closing one partition drops only its queued GC deletes; a retained partition's are still drained") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp, other)).value
      Seq(tp, other).foreach { p =>
        h.a.ensureGranularLock(p, pk).value
        h.a.updateForPartitionKey(p, pk, Some(Offset(3)), None).value
        h.a.updateMasterLock(p, Offset(10)).value
        h.a.cleanUpObsoleteLocks(p, Offset(10), Set.empty).value
      }
      h.a.gcQueueSize shouldBe 2

      h.a.closePartitions(Set(tp))
      h.a.gcQueueSize shouldBe 1
      h.a.drainGcQueue()

      h.readLock(h.granularLockPath(tp, pk)).isRight shouldBe true
      h.readLock(h.granularLockPath(other, pk)).isLeft shouldBe true
    } finally h.close()
  }

  test("a revoke-only close resumes scheduled GC for the partitions still held, with no open() after it") {
    val h = new TwoInstanceHarness(gcSeconds = 1)
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp, other)).value
      // CloudSinkTask.close(partitions) suspends background work first; queue the retained
      // partition's obsolete lock only then, so no tick before the close can delete it.
      h.a.suspendBackgroundWork()
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value

      // Cooperative revoke of `other` only: Connect skips open() when nothing is added.
      h.a.closePartitions(Set(other))

      h.a.acceptingWork shouldBe true
      val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10)
      while (h.readLock(h.granularLockPath(tp, pk)).isRight && System.nanoTime() < deadline) Thread.sleep(50)
      h.readLock(h.granularLockPath(tp, pk)).isLeft shouldBe true
    } finally h.close()
  }

  test("closing every held partition keeps scheduled background work suspended") {
    // A one-second sweep tick: every scheduled run the gate lets through counts a sweep run.
    val h = new TwoInstanceHarness(sweepEnabled = true, sweepSeconds = 1)
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp, other)).value
      h.a.suspendBackgroundWork()
      val runsBefore = h.metrics(0).getSweepRuns

      h.a.closePartitions(Set(tp, other))

      Thread.sleep(2500L) // past two ticks
      h.metrics(0).getSweepRuns shouldBe runsBefore
      // The gate, not a stopped scheduler, held it back: the next open() lets the ticks run again.
      h.a.open(Set(tp)).value
      val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10)
      while (h.metrics(0).getSweepRuns == runsBefore && System.nanoTime() < deadline) Thread.sleep(50)
      h.metrics(0).getSweepRuns should be > runsBefore
    } finally h.close()
  }

  test("a close whose release of one partition throws still releases the rest, rethrows, and stops its sweep") {
    implicit val validator: CloudLocationValidator = (l: CloudLocation) => Validated.valid(l)
    val storage     = new InMemoryStorageInterface()
    val taskId      = ConnectorTaskId("release-failure", 1, 0)
    val failing     = other
    val failRelease = new java.util.concurrent.atomic.AtomicBoolean(false)
    val im = new IndexManagerV2(
      bucketAndPrefixFn =
        (p: TopicPartition) => Right(CloudLocation("test-bucket", Some(s"data/${p.topic.value}/${p.partition}/"))),
      pendingOperationsProcessors = new PendingOperationsProcessors(storage)(taskId),
      directoryFileName           = ".indexes2",
      gcSweepEnabled              = true,
      gcSweepIntervalSeconds      = 3600,
      gcSweepMinAgeSeconds        = 3600,
      commitMode                  = CommitMode.Batch,
    )(storage, taskId) {
      // The first step of releasing a partition; a failure here skips the rest of its release.
      override def evictAllGranularLocks(topicPartition: TopicPartition): Unit =
        if (failRelease.get() && topicPartition == failing) throw new IllegalStateException("release failed")
        else super.evictAllGranularLocks(topicPartition)
    }
    try {
      Seq(tp, failing).foreach { p =>
        val _ = storage.writeBlobToFile(
          "test-bucket",
          IndexManagerV2.generateLockFilePath(taskId, p, ".indexes2"),
          NoOverwriteExistingObject(IndexFile("previous-owner", Some(Offset(9)), None)),
        ).value
      }
      im.open(Set(tp, failing)).value
      // An aged, unreferenced temp of the partition whose release fails: an orphan to any sweep of it.
      val temp = s".temp-upload/release-failure/${failing.topic}/${failing.partition}/batch-uuid/staged.json"
      storage.writeStringToFile("test-bucket", temp, UploadableString("payload")).value
      storage.setLastModified("test-bucket", temp, java.time.Instant.now().minusSeconds(7200L)) shouldBe true
      failRelease.set(true)

      intercept[IllegalStateException](im.closePartitions(Set(tp, failing))).getMessage shouldBe "release failed"

      // The other partition was released all the same.
      im.heldEpochOf(tp) shouldBe None
      im.getSeekedOffsetForTopicPartition(tp) shouldBe None
      // The failing one kept its state, but it left the owned set before its release ran, so a
      // sweep (which still visits it) deletes nothing under it.
      im.getSeekedOffsetForTopicPartition(failing) shouldBe Some(Offset(9))
      im.sweepAllBatchTemps()
      storage.pathExists("test-bucket", temp).value shouldBe true
    } finally {
      failRelease.set(false)
      im.close()
    }
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
      h.a.heldEpochOf(tp) shouldBe Some(1002L)

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
      refusedWith(h.a.getSeekedOffsetForPartitionKey(tp, pk), "epoch 1001 > held epoch 1000")
      h.a.fencedPartitions should contain(tp)
      val otherBefore = h.readLock(h.granularLockPath(tp, other)).value

      refusedWith(h.a.updateForPartitionKey(tp, other, Some(Offset(4)), None), "Fenced on")
      refusedWith(h.a.updateMasterLock(tp, Offset(4)), "Fenced on")
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
        NoOverwriteExistingObject(IndexFile(h.lockUuid(0), Some(Offset(3)), Some(pending), h.a.heldEpochOf(tp))),
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

      refusedWith(h.a.getSeekedOffsetForPartitionKey(tp, other), "Fenced on")

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

      refusedWith(h.a.ensureGranularLock(tp, other), "Fenced on")

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

      refusedWith(h.a.getSeekedOffsetForPartitionKey(tp, other), "Fenced on")
      refusedWith(h.a.ensureGranularLock(tp, other), "Fenced on")
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

  test("a GC delete re-queued after a close does not survive the re-open: the new owner's lock and floor are kept") {
    val h = new TwoInstanceHarness(sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(3)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      // The key's writer went idle: below the floor, uncached and aged, so the sweep reads it.
      h.a.evictGranularLock(tp, pk)
      h.storage.setLastModified(h.bucket, h.granularLockPath(tp, pk), longAgo) shouldBe true
      // The partition is revoked while the orphan sweep reads the key's lock: the sweep queues it
      // after the close has dropped the partition's queued deletes.
      h.storage.arm(OnReadAt(h.bucket, h.granularLockPath(tp, pk), () => h.a.closePartitions(Set(tp))))
      h.a.sweepOrphanedLocks()
      h.a.heldEpochOf(tp) shouldBe None
      h.a.gcQueueSize shouldBe 1

      // B owns the partition for a while and commits the key above the floor.
      h.b.open(Set(tp)).value
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value
      h.b.updateForPartitionKey(tp, pk, Some(Offset(12)), None).value
      val bLock = h.readLock(h.granularLockPath(tp, pk)).value

      // The partition is assigned back to this instance.
      h.a.open(Set(tp)).value
      h.a.drainGcQueue()

      h.readLock(h.granularLockPath(tp, pk)).value shouldBe bLock
      h.a.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(12))
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
      refusedWith(h.a.update(tp, Some(Offset(10)), None), "Fenced on")
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

  test("a batch temp sweep that reads our own owner on the master under a higher epoch is fenced and deletes nothing") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(9)), None))
      h.a.open(Set(tp)).value // epoch 1000
      // Same lockUuid, newer epoch: only the epoch tells this lock is not ours any more.
      val _    = overwriteMaster(h, IndexFile(h.lockUuid(0), Some(Offset(9)), None, Some(1001L)))
      val temp = h.batchTempPrefix(tp) + "batch-uuid/data/topic/0/staged.json"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      h.storage.setLastModified(h.bucket, temp, longAgo) shouldBe true

      h.a.sweepAllBatchTemps()

      h.storage.pathExists(h.bucket, temp).value shouldBe true
      h.a.fencedPartitions should contain(tp)
      h.a.fencingReason(tp).getOrElse("") should include("epoch 1001, held epoch 1000")
    } finally h.close()
  }

  test("a batch temp sweep whose partition is released during a delete chunk's master read deletes nothing") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(9)), None))
      h.a.open(Set(tp)).value
      val temp = h.batchTempPrefix(tp) + "batch-uuid/data/topic/0/staged.json"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      h.storage.setLastModified(h.bucket, temp, longAgo) shouldBe true
      // The pre-LIST master read passes. The chunk's owned-set check passes too, then another
      // thread's close clears the partition state while the chunk re-reads the master: only the
      // missing held epoch is left to stop the delete.
      val masterPath = h.masterLockPath(tp)
      def clearOnNextMasterRead(): Unit = {
        val _ = h.storage.arm(OnReadAt(h.bucket, masterPath, () => h.a.clearTopicPartitionState(tp)))
      }
      h.storage.arm(OnReadAt(h.bucket, masterPath, () => clearOnNextMasterRead()))

      h.a.sweepAllBatchTemps()

      h.a.heldEpochOf(tp) shouldBe None
      h.storage.pathExists(h.bucket, temp).value shouldBe true
    } finally h.close()
  }

  test("a fence that lands while a key's lock is being read refuses the load: its pending chain is not driven") {
    val h = new TwoInstanceHarness()
    try {
      val sibling = "date=2024-01-02"
      h.clock.set(1000L)
      h.a.open(Set(tp)).value // epoch 1000
      // `pk` carries this instance's own epoch, with a pending Delete of a real temp object: the
      // epoch read rule alone would accept it and drive the Delete.
      val temp = "data/topic/0/.temp-upload/own-epoch-pending"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      val tempETag = h.storage.getBlobAsStringAndEtag(h.bucket, temp).value._2
      val pending  = PendingState(Offset(8), NonEmptyList.one(DeleteOperation(h.bucket, temp, tempETag)))
      val _ = h.storage
        .writeBlobToFile(
          h.bucket,
          h.granularLockPath(tp, pk),
          NoOverwriteExistingObject(IndexFile(h.lockUuid(0), Some(Offset(3)), Some(pending), Some(1000L))),
        )
        .value
      h.b.open(Set(tp)).value
      h.b.ensureGranularLock(tp, sibling).value // the sibling key carries B's epoch
      val before = h.readLock(h.granularLockPath(tp, pk)).value
      // After this load's own fence check, while it reads pk's lock, another thread's load of the
      // sibling key fences the partition.
      def loadSibling(): Unit = {
        val _ = h.a.getSeekedOffsetForPartitionKey(tp, sibling)
      }
      h.storage.arm(OnReadAt(h.bucket, h.granularLockPath(tp, pk), () => loadSibling()))

      refusedWith(h.a.getSeekedOffsetForPartitionKey(tp, pk), "Fenced on")

      h.a.fencedPartitions should contain(tp)
      h.storage.pathExists(h.bucket, temp).value shouldBe true
      h.readLock(h.granularLockPath(tp, pk)).value shouldBe before
      h.a.granularCacheSize shouldBe 0
    } finally h.close()
  }

  test("a 0-byte legacy lock read by a batch open is overwritten with the held epoch") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch)
    try {
      h.clock.set(1000L)
      h.storage.putRaw(h.bucket, h.granularLockPath(tp, pk), Array.emptyByteArray)
      // Keep the lock past the purge that follows a fully resolved snapshot, to read what was written.
      h.storage.arm(FailDeleteAt(h.bucket, h.granularLockPath(tp, pk)))

      h.a.open(Set(tp)).value

      storedGranular(h) shouldBe IndexFile(h.lockUuid(0), None, None, Some(1000L))
      h.a.heldEpochOf(tp) shouldBe Some(1000L)
    } finally h.close()
  }

  // An older connector version takes over without writing an epoch (read as 0, below ours): only
  // the owner tells the lock is no longer ours.

  test("a master lock rewritten by an older-version owner (no epoch) fences us on the fencing check") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      val _ = overwriteMaster(h, IndexFile("old-version-owner", Some(Offset(9)), None))

      h.a.confirmMasterFenced(tp) shouldBe true

      h.a.fencedPartitions should contain(tp)
      h.a.fencingReason(tp).getOrElse("") should include("owner old-version-owner")
    } finally h.close()
  }

  test("a batch temp sweep that reads an older-version owner's master (no epoch) is fenced and deletes nothing") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(9)), None))
      h.a.open(Set(tp)).value
      val _    = overwriteMaster(h, IndexFile("old-version-owner", Some(Offset(9)), None))
      val temp = h.batchTempPrefix(tp) + "batch-uuid/data/topic/0/staged.json"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
      h.storage.setLastModified(h.bucket, temp, longAgo) shouldBe true

      h.a.sweepAllBatchTemps()

      h.storage.pathExists(h.bucket, temp).value shouldBe true
      h.a.fencedPartitions should contain(tp)
    } finally h.close()
  }

  // ---- a sweep decides on the epoch held when it read the master lock ---------------------

  test("a batch temp sweep whose partition this instance re-takes after the sweep's master read does not fence it") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(9)), None))
      h.a.open(Set(tp)).value // epoch 1000
      h.b.open(Set(tp)).value // epoch 1001; this instance has not noticed
      // The sweep reads B's master lock. Before it decides on it, a rebalance closes the partition
      // here and assigns it back to this instance, which re-takes it above B.
      h.storage.arm(
        AfterReadAt(
          h.bucket,
          h.masterLockPath(tp),
          () => {
            h.a.closePartitions(Set(tp))
            val _ = h.a.open(Set(tp)).value
          },
        ),
      )

      h.a.sweepAllBatchTemps()

      h.a.heldEpochOf(tp) shouldBe Some(1002L)
      h.a.isFenced(tp) shouldBe false
      h.a.fencedPartitions shouldBe empty
      h.a.update(tp, Some(Offset(10)), None).value shouldBe Some(Offset(10))
    } finally h.close()
  }

  test("a delete chunk whose partition this instance re-takes after the chunk's master read does not fence it") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(9)), None))
      h.a.open(Set(tp)).value // epoch 1000
      val temp       = h.batchTempPrefix(tp) + "batch-uuid/data/topic/0/staged.json"
      val masterPath = h.masterLockPath(tp)
      // The pre-LIST read sees this instance's own master lock. B then takes the partition over and
      // stages an aged temp the LIST picks up. The chunk's re-read sees B's master lock and, before
      // the chunk decides on it, this instance closes and re-takes the partition above B.
      val retakeAfterChunkRead = () => {
        h.a.closePartitions(Set(tp))
        val _ = h.a.open(Set(tp)).value
      }
      h.storage.arm(
        AfterReadAt(
          h.bucket,
          masterPath,
          () => {
            val _ = h.b.open(Set(tp)).value // epoch 1001
            h.storage.writeStringToFile(h.bucket, temp, UploadableString("payload")).value
            h.storage.setLastModified(h.bucket, temp, longAgo) shouldBe true
            val _ = h.storage.arm(AfterReadAt(h.bucket, masterPath, retakeAfterChunkRead))
          },
        ),
      )

      h.a.sweepAllBatchTemps()

      h.a.heldEpochOf(tp) shouldBe Some(1002L)
      h.a.isFenced(tp) shouldBe false
      h.a.fencedPartitions shouldBe empty
      h.a.update(tp, Some(Offset(10)), None).value shouldBe Some(Offset(10))
    } finally h.close()
  }

  test("a batch temp sweep whose partition is closed after the sweep's master read leaves no fence behind") {
    val h = new TwoInstanceHarness(commitMode = CommitMode.Batch, sweepEnabled = true, sweepSeconds = 3600)
    try {
      h.clock.set(1000L)
      seedMaster(h, IndexFile("previous-owner", Some(Offset(9)), None))
      h.a.open(Set(tp)).value
      h.b.open(Set(tp)).value
      h.storage.arm(AfterReadAt(h.bucket, h.masterLockPath(tp), () => h.a.closePartitions(Set(tp))))

      h.a.sweepAllBatchTemps()

      h.a.heldEpochOf(tp) shouldBe None
      h.a.isFenced(tp) shouldBe false
      h.a.fencedPartitions shouldBe empty
    } finally h.close()
  }

  test("a fence on a partition this instance no longer holds an epoch for is not reported") {
    val h = new TwoInstanceHarness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.b.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(10)).isLeft shouldBe true
      h.a.confirmMasterFenced(tp) shouldBe true
      h.a.fencedPartitions should contain(tp)

      // The state a fence marked after the partition's close leaves: no held epoch, a fence entry.
      IndexManagerV2.IndexManagerV2TestHooks.dropHeldEpoch(h.a, tp)

      h.a.fencedPartitions shouldBe empty
    } finally h.close()
  }
}
