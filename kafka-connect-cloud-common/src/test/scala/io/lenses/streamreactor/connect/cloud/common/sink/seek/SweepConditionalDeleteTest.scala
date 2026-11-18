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
import io.circe.syntax.EncoderOps
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.AfterReadAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailMoveAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.OnReadAt
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.time.Instant

/**
 * The orphan sweep deletes a lock only at the version it read, and enqueues nothing for a
 * partition whose master lock no longer shows this instance's own last write.
 */
class SweepConditionalDeleteTest extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private val tp = Topic("topic").withPartition(0)
  private val pk = "date=2024-01-01"

  private def harness(sweepMaxReads: Int = IndexManagerV2.DefaultGcSweepMaxReads) =
    new TwoInstanceHarness(sweepEnabled = true, sweepSeconds = 60, sweepMaxReads = sweepMaxReads)

  private def lockPath(h: TwoInstanceHarness, key: String = pk): String = h.granularLockPath(tp, key)

  private def stored(h: TwoInstanceHarness, key: String = pk): Option[IndexFile] =
    h.readLock(lockPath(h, key)).toOption.map(_.wrappedObject)

  private def age(h: TwoInstanceHarness, key: String = pk): Unit = {
    val _ = h.storage.setLastModified(h.bucket, lockPath(h, key), Instant.EPOCH)
  }

  /**
   * `im` (already open) commits `key` at 5, advances the master floor to 19, and lets the key go
   * idle (evicted from its cache), with the lock aged past the sweep threshold: an orphan to its
   * sweep.
   */
  private def idleObsoleteKey(h: TwoInstanceHarness, im: IndexManagerV2, key: String = pk): Unit = {
    im.ensureGranularLock(tp, key).value
    im.updateForPartitionKey(tp, key, Some(Offset(5)), None).value
    im.updateMasterLock(tp, Offset(20)).value
    im.evictGranularLock(tp, key)
    age(h, key)
  }

  private def bOpens(h: TwoInstanceHarness): Unit = {
    val _ = h.b.open(Set(tp)).value
  }

  private def bTakesOverAndCommits(h: TwoInstanceHarness, at: Long): Unit = {
    h.b.open(Set(tp)).value
    val _ = h.b.getSeekedOffsetForPartitionKey(tp, pk).value
    val _ = h.b.updateForPartitionKey(tp, pk, Some(Offset(at)), None).value
  }

  test("a lock the sweep read is not deleted once a new owner has taken it over and committed it") {
    val h = harness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      idleObsoleteKey(h, h.a)
      h.a.sweepOrphanedLocks()
      h.a.gcQueueSize shouldBe 1

      bTakesOverAndCommits(h, at = 25)
      h.a.drainGcQueue()

      stored(h).value shouldBe IndexFile(h.lockUuid(1), Some(Offset(25)), None, Some(1001L))
      h.metrics(0).getGcLocksSkippedChanged shouldBe 1L
    } finally h.close()
  }

  test("a sweep that finds the partition taken over after its lock reads enqueues nothing, and is fenced") {
    val h = harness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      idleObsoleteKey(h, h.a)
      // B opens right after A's sweep has read the key, before A enqueues anything.
      h.storage.arm(AfterReadAt(h.bucket, lockPath(h), () => bOpens(h)))

      h.a.sweepOrphanedLocks()

      h.a.gcQueueSize shouldBe 0
      h.metrics(0).getSweepSkippedOwnershipLost shouldBe 1L
      h.metrics(0).getSweepOrphansEnqueued shouldBe 0L
      h.a.fencedPartitions should contain(tp)
      h.a.drainGcQueue()
      stored(h).value.committedOffset shouldBe Some(Offset(5))
    } finally h.close()
  }

  test("a lock another instance holds at the same epoch is not deleted by the sweep") {
    val h = harness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      idleObsoleteKey(h, h.a)
      // The master is lost, so B's open creates it with the clock-only epoch A also holds.
      h.storage.deleteFiles(h.bucket, Seq(h.masterLockPath(tp))).value
      h.b.open(Set(tp)).value
      h.b.heldEpochOf(tp) shouldBe h.a.heldEpochOf(tp)
      // B takes the key over (same epoch, its owner) and commits below A's floor: the epoch read
      // rule cannot tell this lock from one of A's own.
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
      h.b.updateForPartitionKey(tp, pk, Some(Offset(7)), None).value
      age(h)

      h.a.sweepOrphanedLocks()
      h.a.drainGcQueue()

      stored(h).value shouldBe IndexFile(h.lockUuid(1), Some(Offset(7)), None, Some(1000L))
      h.metrics(0).getSweepSkippedOwnershipLost shouldBe 1L
      h.b.updateForPartitionKey(tp, pk, Some(Offset(9)), None).value shouldBe Some(Offset(9))
    } finally h.close()
  }

  test("an obsolete lock nobody touched is swept and deleted, the master re-check costing one read") {
    val h = harness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      idleObsoleteKey(h, h.a)
      val masterReads = h.storage.readCount(h.bucket, h.masterLockPath(tp))

      h.a.sweepOrphanedLocks()
      h.a.drainGcQueue()

      stored(h) shouldBe None
      h.storage.readCount(h.bucket, h.masterLockPath(tp)) - masterReads shouldBe 1
      h.metrics(0).getSweepGetBudgetUsed shouldBe 2 // the lock and the master
      h.metrics(0).getSweepSkippedOwnershipLost shouldBe 0L
    } finally h.close()
  }

  test("a sweep that selects nothing does not read the master lock") {
    val h = harness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(30)), None).value // above the floor
      h.a.updateMasterLock(tp, Offset(20)).value
      h.a.evictGranularLock(tp, pk)
      age(h)
      val masterReads = h.storage.readCount(h.bucket, h.masterLockPath(tp))

      h.a.sweepOrphanedLocks()

      h.a.gcQueueSize shouldBe 0
      h.storage.readCount(h.bucket, h.masterLockPath(tp)) shouldBe masterReads
      h.metrics(0).getSweepGetBudgetUsed shouldBe 1
    } finally h.close()
  }

  test("the master re-check counts against the sweep's read budget") {
    val h = harness(sweepMaxReads = 3)
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      val keys = (1 to 3).map(n => s"date=2024-01-0$n")
      keys.foreach(k => idleObsoleteKey(h, h.a, k))

      h.a.sweepOrphanedLocks()

      // Two lock reads and the master re-check: three reads, two keys enqueued.
      h.metrics(0).getSweepGetBudgetUsed shouldBe 3
      h.a.gcQueueSize shouldBe 2
      // Each key was read once by its own ensureGranularLock; the sweep read two of them.
      keys.count(k => h.storage.readCount(h.bucket, lockPath(h, k)) == 2) shouldBe 2
    } finally h.close()
  }

  test("a 0-byte lock repaired between the listing and the sweep's read is judged by its content and kept") {
    val h = harness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(20)).value
      h.storage.putRaw(h.bucket, lockPath(h), Array.emptyByteArray)
      age(h)
      val repaired = IndexFile(h.lockUuid(0), Some(Offset(50)), None, Some(1000L))
      // A writer repairs the poison blob after the sweep's listing reported it as 0 bytes.
      h.storage.arm(
        OnReadAt(
          h.bucket,
          lockPath(h),
          () => h.storage.putRaw(h.bucket, lockPath(h), repaired.asJson.noSpaces.getBytes(StandardCharsets.UTF_8)),
        ),
      )

      h.a.sweepOrphanedLocks()
      h.a.drainGcQueue()

      // The sweep read the lock (the hook is spent), and the repaired lock is still there.
      h.storage.disarmAll()
      h.storage.readCount(h.bucket, lockPath(h)) shouldBe 1
      stored(h).value shouldBe repaired
    } finally h.close()
  }

  test("a 0-byte lock still 0 bytes when the sweep reads it is deleted at that version") {
    val h = harness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.updateMasterLock(tp, Offset(20)).value
      h.storage.putRaw(h.bucket, lockPath(h), Array.emptyByteArray)
      age(h)

      h.a.sweepOrphanedLocks()
      h.a.drainGcQueue()

      h.storage.pathExists(h.bucket, lockPath(h)).value shouldBe false
      h.metrics(0).getGcLocksDeleted shouldBe 1L
    } finally h.close()
  }

  test("a never-loaded key with a pending chain above the floor survives the sweep and its load writes one file") {
    val h = harness()
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      h.a.ensureGranularLock(tp, pk).value
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value
      // A commits records 6..30 for the key: the temp is uploaded and the chain recorded, then A
      // dies before the copy.
      val temp     = "data/topic/0/.temp-upload/uuid/final-30.json"
      val finalKey = "data/topic/0/date=2024-01-01/final-30.json"
      h.storage.writeStringToFile(h.bucket, temp, UploadableString("records 6..30")).value
      val tempTag = h.storage.getBlobAsStringAndEtag(h.bucket, temp).value._2
      val chain = PendingState(
        Offset(30),
        NonEmptyList.of(CopyOperation(h.bucket, temp, finalKey, tempTag), DeleteOperation(h.bucket, temp, tempTag)),
      )
      h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), Some(chain)).value
      h.a.updateMasterLock(tp, Offset(20)).value

      // B takes over and sweeps before any record of the key reaches it.
      h.b.open(Set(tp)).value
      age(h)
      h.b.sweepOrphanedLocks()
      h.b.drainGcQueue()
      stored(h).value.pendingState shouldBe Some(chain)

      // The replay from the floor reaches the key: B's load resolves the chain.
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(30))
      h.storage.keysUnder(h.bucket, "data/topic/0/date=2024-01-01/") shouldBe Seq(finalKey)
      h.storage.pathExists(h.bucket, temp).value shouldBe false
      stored(h).value.pendingState shouldBe None
    } finally h.close()
  }

  /**
   * A's first commit of the key, records up to `pendingOffset`: the upload lands and the lock
   * records the remaining chain from no committed offset, then the copy fails and A dies. Then the
   * master floor is at 19 and B owns the partition.
   */
  private def firstCommitDiesMidChain(h: TwoInstanceHarness, pendingOffset: Long): Unit = {
    h.clock.set(1000L)
    h.a.open(Set(tp)).value
    h.a.ensureGranularLock(tp, pk).value
    val local = java.io.File.createTempFile("staging", ".json")
    java.nio.file.Files.writeString(local.toPath, "records")
    val temp = s"data/topic/0/.temp-upload/uuid/final-$pendingOffset.json"
    val chain = PendingState(
      Offset(pendingOffset),
      NonEmptyList.of(
        UploadOperation(h.bucket, local, temp),
        CopyOperation(h.bucket, temp, s"data/topic/0/date=2024-01-01/final-$pendingOffset.json", "placeholder"),
        DeleteOperation(h.bucket, temp, "placeholder"),
      ),
    )
    h.storage.arm(FailMoveAt(h.bucket, temp))
    val _ = new PendingOperationsProcessors(h.storage)(h.taskIds.head)
      .processPendingOperations(tp, None, chain, (t, co, ps) => h.a.updateForPartitionKey(t, pk, co, ps))
      .isLeft shouldBe true
    stored(h).value.committedOffset shouldBe None
    stored(h).value.pendingState.map(_.pendingOffset) shouldBe Some(Offset(pendingOffset))
    h.a.updateMasterLock(tp, Offset(20)).value
    h.b.open(Set(tp)).value
    age(h)
  }

  test("a first-commit lock whose pending chain is below the floor is swept, at the version read") {
    val h = harness()
    try {
      firstCommitDiesMidChain(h, pendingOffset = 10)

      h.b.sweepOrphanedLocks()
      h.b.gcQueueSize shouldBe 1
      h.b.drainGcQueue()

      stored(h) shouldBe None
      h.metrics(1).getGcLocksDeleted shouldBe 1L
    } finally h.close()
  }

  test("a first-commit lock swept below the floor is not deleted once its key is reloaded and rewritten") {
    val h = harness()
    try {
      firstCommitDiesMidChain(h, pendingOffset = 10)
      h.b.sweepOrphanedLocks()
      h.b.gcQueueSize shouldBe 1
      // The key comes back to B before the drain: its load resolves (rewrites) the lock.
      h.b.getSeekedOffsetForPartitionKey(tp, pk).isRight shouldBe true
      val resolved = stored(h).value

      h.b.drainGcQueue()

      stored(h).value shouldBe resolved
      h.metrics(1).getGcLocksDeleted shouldBe 0L
    } finally h.close()
  }

  test("a first-commit lock whose pending chain is at or above the floor is retained") {
    val h = harness()
    try {
      firstCommitDiesMidChain(h, pendingOffset = 30)

      h.b.sweepOrphanedLocks()
      h.b.drainGcQueue()

      stored(h).value.pendingState.map(_.pendingOffset) shouldBe Some(Offset(30))
      h.b.gcQueueSize shouldBe 0
    } finally h.close()
  }

  // ---- budget fairness ----------------------------------------------------------------

  /** One sweep pass for A, made due again first (the marker gates a partition to one pass per interval). */
  private def sweepPass(h: TwoInstanceHarness): Int = {
    h.storage.deleteFiles(h.bucket, Seq(h.sweepMarkerPath(tp))).value
    h.a.sweepOrphanedLocks()
    h.a.drainGcQueue()
    h.metrics(0).getSweepGetBudgetUsed
  }

  private def commitIdle(h: TwoInstanceHarness, key: String, offset: Long): Unit = {
    h.a.ensureGranularLock(tp, key).value
    h.a.updateForPartitionKey(tp, key, Some(Offset(offset)), None).value
    h.a.evictGranularLock(tp, key)
  }

  test("retained locks listed first do not starve the obsolete locks listed after them") {
    val h = harness(sweepMaxReads = 3) // two lock reads a pass, one kept for the master re-check
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      val retained = Seq("a-1", "a-2", "a-3")
      val obsolete = Seq("z-1", "z-2")
      retained.foreach(commitIdle(h, _, offset = 30)) // above the floor
      obsolete.foreach(commitIdle(h, _, offset = 5)) // below it
      h.a.updateMasterLock(tp, Offset(20)).value
      (retained ++ obsolete).foreach(age(h, _))

      val budgets = (1 to 3).map(_ => sweepPass(h))

      obsolete.map(stored(h, _)) shouldBe Seq(None, None)
      retained.map(stored(h, _).map(_.committedOffset)) shouldBe retained.map(_ => Some(Some(Offset(30))))
      all(budgets) should be <= 3
    } finally h.close()
  }

  test("the sweep wraps around, so a retained lock that later becomes obsolete is still reclaimed") {
    val h = harness(sweepMaxReads = 3)
    try {
      h.clock.set(1000L)
      h.a.open(Set(tp)).value
      val keys = Seq("a-1", "a-2", "a-3", "a-4", "a-5")
      keys.foreach(commitIdle(h, _, offset = 30))
      h.a.updateMasterLock(tp, Offset(20)).value
      keys.foreach(age(h, _))
      // Two passes read the first four keys, all retained.
      val _ = (1 to 2).map(_ => sweepPass(h))
      keys.flatMap(stored(h, _)) should have size 5

      // The floor passes every key: within a bounded number of passes each is reclaimed, the ones
      // read before included.
      h.a.updateMasterLock(tp, Offset(40)).value
      val budgets = (1 to 3).map(_ => sweepPass(h))

      keys.flatMap(stored(h, _)) shouldBe empty
      all(budgets) should be <= 3
    } finally h.close()
  }
}
