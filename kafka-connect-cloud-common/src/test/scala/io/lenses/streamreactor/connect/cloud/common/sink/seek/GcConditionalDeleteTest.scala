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

import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailDeleteAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.OnDeleteAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.OnReadAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.OnWriteAt
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Granular-lock GC deletes are conditional on the version GC selected. A lock rewritten after it
 * was queued (taken over and committed by a newer owner, or reclaimed and committed by this
 * instance) survives the delete, so its key keeps its dedup floor.
 */
class GcConditionalDeleteTest extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private val tp = Topic("topic").withPartition(0)
  private val pk = "date=2024-01-01"

  private def lockPath(h: TwoInstanceHarness): String = h.granularLockPath(tp, pk)

  private def stored(h: TwoInstanceHarness): Option[IndexFile] = h.readLock(lockPath(h)).toOption.map(_.wrappedObject)

  /**
   * A (epoch 1000) commits `pk` at 5, advances the master floor to 19 and queues `pk` for GC
   * (5 < 19): the state of a task that queued an obsolete lock just before a rebalance.
   */
  private def aQueuesTheKey(h: TwoInstanceHarness): Unit = {
    h.clock.set(1000L)
    h.a.open(Set(tp)).value
    h.a.ensureGranularLock(tp, pk).value
    h.a.updateForPartitionKey(tp, pk, Some(Offset(5)), None).value
    h.a.updateMasterLock(tp, Offset(20)).value
    h.a.cleanUpObsoleteLocks(tp, Offset(20), Set.empty).value
    val _ = h.a.gcQueueSize shouldBe 1
  }

  private def bOpens(h: TwoInstanceHarness): Unit = {
    val _ = h.b.open(Set(tp)).value
  }

  private def bOpensOnTheNextMasterRead(h: TwoInstanceHarness): Unit = {
    val _ = h.storage.arm(OnReadAt(h.bucket, h.masterLockPath(tp), () => bOpens(h)))
  }

  /** B opens (epoch 1001), loads `pk` (taking it over) and commits it at 25, past the master floor. */
  private def bTakesOverAndCommits(h: TwoInstanceHarness): Unit = {
    h.b.open(Set(tp)).value
    h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
    h.b.updateForPartitionKey(tp, pk, Some(Offset(25)), None).value
    ()
  }

  test("a zombie's queued GC delete does not remove a lock the new owner took over and committed after it was queued") {
    val h = new TwoInstanceHarness()
    try {
      aQueuesTheKey(h)
      // B takes the partition over while A's drain is between its filters and its delete.
      h.storage.arm(OnDeleteAt(h.bucket, lockPath(h), () => bTakesOverAndCommits(h)))

      h.a.drainGcQueue()

      stored(h).value shouldBe IndexFile(h.lockUuid(1), Some(Offset(25)), None, Some(1001L))
      // B's next commit on the key is not broken by A's GC.
      h.b.updateForPartitionKey(tp, pk, Some(Offset(30)), None).value shouldBe Some(Offset(30))
      // Dropped, not retried: the lock is someone else's now.
      h.a.gcQueueSize shouldBe 0
      h.metrics(0).getGcLocksSkippedChanged shouldBe 1L
      h.metrics(0).getGcLocksDeleted shouldBe 0L
      h.metrics(0).getGcDeleteFailures shouldBe 0L
    } finally h.close()
  }

  test("a zombie's queued GC delete drained after the new owner committed the key leaves the lock in place") {
    val h = new TwoInstanceHarness()
    try {
      aQueuesTheKey(h)
      bTakesOverAndCommits(h)

      h.a.drainGcQueue()

      stored(h).value.committedOffset shouldBe Some(Offset(25))
      h.b.updateForPartitionKey(tp, pk, Some(Offset(30)), None).value shouldBe Some(Offset(30))
    } finally h.close()
  }

  test("the final drain at stop() does not delete a key this instance reclaimed and committed after queueing it") {
    val h = new TwoInstanceHarness()
    try {
      aQueuesTheKey(h)
      // A routes a new record to the key: it reloads the lock (its own epoch, no take-over write)
      // and commits it past the master floor.
      h.a.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe Some(Offset(5))
      h.a.updateForPartitionKey(tp, pk, Some(Offset(25)), None).value
      // stop() without close(partitions): the writers' close evicts the granular cache, so the
      // reclaim filter of the final drain sees nothing.
      h.a.evictAllGranularLocks(tp)

      h.a.close()

      stored(h).value shouldBe IndexFile(h.lockUuid(0), Some(Offset(25)), None, Some(1000L))
    } finally h.close()
  }

  test("the final drain at stop() does not delete a key a new owner took over after it was queued") {
    val h = new TwoInstanceHarness()
    try {
      aQueuesTheKey(h)
      bTakesOverAndCommits(h)
      h.a.evictAllGranularLocks(tp)

      h.a.close()

      stored(h).value shouldBe IndexFile(h.lockUuid(1), Some(Offset(25)), None, Some(1001L))
    } finally h.close()
  }

  test("a queued lock nobody touched since is deleted, and the new owner then dedups the key from the master floor") {
    val h = new TwoInstanceHarness()
    try {
      aQueuesTheKey(h)
      h.b.open(Set(tp)).value // never loads the key

      h.a.drainGcQueue()

      stored(h) shouldBe None
      h.a.gcQueueSize shouldBe 0
      h.metrics(0).getGcLocksDeleted shouldBe 1L
      h.metrics(0).getGcLocksSkippedChanged shouldBe 0L
      // The key's last committed offset (5) is below the floor B inherits, so nothing B replays
      // from the floor was written for the key after it.
      h.b.getSeekedOffsetForPartitionKey(tp, pk).value shouldBe None
      h.b.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(19))
    } finally h.close()
  }

  test("the final drain at stop() still deletes a queued lock that nothing rewrote") {
    val h = new TwoInstanceHarness()
    try {
      aQueuesTheKey(h)
      h.a.evictAllGranularLocks(tp)

      h.a.close()

      stored(h) shouldBe None
    } finally h.close()
  }

  test("a failed conditional delete is retried with the same token, and still refuses a lock rewritten meanwhile") {
    val h = new TwoInstanceHarness()
    try {
      aQueuesTheKey(h)
      h.storage.arm(FailDeleteAt(h.bucket, lockPath(h)))

      h.a.drainGcQueue()
      stored(h).value.committedOffset shouldBe Some(Offset(5))
      h.a.gcQueueSize shouldBe 1
      h.metrics(0).getGcDeleteFailures shouldBe 1L
      h.metrics(0).getGcDeleteRetries shouldBe 1L

      // B takes the key over before the retry: the retried delete still carries A's token.
      bTakesOverAndCommits(h)
      h.a.drainGcQueue()

      stored(h).value.committedOffset shouldBe Some(Offset(25))
      h.a.gcQueueSize shouldBe 0
      h.metrics(0).getGcLocksSkippedChanged shouldBe 1L
    } finally h.close()
  }

  test("a queued lock already gone when the drain runs counts as deleted") {
    val h = new TwoInstanceHarness()
    try {
      aQueuesTheKey(h)
      h.storage.deleteFiles(h.bucket, Seq(lockPath(h))).value

      h.a.drainGcQueue()

      h.a.gcQueueSize shouldBe 0
      h.metrics(0).getGcLocksDeleted shouldBe 1L
      h.metrics(0).getGcDeleteFailures shouldBe 0L
    } finally h.close()
  }

  /** Three idle keys committed at 5 and queued for GC under a floor of 20; the second is then rewritten. */
  private def threeQueuedKeysOneRewritten(h: TwoInstanceHarness): Seq[String] = {
    h.clock.set(1000L)
    h.a.open(Set(tp)).value
    val keys = (1 to 3).map(n => s"date=2024-01-0$n")
    keys.foreach { k =>
      h.a.ensureGranularLock(tp, k).value
      h.a.updateForPartitionKey(tp, k, Some(Offset(5)), None).value
    }
    h.a.updateMasterLock(tp, Offset(20)).value
    h.a.cleanUpObsoleteLocks(tp, Offset(20), Set.empty).value
    h.a.gcQueueSize shouldBe 3
    // One of them is rewritten (by anyone) after it was queued.
    h.storage.putRaw(h.bucket, h.granularLockPath(tp, keys(1)), "rewritten".getBytes)
    keys
  }

  test(
    "on a store that deletes many objects per request, GC deletes of many keys go out as one conditional request per batch, each key with its own token",
  ) {
    val h = new TwoInstanceHarness(batchedConditionalDeletes = true)
    try {
      val keys   = threeQueuedKeysOneRewritten(h)
      val before = h.storage.deleteFilesIfMatchCalls

      h.a.drainGcQueue()

      h.storage.deleteFilesIfMatchCalls - before shouldBe 1
      keys.map(k => h.storage.pathExists(h.bucket, h.granularLockPath(tp, k)).value) shouldBe Seq(false, true, false)
      h.metrics(0).getGcLocksDeleted shouldBe 2L
      h.metrics(0).getGcLocksSkippedChanged shouldBe 1L
      h.a.gcQueueSize shouldBe 0
    } finally h.close()
  }

  test("on a store that deletes one object per request, GC sends one conditional request per key") {
    val h = new TwoInstanceHarness()
    try {
      val keys   = threeQueuedKeysOneRewritten(h)
      val before = h.storage.deleteFilesIfMatchCalls

      h.a.drainGcQueue()

      h.storage.deleteFilesIfMatchCalls - before shouldBe 3
      keys.map(k => h.storage.pathExists(h.bucket, h.granularLockPath(tp, k)).value) shouldBe Seq(false, true, false)
      h.metrics(0).getGcLocksDeleted shouldBe 2L
      h.metrics(0).getGcLocksSkippedChanged shouldBe 1L
      h.a.gcQueueSize shouldBe 0
    } finally h.close()
  }

  // ---- legacy purge (batch mode) ------------------------------------------------------

  /**
   * X1 (instance 0) runs in batch mode, B (instance 1) in granular mode. A lock left by an older
   * granular deployment sits at `pk` with offset 5.
   */
  private def batchThenGranular(): TwoInstanceHarness = {
    val h = new TwoInstanceHarness(commitModes = Seq(CommitMode.Batch, CommitMode.Granular))
    h.clock.set(1000L)
    h.storage.writeBlobToFile(
      h.bucket,
      lockPath(h),
      NoOverwriteExistingObject(IndexFile("old-granular", Some(Offset(5)), None)),
    ).value
    h
  }

  test("a batch instance's legacy purge does not delete a lock a later granular owner took over after the fencing") {
    val h = batchThenGranular()
    try {
      // X1 has fenced the lock and pauses right before its purge deletes it; B opens in granular
      // mode, takes the key over and commits it.
      h.storage.arm(OnDeleteAt(h.bucket, lockPath(h), () => bTakesOverAndCommits(h)))

      h.a.open(Set(tp)).value

      stored(h).value shouldBe IndexFile(h.lockUuid(1), Some(Offset(25)), None, Some(1001L))
      h.b.updateForPartitionKey(tp, pk, Some(Offset(30)), None).value shouldBe Some(Offset(30))
      // Refused on version: the lock is B's now, so X1 counts the key as resolved.
      h.a.legacyPurged(tp) shouldBe true
    } finally h.close()
  }

  test("a batch instance's legacy purge deletes nothing once a later owner has opened the partition") {
    val h = batchThenGranular()
    try {
      // B opens after X1 has fenced the lock, before X1's purge re-reads the master.
      h.storage.arm(OnWriteAt(h.bucket, lockPath(h), () => bOpensOnTheNextMasterRead(h)))

      h.a.open(Set(tp)).value

      stored(h).value.committedOffset shouldBe Some(Offset(5))
      h.storage.pathExists(h.bucket, lockPath(h)).value shouldBe true
      h.a.legacyPurged(tp) shouldBe false
      h.a.fencedPartitions should contain(tp)
    } finally h.close()
  }

  test("a batch instance's legacy purge deletes a fenced lock nobody touched since, and the sweep marker") {
    val h = batchThenGranular()
    try {
      h.storage.putRaw(h.bucket, h.sweepMarkerPath(tp), "{}".getBytes)

      h.a.open(Set(tp)).value

      h.storage.pathExists(h.bucket, lockPath(h)).value shouldBe false
      h.storage.pathExists(h.bucket, h.sweepMarkerPath(tp)).value shouldBe false
      h.a.legacyPurged(tp) shouldBe true
    } finally h.close()
  }

  test("a failed legacy lock delete leaves the partition unresolved, and the next open purges it") {
    val h = batchThenGranular()
    try {
      h.storage.arm(FailDeleteAt(h.bucket, lockPath(h)))

      h.a.open(Set(tp)).value
      h.a.legacyPurged(tp) shouldBe false
      h.storage.pathExists(h.bucket, lockPath(h)).value shouldBe true

      h.a.clearTopicPartitionState(tp)
      h.a.open(Set(tp)).value
      h.a.legacyPurged(tp) shouldBe true
      h.storage.pathExists(h.bucket, lockPath(h)).value shouldBe false
    } finally h.close()
  }
}
