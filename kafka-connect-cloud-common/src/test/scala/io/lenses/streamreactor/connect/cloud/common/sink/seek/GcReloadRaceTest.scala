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
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.OnDeleteAt
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.UUID
import scala.collection.mutable

/**
 * The GC drain and the `put()` thread reloading a key it has queued for deletion. The drain
 * re-checks the granular cache before each delete call (one key per call on a store that deletes
 * one object per request), so a key reloaded before the call covering it is sent keeps its lock.
 * A reload that lands during that one call can still lose the lock; the key's next commit then fails before anything reaches a final path, and the restarted
 * task replays from the master floor with no loss and no duplicate.
 */
class GcReloadRaceTest extends AnyFunSuite with Matchers with EitherValues with OptionValues {

  private val tp = Topic("topic").withPartition(0)

  test(
    "on a store that deletes many objects per request, a key reloaded while an earlier GC chunk is being deleted keeps its lock and commits",
  ) {
    val h = new TwoInstanceHarness(gcBatchSize = 1, batchedConditionalDeletes = true)
    try {
      val first  = "date=2024-01-01"
      val second = "date=2024-01-02"
      h.a.open(Set(tp)).value
      // Queued one after the other, so each sits in its own chunk, in this order.
      h.a.ensureGranularLock(tp, first).value
      h.a.updateForPartitionKey(tp, first, Some(Offset(5)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value
      h.a.ensureGranularLock(tp, second).value
      h.a.updateForPartitionKey(tp, second, Some(Offset(12)), None).value
      h.a.updateMasterLock(tp, Offset(20)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(20), Set.empty).value
      h.a.gcQueueSize shouldBe 2

      // A record for the second key arrives while the first chunk's delete is in flight: the put()
      // thread reloads the key, at the same version the GC item was selected at.
      h.storage.arm(
        OnDeleteAt(
          h.bucket,
          h.granularLockPath(tp, first),
          () => {
            h.a.getSeekedOffsetForPartitionKey(tp, second).value shouldBe Some(Offset(12))
            ()
          },
        ),
      )

      h.a.drainGcQueue()

      h.storage.pathExists(h.bucket, h.granularLockPath(tp, first)).value shouldBe false
      h.readLock(h.granularLockPath(tp, second)).value.wrappedObject.committedOffset shouldBe Some(Offset(12))
      h.a.updateForPartitionKey(tp, second, Some(Offset(25)), None).value shouldBe Some(Offset(25))
      h.readLock(h.granularLockPath(tp, second)).value.wrappedObject.committedOffset shouldBe Some(Offset(25))
      h.a.gcQueueSize shouldBe 0
      h.metrics(0).getGcLocksDeleted shouldBe 1L
      h.metrics(0).getGcLocksSkippedReclaimed shouldBe 1L
      h.storage.deleteFilesIfMatchCalls shouldBe 1
    } finally h.close()
  }

  test(
    "on a store that deletes one object per request, a key reloaded while an earlier key of the same batch is deleted keeps its lock and commits",
  ) {
    // Default batch size: both keys fit in one batch of the drain.
    val h = new TwoInstanceHarness()
    try {
      val first  = "date=2024-01-01"
      val second = "date=2024-01-02"
      h.a.open(Set(tp)).value
      // Queued one after the other, so the drain takes them in this order.
      h.a.ensureGranularLock(tp, first).value
      h.a.updateForPartitionKey(tp, first, Some(Offset(5)), None).value
      h.a.updateMasterLock(tp, Offset(10)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(10), Set.empty).value
      h.a.ensureGranularLock(tp, second).value
      h.a.updateForPartitionKey(tp, second, Some(Offset(12)), None).value
      h.a.updateMasterLock(tp, Offset(20)).value
      h.a.cleanUpObsoleteLocks(tp, Offset(20), Set.empty).value
      h.a.gcQueueSize shouldBe 2

      // A record for the second key arrives while the first key's delete is in flight.
      h.storage.arm(
        OnDeleteAt(
          h.bucket,
          h.granularLockPath(tp, first),
          () => {
            h.a.getSeekedOffsetForPartitionKey(tp, second).value shouldBe Some(Offset(12))
            ()
          },
        ),
      )

      h.a.drainGcQueue()

      h.storage.pathExists(h.bucket, h.granularLockPath(tp, first)).value shouldBe false
      h.readLock(h.granularLockPath(tp, second)).value.wrappedObject.committedOffset shouldBe Some(Offset(12))
      h.a.updateForPartitionKey(tp, second, Some(Offset(25)), None).value shouldBe Some(Offset(25))
      h.readLock(h.granularLockPath(tp, second)).value.wrappedObject.committedOffset shouldBe Some(Offset(25))
      h.a.gcQueueSize shouldBe 0
      h.metrics(0).getGcLocksDeleted shouldBe 1L
      h.metrics(0).getGcLocksSkippedReclaimed shouldBe 1L
      h.storage.deleteFilesIfMatchCalls shouldBe 1
    } finally h.close()
  }

  test(
    "a key reloaded during its own GC delete loses its lock, its next commit fails before a final write, and the restarted task replays every record exactly once",
  ) {
    val h = new TwoInstanceHarness()
    try {
      val gcd   = "date=2024-01-01" // committed below the master floor, then queued for GC
      val other = "date=2024-01-02" // committed above the floor before the crash
      val first = new Task(h, instance = 0)
      first.open() shouldBe None

      (0L to 5L).foreach(o => first.deliver(o, gcd))
      first.commit(gcd).value
      (6L to 19L).foreach(o => first.deliver(o, other))
      first.commit(other).value
      first.preCommit() shouldBe 20L // master floor 19; `gcd` (at 5) has no writer and is queued
      h.a.gcQueueSize shouldBe 1
      first.deliver(20L, other)
      first.commit(other).value

      // Record 21 routes to the queued key while its own GC delete is in flight: the writer is
      // created (lock reloaded at the version GC selected) just before the delete lands.
      val gcdLock = h.granularLockPath(tp, gcd)
      h.storage.arm(OnDeleteAt(h.bucket, gcdLock, () => first.deliver(21L, gcd)))
      h.a.drainGcQueue()
      first.buffered(gcd) shouldBe Seq(21L)
      h.storage.pathExists(h.bucket, gcdLock).value shouldBe false
      h.metrics(0).getGcLocksDeleted shouldBe 1L

      // The next commit uploads its temp, then fails on the lock write: nothing reaches a final path.
      val finalsBefore = h.finalObjects
      val failed       = first.commit(gcd).left.value
      failed shouldBe a[FatalCloudSinkError]
      h.finalObjects shouldBe finalsBefore
      h.storage.pathExists(h.bucket, gcdLock).value shouldBe false

      // The task fails and Connect restarts it: a new instance opens the partition and Kafka
      // redelivers from the master floor, which open() seeks the consumer to.
      h.a.close()
      val restarted = new Task(h, instance = 1)
      val floor     = restarted.open().value
      floor shouldBe Offset(19)
      val redelivered = Seq(19L -> other, 20L -> other, 21L -> gcd)
      redelivered.foreach { case (o, pk) => restarted.deliver(o, pk) }
      restarted.buffered(other) shouldBe empty
      restarted.buffered(gcd) shouldBe Seq(21L)
      restarted.commit(gcd).value
      restarted.preCommit() shouldBe 22L

      val written = h.finalObjects.values.flatten.toSeq
      written.sorted shouldBe (0L to 21L)
      written.distinct.size shouldBe written.size
      h.finalObjects.keys.filter(_.contains(s"/${first.attempt}/")).flatMap(h.finalObjects).toSeq.sorted shouldBe
        (0L to 20L)
      h.finalObjects.keys.filter(_.contains(s"/${restarted.attempt}/")).flatMap(h.finalObjects).toSeq shouldBe
        Seq(21L)
    } finally h.close()
  }

  implicit private class FinalObjects(h: TwoInstanceHarness) {

    /** Every object under the final-path prefix, with the record offsets it holds. */
    def finalObjects: Map[String, Seq[Long]] =
      h.storage.keysUnder(h.bucket, "data/").map { key =>
        val bytes = h.storage.snapshot(h.bucket)(key).bytes
        key -> new String(bytes, StandardCharsets.UTF_8).linesIterator.map(_.toLong).toSeq
      }.toMap
  }

  /**
   * One task instance driving one `IndexManagerV2` the way `WriterManager` and `Writer` do: a writer
   * is created on the first record for a key (`ensureGranularLock`, then a skip floor of the max of
   * the key's lock and the master floor), a record at or below the floor is skipped, a commit runs
   * the `[Upload temp, Copy temp -> final, Delete temp]` chain, and an idle writer is closed after
   * its commit, so its key is eligible for GC at the next pre-commit.
   */
  final private class Task(h: TwoInstanceHarness, instance: Int) {
    val attempt: String = s"task-$instance"

    private val im:              IndexManagerV2  = h.instances(instance)
    implicit private val taskId: ConnectorTaskId = h.taskIds(instance)
    private val pop = new PendingOperationsProcessors(h.storage)

    private val floors    = mutable.Map.empty[String, Option[Offset]]
    private val buffers   = mutable.Map.empty[String, mutable.ListBuffer[Long]]
    private val committed = mutable.Map.empty[String, Long]

    def open(): Option[Offset] = im.open(Set(tp)).value(tp)

    def buffered(pk: String): Seq[Long] = buffers.get(pk).map(_.toList).getOrElse(Nil)

    def deliver(offset: Long, pk: String): Unit = {
      val floor = floors.getOrElseUpdate(pk, createWriter(pk))
      if (!floor.exists(_.value >= offset)) { val _ = buffers.getOrElseUpdate(pk, mutable.ListBuffer.empty) += offset }
    }

    private def createWriter(pk: String): Option[Offset] = {
      im.ensureGranularLock(tp, pk).value
      val granular = im.getSeekedOffsetForPartitionKey(tp, pk).value
      (granular.toList ++ im.getSeekedOffsetForTopicPartition(tp).toList).maxByOption(_.value)
    }

    def commit(pk: String): Either[SinkError, Unit] = {
      val offsets = buffered(pk)
      val local   = Files.createTempFile("gc-reload-race-", ".jsonl").toFile
      Files.write(local.toPath, offsets.mkString("\n").getBytes(StandardCharsets.UTF_8))
      val temp      = s".temp-upload/${tp.topic.value}/${tp.partition}/${UUID.randomUUID()}"
      val finalPath = s"data/$pk/$attempt/${offsets.max}.jsonl"
      val ops = NonEmptyList.of[FileOperation](
        UploadOperation(h.bucket, local, temp),
        CopyOperation(h.bucket, temp, finalPath, "placeholder"),
        DeleteOperation(h.bucket, temp, "placeholder"),
      )
      val result = pop.processPendingOperations(
        tp,
        committed.get(pk).map(Offset(_)),
        PendingState(Offset(offsets.max), ops),
        (t, c, ps) => im.updateForPartitionKey(t, pk, c, ps),
      )
      val _ = local.delete()
      result.map { _ =>
        committed(pk) = offsets.max
        buffers.remove(pk)
        // The writer is idle once committed: closing it leaves the key to GC.
        floors.remove(pk)
        ()
      }
    }

    /** The two-arm safe offset of `WriterManager.preCommit`, written to the master lock, then GC enqueue. */
    def preCommit(): Long = {
      val firstBuffered = buffers.values.filter(_.nonEmpty).map(_.head)
      val k =
        if (firstBuffered.nonEmpty) firstBuffered.min
        else committed.values.maxOption.map(_ + 1).getOrElse(0L)
      im.updateMasterLock(tp, Offset(k)).value
      im.cleanUpObsoleteLocks(tp, Offset(k), floors.keySet.toSet).value
      k
    }
  }
}
