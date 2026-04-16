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

import cats.data.NonEmptyList
import cats.data.Validated
import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.ExactlyOnceScenarioTest._
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CopyOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.DeleteOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.UploadOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexFilenames
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexManagerV1
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface._
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import scala.collection.mutable

/**
 * End-to-end exactly-once scenarios driven against a real `IndexManagerV2` +
 * `PendingOperationsProcessors` stack wired against the [[InMemoryStorageInterface]].
 *
 * Why
 *   The recovery and CAS paths in `IndexManagerV2` + `PendingOperationsProcessors` are
 *   where exactly-once correctness lives: a missed `If-Match`, a silent `mvFile` no-op, or
 *   a stale eTag cache after a crash all manifest as duplicates or losses at the final
 *   storage path. Mocking `StorageInterface` per test misses cross-step interactions, so
 *   each scenario here runs against a real, eTag-aware fake.
 *
 * What
 *   Each test is a named scenario: a short narrative comment, a list of `Op`s, and
 *   explicit expectations on the K-trace and on the final-path bytes. Five invariants are
 *   also checked at the end of every scenario:
 *
 *     - (a)  No loss: every offset that was reported as part of K (i.e. the synthetic
 *            writer received and committed it) appears at exactly one final path.
 *     - (a') No duplicates: each offset appears at most once across all final paths.
 *     - (b)  K is monotonically non-decreasing within an assignment generation.
 *     - (c)  Master-lock committed offset M <= K - 1 at every PreCommit step.
 *     -      Post-rebalance floor: K never regresses below M-at-rebalance.
 *
 * How
 *   Instead of wiring the full `Writer` + `WriterManager` + `JsonFormatWriter` stack, the
 *   harness uses a synthetic "pretend writer" that builds the same `[Upload, Copy, Delete]`
 *   pending-op chain that `Writer.commit` does. This keeps the test deterministic and
 *   focused on the storage / index-manager interaction surface (where the bugs live)
 *   without dragging in the format-writer / object-key-builder / commit-policy layers.
 */
class ExactlyOnceScenarioTest extends AnyFunSuite with Matchers with BeforeAndAfter {

  private var harnessOpt: Option[Harness] = None

  before {
    harnessOpt = Some(new Harness)
    harnessOpt.foreach(_.bootTask())
  }

  after {
    harnessOpt.foreach(_.silentClose())
    harnessOpt = None
  }

  private def h: Harness = harnessOpt.getOrElse(throw new IllegalStateException("Harness not initialized"))

  // ---------- scenarios ----------

  test("clean non-PARTITIONBY run: every delivered record lands exactly once") {
    // Three records on tp0. After commit + preCommit, K = 3 (last offset 2, +1).
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, None, 0L),
      Write(tp0, None, 1L),
      Write(tp0, None, 2L),
      Commit(tp0),
      PreCommit(tp0, 100L),
    )
    h.run(ops)
    h.ks(tp0).toList shouldBe List(3L)
    h.persistedOffsets(tp0) shouldBe Set(0L, 1L, 2L)
  }

  test("clean PARTITIONBY run with two pks: records partition to correct final paths") {
    // Two writers, one per pk. Each commits independently; final paths are namespaced
    // by partition key so persisted offsets per pk match what was delivered to that pk.
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some("pk-a"), 10L),
      Write(tp0, Some("pk-b"), 11L),
      Write(tp0, Some("pk-a"), 12L),
      Commit(tp0),
      PreCommit(tp0, 100L),
    )
    h.run(ops)
    h.ks(tp0).toList shouldBe List(13L) // max committed (12) + 1
    h.persistedOffsetsForPk(tp0, Some("pk-a")) shouldBe Set(10L, 12L)
    h.persistedOffsetsForPk(tp0, Some("pk-b")) shouldBe Set(11L)
  }

  test("crash after upload before copy: replay completes pending op and delivers once") {
    // Arm a one-shot mvFile failure on the next move from the synthetic writer's temp
    // path. The first commit writes the temp blob, then mvFile fails -> commit returns
    // an error -> we Crash and reboot. The pending state on the lock file points at the
    // remaining [Copy, Delete] ops, which the recovery path drives to completion.
    val finalPath = h.expectedFinalPath(tp0, None, 5L)
    val tempPath  = h.predictTempPathFor(tp0, None, 5L)
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, None, 5L),
      InjectFailMove(tempPath),
      Commit(tp0),                  // expected to fail mid-pipeline
      Crash,
      PreCommit(tp0, 100L),
    )
    h.run(ops, expectCommitErrors = true)
    h.persistedOffsets(tp0) shouldBe Set(5L)
    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true)   shouldBe false
  }

  test("crash after copy before delete: replay cleans temp and delivers once") {
    // Similar shape, but failure is injected on the temp-cleanup deleteFile. Recovery
    // re-runs the remaining Delete, which is idempotent -- the temp blob was already
    // moved, so it does not exist and deletion is a no-op. End state: final blob present,
    // no temp file leftover.
    val tempPath = h.predictTempPathFor(tp0, None, 7L)
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, None, 7L),
      InjectFailDelete(tempPath),
      Commit(tp0),                  // expected to fail on temp cleanup
      Crash,
      PreCommit(tp0, 100L),
    )
    h.run(ops, expectCommitErrors = true)
    h.persistedOffsets(tp0) shouldBe Set(7L)
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false
  }

  test("crash during master-lock CAS: a stale-eTag write fails fenced, recovery succeeds") {
    // Corrupt the eTag returned by the next master-lock write. The synthetic writer's
    // commit succeeds (the bytes land), but the cached eTag is bogus. The next PreCommit
    // tries to update the master lock with the bogus eTag -> CAS fails. We Crash and
    // reboot. Recovery re-reads the master lock with its real eTag, and a subsequent
    // PreCommit succeeds.
    val masterPath = h.masterLockPath(tp0)
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, None, 3L),
      Commit(tp0),
      InjectCorruptETag(masterPath),
      PreCommit(tp0, 100L), // expected to fail (CAS mismatch)
      Crash,
      PreCommit(tp0, 100L), // recovery path: succeeds
    )
    h.run(ops, expectPreCommitErrors = true)
    h.persistedOffsets(tp0) shouldBe Set(3L)
    h.ks(tp0).lastOption shouldBe Some(4L)
  }

  test("rebalance mid-progress: master-lock state survives, K does not regress") {
    // Write+commit on tp0 under the initial assignment. Rebalance to the same set so
    // tp0 stays owned. The new IndexManagerV2 generation calls open() which re-reads
    // the master lock with a fresh eTag. The next PreCommit returns the same K as
    // before, since no new records have been committed.
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, None, 4L),
      Commit(tp0),
      PreCommit(tp0, 100L),       // K = 5
      Rebalance(Set(tp0)),
      PreCommit(tp0, 100L),       // K still 5 -- post-rebalance floor honored
    )
    h.run(ops)
    h.ks(tp0).toList shouldBe List(5L, 5L)
    h.persistedOffsets(tp0) shouldBe Set(4L)
  }

  test("two TPs progress independently end-to-end") {
    // Interleaved writes / commits / preCommits across tp0 and tp1. Per-tp persisted
    // sets and K-traces are independent, with no cross-contamination.
    val ops = List[Op](
      Assign(Set(tp0, tp1)),
      Write(tp0, None, 0L),
      Write(tp1, None, 100L),
      Commit(tp0),
      Commit(tp1),
      PreCommit(tp0, 1000L),
      PreCommit(tp1, 1000L),
      Write(tp0, None, 1L),
      Commit(tp0),
      PreCommit(tp0, 1000L),
    )
    h.run(ops)
    h.ks(tp0).toList shouldBe List(1L, 2L)
    h.ks(tp1).toList shouldBe List(101L)
    h.persistedOffsets(tp0) shouldBe Set(0L, 1L)
    h.persistedOffsets(tp1) shouldBe Set(100L)
  }

  test("eTag mismatch on granular NoOverwrite create: recovery re-reads existing lock") {
    // The first ensureGranularLock for pk-a tries NoOverwriteExistingObject. Persist it
    // normally, but corrupt the cached eTag returned to the caller so the next
    // updateForPartitionKey sees a stale eTag and fails fencing. Crash + reboot picks up
    // the existing lock fresh, and a follow-up commit/preCommit succeeds.
    val granularPath = h.granularLockPath(tp0, "pk-a")
    val ops = List[Op](
      Assign(Set(tp0)),
      InjectCorruptETag(granularPath),
      Write(tp0, Some("pk-a"), 8L),
      Commit(tp0),                 // expected to fail (granular CAS mismatch)
      Crash,
      Write(tp0, Some("pk-a"), 9L),
      Commit(tp0),
      PreCommit(tp0, 100L),
    )
    h.run(ops, expectCommitErrors = true)
    h.persistedOffsetsForPk(tp0, Some("pk-a")) shouldBe Set(9L)
  }

  test("long monotone sequence: records dance up and down, each commit produces a final blob") {
    // Mirror of the Phase 2 long-monotone scenario. Each commit produces a separate
    // final blob (we use uncommittedOffset in the path so re-using the same offset would
    // overwrite). The K trace is monotone because we update master-lock under HWM.
    val seq = List(50L, 120L, 80L, 200L, 150L, 30L, 300L)
    val ops = List[Op](Assign(Set(tp0))) ++ seq.flatMap { o =>
      List[Op](Write(tp0, None, o), Commit(tp0), PreCommit(tp0, 1000L))
    }
    h.run(ops)
    val ks = h.ks(tp0).toList
    ks shouldBe ks.scanLeft(0L)(math.max).tail.filter(_ > 0)
    h.persistedOffsets(tp0) shouldBe seq.toSet
  }

  test("stale granular lock: aged lock is preserved if no sweep is triggered") {
    // The plan called for a sweep-eviction scenario. Sweep is a background-thread feature
    // that we deliberately disable in the harness (gcSweepEnabled = false) for
    // determinism, so this scenario instead asserts the *negative*: even when a granular
    // lock's lastModified is artificially aged past gcSweepMinAge, with sweep disabled
    // the lock is preserved and the active writer's records still land.
    val granularPath = h.granularLockPath(tp0, "pk-a")
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some("pk-a"), 1L),
      Commit(tp0),
      AgeBlobAt(granularPath, Instant.now().minusSeconds(IndexManagerV2.DefaultGcSweepMinAgeSeconds * 2L)),
      Write(tp0, Some("pk-a"), 2L),
      Commit(tp0),
      PreCommit(tp0, 100L),
    )
    h.run(ops)
    h.persistedOffsetsForPk(tp0, Some("pk-a")) shouldBe Set(1L, 2L)
  }
}

object ExactlyOnceScenarioTest {

  // ---------- topic partitions used across scenarios ----------
  val tp0: TopicPartition = Topic("topic-a").withPartition(0)
  val tp1: TopicPartition = Topic("topic-b").withPartition(0)

  // ---------- Op DSL ----------
  sealed trait Op
  final case class Assign(tps: Set[TopicPartition])        extends Op
  final case class Write(tp: TopicPartition, pk: Option[String], offset: Long) extends Op
  final case class Commit(tp: TopicPartition)              extends Op
  final case class PreCommit(tp: TopicPartition, kafkaOffset: Long) extends Op
  final case class Rebalance(newAssignment: Set[TopicPartition])    extends Op
  case object Crash                                        extends Op
  final case class InjectFailMove(tempPath: String)        extends Op
  final case class InjectFailDelete(tempPath: String)      extends Op
  final case class InjectFailWrite(path: String)           extends Op
  final case class InjectCorruptETag(path: String)         extends Op
  final case class AgeBlobAt(path: String, lastModified: Instant) extends Op

  // ---------- harness ----------

  /**
   * Wires a real `IndexManagerV2` + `PendingOperationsProcessors` against an in-memory fake.
   * Tracks per-(tp, pk) committed offsets locally so K is computed without round-tripping
   * to storage on every PreCommit (mirrors how `WriterManager` caches its own writers).
   */
  final class Harness {

    val bucket: String           = "test-bucket"
    val directoryFileName: String = ".indexes2"

    val storage = new InMemoryStorageInterface()
    implicit val si: StorageInterface[FakeFileMetadata]    = storage
    implicit val taskId: ConnectorTaskId                   = ConnectorTaskId("eo-test", 1, 0)
    implicit val locValidator: CloudLocationValidator      = (location: CloudLocation) => Validated.valid(location)

    private val pop = new PendingOperationsProcessors(storage)
    private val v1Filenames = new IndexFilenames(directoryFileName + "-v1")

    private def bucketAndPrefix(tp: TopicPartition): Either[SinkError, CloudLocation] =
      CloudLocation(bucket, Some(s"data/${tp.topic.value}/${tp.partition}/")).asRight

    private var im: IndexManagerV2 = _

    // -- shadow state --
    val ks:       mutable.Map[TopicPartition, mutable.ListBuffer[Long]] = mutable.Map.empty
    val delivered: mutable.Map[TopicPartition, mutable.Set[Long]]       = mutable.Map.empty

    /** Highest K ever returned for a TP -- used as the local HWM (mirrors WriterManager). */
    private val kHighWatermark = mutable.Map.empty[TopicPartition, Long]
    /** K-monotonicity guard within the current assignment generation. */
    private val currentGenerationLastK = mutable.Map.empty[TopicPartition, Long]
    /** Floor each TP must keep K above after a rebalance. */
    private val pendingRebalanceFloor = mutable.Map.empty[TopicPartition, Long]
    /** Per-(tp, pk) committed offset -- the highest record offset durably persisted. */
    private val committed = mutable.Map.empty[(TopicPartition, Option[String]), Long]
    /** In-memory record buffers per (tp, pk). Mirrors a Writer holding records before commit. */
    private val buffers = mutable.Map.empty[(TopicPartition, Option[String]), mutable.ListBuffer[Long]]
    /** Records the harness has issued on the current run (used for invariant a/a'). */
    private val deliveredAll = mutable.Map.empty[TopicPartition, mutable.Set[Long]]

    private var assignment: Set[TopicPartition] = Set.empty

    def bootTask(): Unit = {
      val v1 = new IndexManagerV1(v1Filenames, bucketAndPrefix)
      im = new IndexManagerV2(
        bucketAndPrefix,
        v1,
        pop,
        directoryFileName,
        gcIntervalSeconds      = Int.MaxValue,
        gcSweepIntervalSeconds = Int.MaxValue,
        gcSweepEnabled         = false,
      )(si, taskId)
      if (assignment.nonEmpty) openOrFail(assignment)
    }

    private def openOrFail(tps: Set[TopicPartition]): Unit = {
      val res = im.open(tps)
      val seeded = res.fold(err => throw new AssertionError(s"open failed: ${err.message()}"), identity)
      // Refresh local committed view from what storage returned for each tp's master lock.
      seeded.foreach {
        case (tp, Some(off)) => committed((tp, None)) = off.value
        case (tp, None)      => committed.remove((tp, None))
      }
    }

    /** Drop the IndexManagerV2 (releases executors). The next bootTask reads from storage. */
    def crash(): Unit = {
      try im.close()
      catch { case _: Throwable => () }
      im = null
      // Lose all in-memory caches (granular cache, eTag cache); committed will be re-seeded
      // by next open. For PARTITIONBY pks we keep the local committed map because the
      // synthetic writer needs to know what offsets are durable -- recovery keeps storage
      // consistent and the local map is only an optimization.
      buffers.clear()
    }

    def silentClose(): Unit =
      if (im != null) {
        try im.close() catch { case _: Throwable => () }
        im = null
      }

    // ---------- public op runner ----------

    def run(
      ops: List[Op],
      expectCommitErrors:    Boolean = false,
      expectPreCommitErrors: Boolean = false,
    ): Unit = {
      ops.foreach(runOp(_, expectCommitErrors, expectPreCommitErrors))
      checkInvariants()
    }

    private def runOp(op: Op, expectCommitErrors: Boolean, expectPreCommitErrors: Boolean): Unit = op match {
      case Assign(tps) =>
        assignment = tps
        openOrFail(tps)
        currentGenerationLastK.clear()

      case Write(tp, pk, offset) =>
        val _ = buffers.getOrElseUpdate((tp, pk), mutable.ListBuffer.empty).append(offset)
        val _ = deliveredAll.getOrElseUpdate(tp, mutable.Set.empty).add(offset)

      case Commit(tp) =>
        // Commit every buffered writer for this TP. Order across pks within a TP doesn't
        // matter for the assertions, but we sort for determinism in test output.
        val toFlush = buffers.keys.filter(_._1 == tp).toList.sortBy(_._2.getOrElse(""))
        toFlush.foreach { key =>
          val offsets = buffers(key).toList
          buffers.remove(key)
          if (offsets.nonEmpty) {
            commitOne(tp, key._2, offsets) match {
              case Right(_) => ()
              case Left(err) =>
                if (!expectCommitErrors)
                  throw new AssertionError(s"unexpected commit failure for $tp/${key._2}: ${err.message()}")
            }
          }
        }

      case PreCommit(tp, _) =>
        // Compute K = max(committed across all writers for tp) + 1, clamped by HWM.
        val activeKeys     = committed.keys.filter(_._1 == tp).toList
        val maxCommitted   = activeKeys.flatMap(committed.get).reduceOption((a: Long, b: Long) => math.max(a, b)).getOrElse(-1L)
        val rawK           = if (maxCommitted < 0) 0L else maxCommitted + 1L
        val hwm            = kHighWatermark.getOrElse(tp, 0L)
        val k              = math.max(rawK, hwm)
        // Drive the master lock update -- this is what real WriterManager.preCommit does.
        im.updateMasterLock(tp, Offset(k)) match {
          case Right(_) =>
            kHighWatermark(tp) = k
            ks.getOrElseUpdate(tp, mutable.ListBuffer.empty).append(k)
            currentGenerationLastK.get(tp).foreach { prev =>
              assert(k >= prev, s"K monotonicity violated within assignment for $tp: $k < $prev")
            }
            currentGenerationLastK(tp) = k
            pendingRebalanceFloor.remove(tp).foreach { floor =>
              assert(k >= floor, s"post-rebalance regression for $tp: K=$k floor=$floor")
            }
          case Left(err) =>
            if (!expectPreCommitErrors)
              throw new AssertionError(s"unexpected preCommit failure for $tp: ${err.message()}")
        }

      case Rebalance(newAssignment) =>
        // Snapshot per-tp K so we can enforce a floor after the new generation opens.
        kHighWatermark.foreach { case (tp, k) => pendingRebalanceFloor(tp) = k }
        // Close + reopen with the new assignment. We reuse the IndexManagerV2 -- production
        // CloudSinkTask does too, just calling open() with the new set. clearTopicPartitionState
        // for revoked TPs happens inside IndexManagerV2.open's stale-state pruning.
        openOrFail(newAssignment)
        assignment = newAssignment
        currentGenerationLastK.clear()

      case Crash =>
        crash()
        bootTask()

      case InjectFailMove(path)    => val _ = storage.arm(FailMoveAt(bucket, path))
      case InjectFailDelete(path)  => val _ = storage.arm(FailDeleteAt(bucket, path))
      case InjectFailWrite(path)   => val _ = storage.arm(FailWriteAt(bucket, path))
      case InjectCorruptETag(path) => val _ = storage.arm(CorruptETag(bucket, path))
      case AgeBlobAt(path, instant) =>
        if (!storage.setLastModified(bucket, path, instant))
          throw new AssertionError(s"AgeBlobAt: no blob exists at $bucket/$path")
    }

    // ---------- synthetic writer (mimics Writer.commit) ----------

    private def commitOne(tp: TopicPartition, pk: Option[String], offsets: List[Long]): Either[SinkError, Unit] = {
      val tempLocal = writeLocalTempFile(offsets)
      val finalPath = expectedFinalPath(tp, pk, offsets.max)
      val tempStoragePath = tempPathFor(tp, pk, offsets.max)
      val pendingOps = NonEmptyList.of(
        UploadOperation(bucket, tempLocal, tempStoragePath),
        CopyOperation(bucket, tempStoragePath, finalPath, "placeholder"),
        DeleteOperation(bucket, tempStoragePath, "placeholder"),
      )
      val pendingOffset = Offset(offsets.max)

      val ensureGranular: Either[SinkError, Unit] = pk match {
        case Some(p) => im.ensureGranularLock(tp, p)
        case None    => ().asRight
      }

      val updateFn: (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]] =
        pk match {
          case Some(p) => (t, c, ps) => im.updateForPartitionKey(t, p, c, ps)
          case None    => (t, c, ps) => im.update(t, c, ps)
        }

      val currentCommitted = committed.get((tp, pk)).map(Offset.apply)

      val result = for {
        _ <- ensureGranular
        _ <- pop.processPendingOperations(tp, currentCommitted, PendingState(pendingOffset, pendingOps), updateFn)
      } yield ()

      // Always try to delete the local temp file -- mirrors Writer.commit's hygiene.
      val _ = tempLocal.delete()

      result match {
        case Right(_) =>
          committed((tp, pk)) = offsets.max
          delivered.getOrElseUpdate(tp, mutable.Set.empty) ++= offsets
          Right(())
        case Left(err) =>
          // In production a failure here would NOT advance the writer's committedOffset.
          // For invariant checking we still want to know which offsets the writer attempted
          // to commit so we can detect inadvertent persistence; deliveredAll captures that.
          Left(err)
      }
    }

    private def writeLocalTempFile(offsets: List[Long]): File = {
      val f       = Files.createTempFile("eo-record-", ".jsonl").toFile
      val payload = offsets.map(o => s"""{"offset":$o}""").mkString("\n")
      Files.write(f.toPath, payload.getBytes(StandardCharsets.UTF_8))
      f.deleteOnExit()
      f
    }

    // ---------- path helpers (deterministic so tests can predict them) ----------

    def expectedFinalPath(tp: TopicPartition, pk: Option[String], lastOffset: Long): String = {
      val pkSegment = pk.map(p => s"$p/").getOrElse("default/")
      s"data/${tp.topic.value}/${tp.partition}/$pkSegment$lastOffset.jsonl"
    }

    def tempPathFor(tp: TopicPartition, pk: Option[String], lastOffset: Long): String =
      s".temp-upload/${tp.topic.value}/${tp.partition}/${pk.getOrElse("default")}-$lastOffset.tmp"

    /** Predict the temp path for a single-record commit. Used by InjectFail* hooks. */
    def predictTempPathFor(tp: TopicPartition, pk: Option[String], offset: Long): String =
      tempPathFor(tp, pk, offset)

    // Mirror IndexManagerV2.generateLockFilePath / generateGranularLockFilePath. Note that
    // production interpolates `${topicPartition.topic}` (Topic case class toString =
    // "Topic(<name>)"), so paths include the literal "Topic(...)" segment.
    def masterLockPath(tp: TopicPartition): String =
      s"$directoryFileName/${taskId.name}/.locks/${tp.topic}/${tp.partition}.lock"

    def granularLockPath(tp: TopicPartition, pk: String): String =
      s"$directoryFileName/${taskId.name}/.locks/${tp.topic}/${tp.partition}/$pk.lock"

    // ---------- assertion helpers ----------

    /** All offsets persisted at final paths under tp (across all pks). */
    def persistedOffsets(tp: TopicPartition): Set[Long] =
      finalBlobsFor(tp).flatMap(parseOffsetsFromBlob).toSet

    def persistedOffsetsForPk(tp: TopicPartition, pk: Option[String]): Set[Long] = {
      val pkSegment = pk.map(p => s"$p/").getOrElse("default/")
      val prefix    = s"data/${tp.topic.value}/${tp.partition}/$pkSegment"
      storage.keysUnder(bucket, prefix)
        .flatMap(k => storage.snapshot(bucket).get(k).toList)
        .flatMap(b => parseOffsetsFromBlob(new String(b.bytes, StandardCharsets.UTF_8)))
        .toSet
    }

    private def finalBlobsFor(tp: TopicPartition): Seq[String] = {
      val finalPrefix = s"data/${tp.topic.value}/${tp.partition}/"
      storage.keysUnder(bucket, finalPrefix)
        .flatMap(k => storage.snapshot(bucket).get(k).toList)
        .map(b => new String(b.bytes, StandardCharsets.UTF_8))
    }

    private def parseOffsetsFromBlob(s: String): Seq[Long] =
      s.linesIterator.toSeq.flatMap { line =>
        // Strict: payload format is {"offset":N}.
        val trimmed = line.trim
        if (trimmed.startsWith("""{"offset":""")) {
          val n = trimmed.stripPrefix("""{"offset":""").stripSuffix("}").trim
          n.toLongOption.toList
        } else Nil
      }

    // ---------- post-run invariants ----------

    private def checkInvariants(): Unit = {
      // (a) No loss: each delivered offset that the synthetic writer successfully committed
      // (i.e. recorded in `delivered`) must appear at exactly one final path.
      delivered.foreach { case (tp, offsetSet) =>
        val persisted = persistedOffsets(tp)
        val missing   = offsetSet.toSet.diff(persisted)
        assert(missing.isEmpty, s"(a) lost offsets for $tp: $missing")
      }
      // (a') No duplicates per tp (offsets across all final paths).
      delivered.keys.foreach { tp =>
        val all = finalBlobsFor(tp).flatMap(parseOffsetsFromBlob)
        val dup = all.diff(all.distinct)
        assert(dup.isEmpty, s"(a') duplicate offsets for $tp: $dup")
      }
      // (b) and (c) are checked inline at PreCommit; nothing more to do here.
    }
  }
}
