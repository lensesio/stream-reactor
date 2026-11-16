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
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexFile
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.UploadOperation
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
 * Terminology used throughout the scenarios and the assertions below:
 *
 *   - M  ("committed offset"): the highest record offset whose bytes are durably persisted
 *        at a final storage path. Stored on the master lock file per topic-partition.
 *   - K  ("safe offset"): the offset reported to Kafka at `preCommit` time -- the offset of
 *        the next record Kafka should deliver. Two-arm formula (mirrors production
 *        `WriterManager.getOffsetAndMeta`):
 *          - If any writer has buffered (uncommitted) data: K = min(firstBufferedOffset)
 *          - Otherwise: K = max(committedOffset) + 1
 *        Both arms are modelled in this harness. K must be monotonically non-decreasing
 *        while this task continues to own the topic-partition.
 *   - partition key (often shortened to `pk`): the PARTITIONBY grouping within a
 *        topic-partition. In PARTITIONBY mode there is one writer per distinct key, each
 *        writing to its own namespaced final-path prefix (e.g. `.../pk-a/`, `.../pk-b/`);
 *        in non-PARTITIONBY mode the partition key is `None` and a single writer owns the
 *        topic-partition.
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
 *     - (a)  No loss: every offset the synthetic writer successfully committed appears at
 *            exactly one final path.
 *     - (a') No duplicates: each offset appears at most once across all final paths.
 *     - (b)  K is monotonically non-decreasing while this task continues to own the
 *            topic-partition (i.e. within a single assignment generation -- a rebalance
 *            starts a new generation and introduces a separate floor, captured below).
 *     - (c)  `M <= K - 1` at every `PreCommit`: the reported safe offset never runs ahead
 *            of what is actually persisted.
 *     -      Post-rebalance floor: after a rebalance, K never regresses below whatever K
 *            was immediately before the rebalance -- the new generation picks up from the
 *            persisted state, it does not roll it back.
 *
 * How
 *   Instead of wiring the full `Writer` + `WriterManager` + `JsonFormatWriter` stack, the
 *   harness uses a synthetic "pretend writer" that builds the same `[Upload, Copy, Delete]`
 *   pending-op chain that `Writer.commit` does. This keeps the test deterministic and
 *   focused on the storage / index-manager interaction surface (where the bugs live)
 *   without dragging in the format-writer / object-key-builder / commit-policy layers.
 *
 * Op DSL semantics
 *   - `Write(tp, pk, off)`:       append `off` to the in-memory buffer for `(tp, pk)`.
 *                                 Fails fast if `(tp, pk)` is already in `Uploading` state
 *                                 -- use `RecommitPending(tp)` first to clear it.
 *   - `Commit(tp)`:               flush ALL buffered `(tp, pk)` pairs. On failure the
 *                                 offsets move to `uploading` (mirroring `Writer.Uploading`).
 *   - `CommitOnly(tp, pk)`:       flush a single buffered `(tp, pk)`, leaving siblings
 *                                 buffered. Models a selective-commit cycle. Same
 *                                 failure semantics as `Commit`.
 *   - `RecommitPending(tp)`:      re-runs `commitOne` for every `(tp, pk)` currently in
 *                                 the `uploading` map. Models `WriterManager.recommitPending`
 *                                 retrying the stored pending-op chain without re-buffering.
 *   - `Crash`:                    drops the `IndexManagerV2` instance, clears `buffers`
 *                                 and `uploading` (in-memory `Uploading` state is lost on
 *                                 JVM crash; only the granular/master lock's `PendingState`
 *                                 survives in storage).
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
      Commit(tp0), // expected to fail mid-pipeline
      Crash,
      PreCommit(tp0, 100L),
    )
    h.run(ops, expectCommitErrors = true)
    h.persistedOffsets(tp0) shouldBe Set(5L)
    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false
    // K = max(committed)+1 = 5+1 = 6; buffered and uploading are both empty after Crash.
    h.ks(tp0).toList shouldBe List(6L)
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
      Commit(tp0), // expected to fail on temp cleanup
      Crash,
      PreCommit(tp0, 100L),
    )
    h.run(ops, expectCommitErrors = true)
    h.persistedOffsets(tp0) shouldBe Set(7L)
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false
    // K = max(committed)+1 = 7+1 = 8; recovery seeded committed[(tp0,None)]=7 via open().
    h.ks(tp0).toList shouldBe List(8L)
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
      PreCommit(tp0, 100L), // K = 5
      Rebalance(Set(tp0)),
      PreCommit(tp0, 100L), // K still 5 -- post-rebalance floor honored
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
      Commit(tp0), // expected to fail (granular CAS mismatch)
      Crash,
      Write(tp0, Some("pk-a"), 9L),
      Commit(tp0),
      PreCommit(tp0, 100L),
    )
    h.run(ops, expectCommitErrors = true)
    h.persistedOffsetsForPk(tp0, Some("pk-a")) shouldBe Set(9L)
    // K = max(committed)+1 = 9+1 = 10; uploading cleared by Crash before the follow-up commit.
    h.ks(tp0).lastOption shouldBe Some(10L)
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

  // (a) Copy/Delete pending recovery via getSeekedOffsetForPartitionKey (loadGranularLock)
  test(
    "PARTITIONBY: Copy/Delete pending recovery resolves via getSeekedOffsetForPartitionKey (loadGranularLock path)",
  ) {
    // Crash after Upload (temp blob written, Copy fails). On reboot, LoadGranularLock
    // triggers getSeekedOffsetForPartitionKey -> loadGranularLock -> resolveAndCacheGranularLock,
    // which replays [Copy, Delete] from the still-present temp blob. Final blob arrives
    // at the expected path; no orphaned temp remains.
    val pk        = "pk-load"
    val tempPath  = h.predictTempPathFor(tp0, Some(pk), 20L)
    val finalPath = h.expectedFinalPath(tp0, Some(pk), 20L)
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some(pk), 20L),
      InjectFailMove(tempPath), // Copy will fail; lock records PendingState=[Copy, Delete]
      Commit(tp0),              // expected to fail
      Crash,
      LoadGranularLock(tp0, pk), // triggers getSeekedOffsetForPartitionKey -> loadGranularLock;
      // seeds committed[(tp0, Some(pk))] = 20
      PreCommit(tp0, 100L), // K = max(committed)+1 = 20+1 = 21
    )
    h.run(ops, expectCommitErrors = true)
    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false
    h.persistedOffsetsForPk(tp0, Some(pk)) shouldBe Set(20L)
    h.ks(tp0).toList shouldBe List(21L)
  }

  // (b) Copy/Delete pending recovery via ensureGranularLock (resolveAndCacheGranularLock path)
  test("PARTITIONBY: Copy/Delete pending recovery resolves via ensureGranularLock (resolveAndCacheGranularLock path)") {
    // Same crash-after-Upload scenario as (a), but recovery is triggered by a subsequent
    // commitOne call that exercises ensureGranularLock -> resolveAndCacheGranularLock,
    // completing [Copy, Delete] for the old commit before processing the new one.
    val pk       = "pk-ensure"
    val tempPath = h.predictTempPathFor(tp0, Some(pk), 21L)
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some(pk), 21L),
      InjectFailMove(tempPath), // Copy will fail
      Commit(tp0),              // fails mid-chain; temp blob still present in storage
      Crash,
      Write(tp0, Some(pk), 22L), // new record; ensureGranularLock detects and resolves PendingState
      Commit(tp0),               // recovery + new commit both succeed
      PreCommit(tp0, 100L),
    )
    h.run(ops, expectCommitErrors = true)
    h.persistedOffsetsForPk(tp0, Some(pk)) shouldBe Set(21L, 22L)
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false
    // K = max(committed)+1 = 22+1 = 23; recovery seeded committed=22 via ensureGranularLock.
    h.ks(tp0).lastOption shouldBe Some(23L)
  }

  // (c) crash-after-Copy-before-lock-update idempotence (mvFile source absent + dest present)
  test("PARTITIONBY: crash-after-Copy idempotence: recovery does not escalate when source absent and dest present") {
    // Simulate the scenario where Copy already ran (final blob present, temp blob absent)
    // but the lock still records PendingState=[Copy, Delete] because the lock write after
    // Copy failed (CAS mismatch via CorruptETag). Recovery must NOT raise FatalCloudSinkError:
    // it uses InMemoryStorageInterface.mvFile's idempotence (source absent + dest present -> Right(())).
    val pk        = "pk-idempotent"
    val tempPath  = h.predictTempPathFor(tp0, Some(pk), 5L)
    val finalPath = h.expectedFinalPath(tp0, Some(pk), 5L)

    // Normal commit creates final blob and clears the granular lock.
    val setupOps = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some(pk), 5L),
      Commit(tp0),
    )
    h.run(setupOps)
    // Sanity: final blob exists, temp gone, lock committed.
    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false

    // Manipulate the granular lock to simulate crash-after-Copy:
    // final blob stays present, but lock is reset to PendingState=[Copy, Delete].
    val manipulateOps = List[Op](
      ManipulateGranularLock(
        tp0,
        pk,
        Some(
          PendingState(
            Offset(5L),
            NonEmptyList.of(
              CopyOperation(h.bucket, tempPath, finalPath, "placeholder"),
              DeleteOperation(h.bucket, tempPath, "placeholder"),
            ),
          ),
        ),
      ),
      Crash,                    // clear IndexManagerV2 cache so recovery re-reads storage
      LoadGranularLock(tp0, pk),// triggers loadGranularLock -> idempotent Copy + no-op Delete
    )
    h.run(manipulateOps)
    // Final blob still present (idempotent Copy did not destroy it), temp still absent.
    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false
    h.persistedOffsetsForPk(tp0, Some(pk)) shouldBe Set(5L)
  }

  // ── LC-2451 retry-replay completes [Copy, Delete] PendingState without crash ─────────────────
  test(
    "(LC-2451): retry on same task instance completes [Copy, Delete] PendingState after cleanUp; no duplication; preCommit advances",
  ) {
    // PR #307 invariant: WriterManager.cleanUp(tp) evicts the in-memory granular cache but
    // preserves seekedOffsets + topicPartitionToETags. The next put() call thus triggers
    // ensureGranularLock which re-reads the storage-side granular lock, detects PendingState,
    // and replays [Copy, Delete] before processing the new batch.

    val pk        = "pk-lc2451"
    val tempPath  = h.predictTempPathFor(tp0, Some(pk), 30L)
    val finalPath = h.expectedFinalPath(tp0, Some(pk), 30L)

    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some(pk), 30L),
      InjectFailMove(tempPath), // arm one-shot Copy failure
      Commit(tp0),              // Upload succeeds; Copy fails → PendingState=[Copy,Delete] recorded
      // offsets moved to `uploading[(tp0, Some(pk))]` by flushOne
      // Simulate WriterManager.cleanUp(tp) after handleErrors: evict granular cache only.
      // seekedOffsets + topicPartitionToETags are preserved (LC-2451 invariant).
      EvictGranularLocks(tp0),
      // RecommitPending models exactly what WriterManager.recommitPending does in production:
      // re-runs the failed commitOne chain on the preserved staging data without re-buffering.
      // ensureGranularLock detects PendingState=[Copy,Delete], replays them, then the new
      // processPendingOperations chain completes successfully.
      RecommitPending(tp0),
      PreCommit(tp0, 100L),
    )
    h.run(ops, expectCommitErrors = true)

    // Final blob present (exactly once — the retry did not duplicate it).
    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    // Temp blob cleaned up (Delete step ran in recovery).
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false
    // Offset 30 persisted exactly once.
    h.persistedOffsetsForPk(tp0, Some(pk)) shouldBe Set(30L)
    // preCommit advanced.
    h.ks(tp0).lastOption shouldBe Some(31L)
  }

  // ── Rebalance while writer in Uploading (mid-commit) state ───────────────────────────────────
  // docs/datalake-exactly-once-partitionby.md L564-L566; docs/datalake-sinks-write-pipeline.md L284
  test(
    "rebalance while writer in Uploading — granular cache evicted, seekedOffsets/master-eTag retained, fresh open replays PendingState, no duplication, K advances",
  ) {
    // 1. Drive a commit that leaves PendingState=[Copy, Delete] in the granular lock (Copy fails).
    val pk        = "pk-rebalance-uploading"
    val tempPath  = h.predictTempPathFor(tp0, Some(pk), 15L)
    val finalPath = h.expectedFinalPath(tp0, Some(pk), 15L)

    val phase1 = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some(pk), 15L),
      InjectFailMove(tempPath), // Copy will fail; PendingState=[Copy, Delete] written to granular lock
      Commit(tp0),              // Upload succeeds → temp blob present; Copy fails → PendingState left in lock
    )
    h.run(phase1, expectCommitErrors = true)
    // Temp blob present in storage (Upload succeeded before Copy failed).
    h.storage.pathExists(h.bucket, tempPath).getOrElse(false) shouldBe true
    h.storage.pathExists(h.bucket, finalPath).getOrElse(true) shouldBe false // not yet

    // 2. Simulate rebalance: WriterManager.close() evicts the in-memory granular cache for tp0
    //    but DOES NOT clear seekedOffsets or topicPartitionToETags (LC-2451 invariant).
    //    The EvictGranularLocks op maps directly to IndexManagerV2.evictAllGranularLocks(tp).
    //
    // 3. Crash + reboot: fresh IndexManagerV2 opens from durable storage.
    //    Because seekedOffsets were retained through the rebalance, the master lock eTag is
    //    still valid and does not need an unnecessary re-read on open.
    val phase2 = List[Op](
      EvictGranularLocks(tp0),  // simulate WriterManager.close() granular-cache eviction
      Crash,                    // new IndexManagerV2 instance reads from storage on open
      LoadGranularLock(tp0, pk),// fresh open triggers getSeekedOffsetForPartitionKey
      // → loadGranularLock → resolveAndCacheGranularLock
      // → PendingState=[Copy, Delete] replayed idempotently
    )
    h.run(phase2)

    // 4. Post-recovery assertions:
    //    - Final blob now present (Copy replayed: temp → final).
    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    //    - Temp blob cleaned up (Delete replayed).
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false
    //    - Offset 15 persisted exactly once (no duplication from the retry).
    h.persistedOffsetsForPk(tp0, Some(pk)) shouldBe Set(15L)

    // 5. PreCommit advances after a fresh put succeeds (K = max committed + 1).
    val phase3 = List[Op](
      Write(tp0, Some(pk), 16L),
      Commit(tp0),
      PreCommit(tp0, 100L),
    )
    h.run(phase3)
    h.ks(tp0).lastOption shouldBe Some(17L)
    h.persistedOffsetsForPk(tp0, Some(pk)) shouldBe Set(15L, 16L)
  }

  // ── Selective commit (PARTITIONBY) end-to-end safety ─────────────────────────────────────
  // Pin the selective-commit semantics (see "Selective commit fan-out" in
  // `docs/datalake-exactly-once-partitionby.md`): under selective commit,
  // a single `commitFlushableWriters` cycle now commits only the writers that are flushable;
  // sibling writers on the same TP that aren't flushable stay buffered and are not pulled
  // in. The exactly-once contract (no loss, no duplication, monotone K) must hold across
  // such asymmetric cycles, including when the buffered sibling commits later in a separate
  // cycle. This scenario exercises the storage / index-manager interaction when one pk
  // commits multiple times while another keeps buffering, then both finalize.

  test(
    "selective commit (PARTITIONBY): pk-a commits across multiple cycles while pk-b only commits at the end; per-pk final paths stay independent and K is monotone",
  ) {
    // Three commit cycles for pk-a (offsets 100, 200, 300), interleaved with PreCommit. In
    // none of the first three cycles is anything in pk-b's buffer — modelling a low-throughput
    // partition key that stays "buffered" (i.e. not flushable) throughout. At the end pk-b
    // commits a single batch (offsets 50, 60). Persisted bytes per pk must match exactly
    // what each writer committed, with no record bleeding across pks, and K must be
    // monotonically non-decreasing across every preCommit.
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some("pk-a"), 100L),
      Commit(tp0),
      PreCommit(tp0, 1000L),
      Write(tp0, Some("pk-a"), 200L),
      Commit(tp0),
      PreCommit(tp0, 1000L),
      Write(tp0, Some("pk-a"), 300L),
      Commit(tp0),
      PreCommit(tp0, 1000L),
      // Now pk-b finally flushes a single batch with strictly increasing offsets.
      Write(tp0, Some("pk-b"), 50L),
      Write(tp0, Some("pk-b"), 60L),
      Commit(tp0),
      PreCommit(tp0, 1000L),
    )
    h.run(ops)

    h.persistedOffsetsForPk(tp0, Some("pk-a")) shouldBe Set(100L, 200L, 300L)
    h.persistedOffsetsForPk(tp0, Some("pk-b")) shouldBe Set(50L, 60L)

    val ks = h.ks(tp0).toList
    ks shouldBe ks.scanLeft(0L)(math.max).tail.filter(_ > 0)
  }

  test(
    "selective commit (PARTITIONBY): crash mid-cycle on pk-a leaves pk-b's earlier durable state intact; recovery completes pk-a; no duplication",
  ) {
    // pk-b commits cleanly first (records 70, 71). Then in a subsequent cycle, pk-a's
    // commit fails mid-chain (Copy injected to fail). The durable state of pk-b must
    // survive untouched; on crash + recovery, pk-a's PendingState replays and completes.
    // Final state: every offset that was successfully committed by either pk lands at
    // exactly one final path; the failed-then-recovered pk-a record also lands once.
    val pkA       = "pk-a"
    val pkB       = "pk-b"
    val tempPathA = h.predictTempPathFor(tp0, Some(pkA), 80L)
    val finalA    = h.expectedFinalPath(tp0, Some(pkA), 80L)

    val ops = List[Op](
      Assign(Set(tp0)),
      // pk-b commits cleanly — establishes durable state we must protect.
      Write(tp0, Some(pkB), 70L),
      Write(tp0, Some(pkB), 71L),
      Commit(tp0),
      PreCommit(tp0, 1000L),
      // Now arm Copy failure for pk-a's next commit, then drive pk-a's commit. Copy fails
      // mid-chain → PendingState=[Copy, Delete] left in pk-a's granular lock. pk-b is not
      // touched (no fan-out), so pk-b's earlier durable state is unaffected.
      Write(tp0, Some(pkA), 80L),
      InjectFailMove(tempPathA),
      Commit(tp0), // pk-a commit fails; pk-b unaffected
      Crash,
      LoadGranularLock(tp0, pkA), // recovery replays pk-a's PendingState idempotently
      PreCommit(tp0, 1000L),
    )
    h.run(ops, expectCommitErrors = true)

    h.persistedOffsetsForPk(tp0, Some(pkB)) shouldBe Set(70L, 71L)
    h.persistedOffsetsForPk(tp0, Some(pkA)) shouldBe Set(80L)
    h.storage.pathExists(h.bucket, finalA).getOrElse(false) shouldBe true
    h.storage.pathExists(h.bucket, tempPathA).getOrElse(true) shouldBe false
    // First PreCommit: buffers and uploading empty, max(committed)=71 → K=72.
    // Second PreCommit: after recovery, max(committed)=max(71,80)=80 → K=81, clamped by hwm=72 → 81.
    h.ks(tp0).toList shouldBe List(72L, 81L)
  }

  test(
    "selective commit (PARTITIONBY): K is bounded by buffered sibling's firstBufferedOffset",
  ) {
    // pk-a commits (offset 200) while pk-b is still buffered (offset 50).
    // The production K-formula uses min(firstBufferedOffset) when any writer is still
    // buffering. Because pk-b has not yet committed, K must equal 50 (the buffered sibling's
    // first offset) rather than 201 (max-committed+1). This pins the buffered-min arm.
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some("pk-b"), 50L),
      Write(tp0, Some("pk-a"), 200L),
      CommitOnly(tp0, Some("pk-a")), // selective flush: only pk-a is committed
      PreCommit(tp0, 1000L),         // K = min(firstBuffered) = 50 (pk-b still buffering)
    )
    h.run(ops)
    // Buffered-min arm fires: K is clamped at pk-b's first buffered offset, not max(committed)+1.
    h.ks(tp0).toList shouldBe List(50L)
    h.persistedOffsetsForPk(tp0, Some("pk-a")) shouldBe Set(200L)
    h.persistedOffsetsForPk(tp0, Some("pk-b")) shouldBe Set.empty
  }

  test(
    "selective commit then sibling commits: K rises only after barrier clears",
  ) {
    // Extends the previous test: after pk-b also commits, there are no more buffered writers
    // so K rises to max(committed)+1. The HWM (50) ensures K never regresses between the
    // two PreCommit calls.
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some("pk-b"), 50L),
      Write(tp0, Some("pk-a"), 200L),
      CommitOnly(tp0, Some("pk-a")), // selective: only pk-a
      PreCommit(tp0, 1000L),         // K = 50 (buffered-min arm, pk-b still in Writing)
      CommitOnly(tp0, Some("pk-b")), // now pk-b commits; barrier clears
      PreCommit(tp0, 1000L),         // K = max(committed)+1 = 201, clamped by hwm=50 → 201
    )
    h.run(ops)
    // K rises from 50 to 201 only after the barrier writer (pk-b) commits.
    h.ks(tp0).toList shouldBe List(50L, 201L)
    h.persistedOffsetsForPk(tp0, Some("pk-a")) shouldBe Set(200L)
    h.persistedOffsetsForPk(tp0, Some("pk-b")) shouldBe Set(50L)
  }

  // ── End-to-end zombie scenario: rebalance race fenced via master-lock eTag ─────────
  // What this pins: the dirty-flag gate does NOT relax the zombie-fencing semantics that
  // `IndexManagerV2.updateMasterLock` provides. A zombie task whose master-lock write is
  // structurally delayed past a rebalance must still be fenced (`If-Match` fails), its
  // K-stream stays pinned (no consumer advance), and the new task's recovery does not
  // duplicate at the final path.
  //
  // How it is engineered: the harness installs the test-only pre-write barrier via
  // `IndexManagerV2.IndexManagerV2TestHooks.installPreWriteMasterLockBarrier` on the first
  // (zombie) IndexManagerV2 instance. The barrier runs an external master-lock write between
  // the cached-eTag read and the eTag-conditional storage write. This external write
  // simulates Task B (the new owner) bumping the master lock during the race window. When
  // the zombie's barrier returns, its own write fails with an eTag mismatch — the exact
  // fencing path the design relies on, exercised deterministically without timing-based
  // sleeps.
  test(
    "PARTITIONBY end-to-end zombie: master-lock write fenced by rebalance race, K does not advance, no duplication at final paths",
  ) {
    val pk        = "pk-zombie"
    val finalPath = h.expectedFinalPath(tp0, Some(pk), 42L)

    val setup = List[Op](
      Assign(Set(tp0)),
      Write(tp0, Some(pk), 42L),
      Commit(tp0), // record successfully persisted to its final path
    )
    h.run(setup)
    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    h.persistedOffsetsForPk(tp0, Some(pk)) shouldBe Set(42L)

    // Engineer the rebalance race: install a one-shot barrier on the zombie IndexManagerV2
    // that, when fired, performs an external master-lock write to bump the eTag. The
    // external write goes through the storage interface directly (bypasses the index
    // manager's cache) so the zombie's cached eTag becomes stale at the exact moment its
    // write is about to land.
    //
    // The barrier is installed AND torn down inside a `try/finally` so a mid-test assertion
    // failure cannot leak the seam onto a still-live `IndexManagerV2` instance for any
    // subsequent operation in this test (the after-each `silentClose()` is also a defence
    // in depth — see `Harness.silentClose()`). Without the `try/finally` an assertion at
    // line "K stream did NOT grow" could throw and skip the manual `clearPreWriteMasterLockBarrier`
    // call below, causing the `Crash` op a few lines later to drive a still-armed barrier
    // through `im.close() → drainGcQueue()` and produce a confusing secondary failure.
    val masterPath = h.masterLockPath(tp0)
    val fired      = new java.util.concurrent.atomic.AtomicBoolean(false)
    val zombieIM   = h.indexManager
    IndexManagerV2.IndexManagerV2TestHooks.installPreWriteMasterLockBarrier(
      zombieIM,
      () =>
        if (fired.compareAndSet(false, true)) {
          // Simulate Task B's `updateMasterLock` succeeding while the zombie is paused
          // mid-write: re-write the lock contents externally so the underlying eTag bumps.
          // The zombie's cached eTag is now stale, and its impending CAS will fail with
          // `If-Match` mismatch — the exact fencing path the design relies on.
          val currentPayload = h.storage.getBlobAsString(h.bucket, masterPath)
            .getOrElse(throw new AssertionError(s"zombie test: master lock missing at $masterPath"))
          h.storage.writeStringToFile(
            h.bucket,
            masterPath,
            io.lenses.streamreactor.connect.cloud.common.model.UploadableString(currentPayload),
          ).left.foreach(err => throw new AssertionError(s"zombie test: external write failed: ${err.message()}"))
        },
    )

    try {
      // Now drive a preCommit on the zombie. The barrier fires, the external write bumps
      // the eTag, and the zombie's own write fails with `If-Match` mismatch. preCommit
      // returns None for this TP; the K stream does not advance.
      val zombieKBeforeAttempt = h.ks.get(tp0).flatMap(_.lastOption)
      h.run(List[Op](PreCommit(tp0, 1000L)), expectPreCommitErrors = true)

      // K stream did NOT grow — the fenced preCommit returned None for this TP.
      h.ks.get(tp0).flatMap(_.lastOption) shouldBe zombieKBeforeAttempt
      fired.get() shouldBe true
    } finally {
      // Reset the barrier so post-fenced recovery is not affected. Idempotent — the
      // harness's `silentClose()` also clears it as a final safety net.
      IndexManagerV2.IndexManagerV2TestHooks.clearPreWriteMasterLockBarrier(zombieIM)
    }

    // Crash + reboot (simulates the new task instance taking over). Recovery reads the
    // durable master lock through `open()`, which seeds the new instance's eTag cache
    // with a fresh value. The next preCommit succeeds — and crucially the final path is
    // not touched: the record at offset 42 still lands at exactly one path.
    h.run(List[Op](Crash, PreCommit(tp0, 1000L)))

    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    h.persistedOffsetsForPk(tp0, Some(pk)) shouldBe Set(42L)
    // Final K is max(committed)+1 = 43, no regression below the zombie's last successful K.
    h.ks(tp0).lastOption shouldBe Some(43L)
  }

  // (d) mid-chain Copy failure returns FatalCloudSinkError (not NonFatal) and leaves no orphan after recovery
  test("non-PARTITIONBY: Copy failure returns FatalCloudSinkError and no temp blob is orphaned after recovery") {
    // The Copy phase is mid-chain (not an UploadOperation), so its failure escalates to
    // FatalCloudSinkError rather than NonFatalCloudSinkError. The PendingState=[Copy, Delete]
    // is recorded in the master lock BEFORE Copy runs, so on restart IndexManagerV2.open
    // replays the chain, deletes the temp blob, and leaves no orphan.
    val tempPath  = h.predictTempPathFor(tp0, None, 10L)
    val finalPath = h.expectedFinalPath(tp0, None, 10L)
    val ops = List[Op](
      Assign(Set(tp0)),
      Write(tp0, None, 10L),
      InjectFailMove(tempPath), // Copy will fail (one-shot); temp blob remains in storage
      Commit(tp0),              // expected FatalCloudSinkError (Copy failure is fatal)
      Crash,                    // open() reads master lock PendingState, replays Copy+Delete
      PreCommit(tp0, 100L),     // drives master lock write after successful recovery
    )
    h.run(ops, expectCommitErrors = true)
    // Post-recovery: final blob present, no orphaned temp.
    h.storage.pathExists(h.bucket, finalPath).getOrElse(false) shouldBe true
    h.storage.pathExists(h.bucket, tempPath).getOrElse(true) shouldBe false
    h.persistedOffsets(tp0) shouldBe Set(10L)
    // The commit error must have been Fatal, not NonFatal.
    h.lastCommitError should not be empty
    h.lastCommitError.get shouldBe a[FatalCloudSinkError]
    // K = max(committed)+1 = 10+1 = 11; recovery via open() seeded committed[(tp0,None)]=10.
    h.ks(tp0).toList shouldBe List(11L)
  }
}

object ExactlyOnceScenarioTest {

  val tp0: TopicPartition = Topic("topic-a").withPartition(0)
  val tp1: TopicPartition = Topic("topic-b").withPartition(0)

  sealed trait Op
  final case class Assign(tps: Set[TopicPartition]) extends Op
  final case class Write(tp: TopicPartition, pk: Option[String], offset: Long) extends Op
  final case class Commit(tp: TopicPartition) extends Op

  /**
   * Flush a single `(tp, pk)` pair, leaving all other buffered writers for `tp` untouched.
   * Models a selective-commit cycle (only the writer whose flush threshold fired commits;
   * siblings stay in `Writing`). Same failure semantics as `Commit`: on `commitOne` failure
   * the offsets move to the `uploading` map.
   */
  final case class CommitOnly(tp: TopicPartition, pk: Option[String]) extends Op

  /**
   * Re-run `commitOne` for every `(tp, pk)` currently in the `uploading` map.
   * Models `WriterManager.recommitPending`: retries the pending-op chain for writers still
   * in `Uploading` state without re-buffering records. Unlike `Commit`, this does NOT
   * require the `(tp, pk)` pair to have buffered data -- the offsets come from `uploading`.
   */
  final case class RecommitPending(tp: TopicPartition) extends Op

  final case class PreCommit(tp: TopicPartition, kafkaOffset: Long) extends Op
  final case class Rebalance(newAssignment: Set[TopicPartition]) extends Op
  case object Crash extends Op
  final case class InjectFailMove(tempPath: String) extends Op
  final case class InjectFailDelete(tempPath: String) extends Op
  final case class InjectFailWrite(path: String) extends Op
  final case class InjectCorruptETag(path: String) extends Op
  final case class AgeBlobAt(path: String, lastModified: Instant) extends Op

  /** Calls `IndexManagerV2.getSeekedOffsetForPartitionKey` directly (cache miss → loadGranularLock). */
  final case class LoadGranularLock(tp: TopicPartition, pk: String) extends Op

  /**
   * Directly overwrites the granular lock on storage with a new `pendingState`.
   * `committedOffset` is set to `None` so the lock looks like a pre-commit write.
   * Use this to simulate crash-after-Copy states without going through the normal commit flow.
   */
  final case class ManipulateGranularLock(
    tp:           TopicPartition,
    pk:           String,
    pendingState: Option[PendingState],
  ) extends Op

  /**
   * Calls `IndexManagerV2.evictAllGranularLocks(tp)` directly, simulating what
   * `WriterManager.cleanUp(tp)` does to the IndexManagerV2 state after a fatal error.
   * Unlike [[Crash]], this does NOT destroy `seekedOffsets` or `topicPartitionToETags`
   * (the LC-2451 invariant: those are preserved so the master-lock eTag remains valid).
   */
  final case class EvictGranularLocks(tp: TopicPartition) extends Op

  /**
   * Wires a real `IndexManagerV2` + `PendingOperationsProcessors` against an in-memory fake.
   * Tracks per-(tp, pk) committed offsets locally so K is computed without round-tripping
   * to storage on every PreCommit (mirrors how `WriterManager` caches its own writers).
   */
  final class Harness {

    val bucket:            String = "test-bucket"
    val directoryFileName: String = ".indexes2"

    val storage = new InMemoryStorageInterface()
    implicit val si:           StorageInterface[FakeFileMetadata] = storage
    implicit val taskId:       ConnectorTaskId                    = ConnectorTaskId("eo-test", 1, 0)
    implicit val locValidator: CloudLocationValidator             = (location: CloudLocation) => Validated.valid(location)

    private val pop = new PendingOperationsProcessors(storage)

    private def bucketAndPrefix(tp: TopicPartition): Either[SinkError, CloudLocation] =
      CloudLocation(bucket, Some(s"data/${tp.topic.value}/${tp.partition}/")).asRight

    private var im: IndexManagerV2 = _

    // -- shadow state --
    val ks:        mutable.Map[TopicPartition, mutable.ListBuffer[Long]] = mutable.Map.empty
    val delivered: mutable.Map[TopicPartition, mutable.Set[Long]]        = mutable.Map.empty

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

    /**
     * Offsets that were drained from `buffers` but whose `commitOne` call failed.
     * Mirrors a `Writer` in `Uploading` state: the offsets contribute their minimum as
     * `firstBufferedOffset` to the K-formula and are inert until `RecommitPending` retries.
     * Cleared by `Crash` (in-memory `Uploading` state is lost on JVM crash).
     */
    private val uploading = mutable.Map.empty[(TopicPartition, Option[String]), List[Long]]

    /** Records the harness has issued on the current run (used for invariant a/a'). */
    private val deliveredAll = mutable.Map.empty[TopicPartition, mutable.Set[Long]]

    private var assignment: Set[TopicPartition] = Set.empty

    /** Last commit error captured from a failed `commitOne`. Used by tests to assert error type. */
    var lastCommitError: Option[SinkError] = None

    def bootTask(): Unit = {
      im = new IndexManagerV2(
        bucketAndPrefix,
        pop,
        directoryFileName,
        gcIntervalSeconds      = Int.MaxValue,
        gcSweepIntervalSeconds = Int.MaxValue,
        gcSweepMinAgeSeconds   = Int.MaxValue,
        gcSweepEnabled         = false,
      )(si, taskId)
      if (assignment.nonEmpty) openOrFail(assignment)
    }

    private def openOrFail(tps: Set[TopicPartition]): Unit = {
      val res    = im.open(tps)
      val seeded = res.fold(err => throw new AssertionError(s"open failed: ${err.message()}"), identity)
      // Refresh local committed view from what storage returned for each tp's master lock.
      seeded.foreach {
        case (tp, Some(off)) => committed((tp, None)) = off.value
        case (tp, None)      => committed.remove((tp, None))
      }
    }

    /**
     * Evict the in-memory granular cache for `tp` without destroying the IndexManagerV2 instance.
     *  Simulates what `WriterManager.cleanUp(tp)` does: calls `evictAllGranularLocks(tp)` but
     *  does NOT clear `seekedOffsets` or `topicPartitionToETags` (LC-2451 invariant).
     */
    def evictGranularLocks(tp: TopicPartition): Unit =
      im.evictAllGranularLocks(tp)

    /**
     * Accessor for the wired `IndexManagerV2` instance. Used by tests that drive the
     * `IndexManagerV2.IndexManagerV2TestHooks` seam directly (rebalance-race / zombie
     * scenarios). Most tests should prefer the high-level `Op` DSL instead — this
     * accessor is the narrow seam for low-level orchestration that the DSL cannot
     * express deterministically.
     */
    def indexManager: IndexManagerV2 = im

    /** Drop the IndexManagerV2 (releases executors). The next bootTask reads from storage. */
    def crash(): Unit = {
      // Defensive: clear any test-only seam that a test installed via
      // `IndexManagerV2.IndexManagerV2TestHooks` so cross-test leakage is impossible
      // even if a test exits before its own teardown. Functionally a no-op once
      // `im = null` runs (the seam dies with the instance), but keeps the harness
      // self-documenting: every Crash conceptually drops ALL in-memory state, hooks
      // included.
      if (im != null) IndexManagerV2.IndexManagerV2TestHooks.clearPreWriteMasterLockBarrier(im)
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
        // Same defensive seam-clear as `crash()` — see comment there. `silentClose()` is
        // the after-each hook, so this is the last line of defence against any test that
        // installed a barrier and then aborted before clearing it.
        IndexManagerV2.IndexManagerV2TestHooks.clearPreWriteMasterLockBarrier(im)
        try im.close()
        catch { case _: Throwable => () }
        im = null
      }

    def run(
      ops:                   List[Op],
      expectCommitErrors:    Boolean = false,
      expectPreCommitErrors: Boolean = false,
      checkInvariants:       Boolean = true,
    ): Unit = {
      lastCommitError = None
      ops.foreach(runOp(_, expectCommitErrors, expectPreCommitErrors))
      if (checkInvariants) this.checkInvariants()
    }

    private def runOp(op: Op, expectCommitErrors: Boolean, expectPreCommitErrors: Boolean): Unit = op match {
      case Assign(tps) =>
        assignment = tps
        openOrFail(tps)
        currentGenerationLastK.clear()

      case Write(tp, pk, offset) =>
        // WS6 guard: a (tp, pk) pair in Uploading must be retried via RecommitPending before
        // new records are buffered. Accepting a Write on top of a failed commit would silently
        // merge new offsets into the retry batch, masking the Uploading state.
        assert(
          !uploading.contains((tp, pk)),
          s"Write($tp, $pk, $offset) attempted while (tp, pk) is in Uploading state. " +
            s"Use RecommitPending($tp) to retry the failed commit before writing new records.",
        )
        val _ = buffers.getOrElseUpdate((tp, pk), mutable.ListBuffer.empty).append(offset)
        val _ = deliveredAll.getOrElseUpdate(tp, mutable.Set.empty).add(offset)

      case Commit(tp) =>
        // Commit every buffered writer for this TP. Order across pks within a TP doesn't
        // matter for the assertions, but we sort for determinism in test output.
        val toFlush = buffers.keys.filter(_._1 == tp).toList.sortBy(_._2.getOrElse(""))
        toFlush.foreach { key =>
          val offsets = buffers(key).toList
          buffers.remove(key)
          if (offsets.nonEmpty) flushOne(tp, key._2, offsets, expectCommitErrors)
        }

      // selective commit — flush a single (tp, pk) only, siblings remain buffered.
      case CommitOnly(tp, pk) =>
        val key     = (tp, pk)
        val offsets = buffers.get(key).map(_.toList).getOrElse(Nil)
        buffers.remove(key)
        if (offsets.nonEmpty) flushOne(tp, pk, offsets, expectCommitErrors)

      // retry writers in Uploading state -- mirrors WriterManager.recommitPending.
      case RecommitPending(tp) =>
        val toRetry = uploading.keys.filter(_._1 == tp).toList.sortBy(_._2.getOrElse(""))
        toRetry.foreach { key =>
          val offsets = uploading(key)
          uploading.remove(key)
          if (offsets.nonEmpty) flushOne(tp, key._2, offsets, expectCommitErrors)
        }

      case PreCommit(tp, _) =>
        // Mirror production WriterManager.getOffsetAndMeta two-arm formula:
        //   if any writer has buffered (uncommitted) data → K = min(firstBufferedOffset)
        //   else                                          → K = max(committedOffset) + 1
        // Both `buffers` (Writing state) and `uploading` (Uploading state) contribute a
        // firstBufferedOffset equal to their smallest offset. The HWM clamp then ensures
        // K never regresses.
        val firstBuffered: Iterable[Long] =
          buffers.collect { case ((t, _), buf) if t == tp && buf.nonEmpty => buf.head } ++
            uploading.collect { case ((t, _), offs) if t == tp && offs.nonEmpty => offs.min }

        val activeKeys = committed.keys.filter(_._1 == tp).toList
        val maxCommitted = activeKeys.flatMap(committed.get)
          .reduceOption((a: Long, b: Long) => math.max(a, b))

        val rawK =
          if (firstBuffered.nonEmpty) firstBuffered.min
          else maxCommitted.map(_ + 1L).getOrElse(0L)

        val hwm = kHighWatermark.getOrElse(tp, 0L)
        val k   = math.max(rawK, hwm)
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
        // Close + reopen with the new assignment, as an eager rebalance does. We reuse the
        // IndexManagerV2 -- production CloudSinkTask does too: close(partitions) releases every
        // partition it is given, and open() re-takes ownership of the new set.
        im.closePartitions(assignment)
        openOrFail(newAssignment)
        assignment = newAssignment
        currentGenerationLastK.clear()

      case Crash =>
        // WS7: uploading is cleared on crash. Production's in-memory Uploading state is lost
        // on a JVM crash; only the granular/master lock's PendingState survives in storage and
        // is replayed on recovery via LoadGranularLock / ensureGranularLock.
        uploading.clear()
        crash()
        bootTask()

      case InjectFailMove(path)    => val _ = storage.arm(FailMoveAt(bucket, path))
      case InjectFailDelete(path)  => val _ = storage.arm(FailDeleteAt(bucket, path))
      case InjectFailWrite(path)   => val _ = storage.arm(FailWriteAt(bucket, path))
      case InjectCorruptETag(path) => val _ = storage.arm(CorruptETag(bucket, path))
      case AgeBlobAt(path, instant) =>
        if (!storage.setLastModified(bucket, path, instant))
          throw new AssertionError(s"AgeBlobAt: no blob exists at $bucket/$path")

      case LoadGranularLock(tp, pk) =>
        im.getSeekedOffsetForPartitionKey(tp, pk) match {
          case Right(maybeOffset) =>
            // Mirror what WriterManager.createWriter does: if resolution returned a committed
            // offset, update the harness committed map so PreCommit sees the right floor.
            maybeOffset.foreach(off => committed((tp, Some(pk))) = off.value)
          case Left(err) =>
            throw new AssertionError(s"LoadGranularLock($tp, $pk) failed: ${err.message()}")
        }

      case ManipulateGranularLock(tp, pk, newPendingState) =>
        val lockPath = granularLockPath(tp, pk)
        val (_, currentETag) = storage.getBlobAsStringAndEtag(bucket, lockPath)
          .getOrElse(throw new AssertionError(s"ManipulateGranularLock: lock not found at $bucket/$lockPath"))
        // Rebuild lock owner using the same format IndexManagerV2 uses internally.
        val lockOwner = s"${taskId.name} - ${taskId.taskNo + 1} of ${taskId.maxTasks}"
        val newIdx    = IndexFile(lockOwner, None, newPendingState)
        import IndexFile.indexFileEncoder
        val _ = storage.writeBlobToFile(bucket, lockPath, ObjectWithETag(newIdx, currentETag))
          .left.map(err => throw new AssertionError(s"ManipulateGranularLock write failed: ${err.message()}"))

      case EvictGranularLocks(tp) =>
        evictGranularLocks(tp)
    }

    // WS2: shared helper for Commit / CommitOnly / RecommitPending.
    // On failure, offsets move to `uploading` (mirroring Writer.Uploading) rather than
    // being discarded. On success, any stale `uploading` entry for the same key is cleared
    // because the new commit supersedes the prior failed one.
    private def flushOne(
      tp:                 TopicPartition,
      pk:                 Option[String],
      offsets:            List[Long],
      expectCommitErrors: Boolean,
    ): Unit =
      commitOne(tp, pk, offsets) match {
        case Right(_) =>
          val _ = uploading.remove((tp, pk)) // clear any prior failed attempt for this key
        case Left(err) =>
          uploading((tp, pk)) = offsets
          lastCommitError     = Some(err)
          if (!expectCommitErrors)
            throw new AssertionError(s"unexpected commit failure for $tp/$pk: ${err.message()}")
      }

    private def commitOne(tp: TopicPartition, pk: Option[String], offsets: List[Long]): Either[SinkError, Unit] = {
      val tempLocal       = writeLocalTempFile(offsets)
      val finalPath       = expectedFinalPath(tp, pk, offsets.max)
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
          // The caller (flushOne) is responsible for moving offsets to `uploading` and
          // recording lastCommitError. commitOne is a pure attempt with no side-effects
          // on the shadow state beyond `committed` / `delivered` on success.
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
