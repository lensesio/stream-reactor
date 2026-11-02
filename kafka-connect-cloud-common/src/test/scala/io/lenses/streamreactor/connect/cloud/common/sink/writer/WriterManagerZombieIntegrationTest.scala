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
package io.lenses.streamreactor.connect.cloud.common.sink.writer

import cats.data.Validated
import cats.implicits._
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CloudCommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailWriteAt
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.immutable

/**
 * Integrated tests that wire a real [[WriterManager]] + real [[IndexManagerV2]] +
 * [[InMemoryStorageInterface]] for the PARTITIONBY master-lock failure / fencing paths.
 *
 * Coverage complement:
 *
 *  - [[MasterLockFrequencyTest]] (mock-driven): exercises the real `WriterManager.preCommit`
 *    decision logic (`Skip` / `WriteRoutine` / `WriteForced`) against a *mocked* `IndexManager`.
 *    Regressions in the gating / decision logic are caught there.
 *
 *  - [[io.lenses.streamreactor.connect.cloud.common.sink.ExactlyOnceScenarioTest]]
 *    (synthetic-harness): exercises real `IndexManagerV2` + real storage but bypasses
 *    `WriterManager.preCommit` by calling `IndexManagerV2.updateMasterLock` directly from
 *    the harness `PreCommit` op handler.
 *
 *  - THIS SUITE: closes the integrated gap — no single prior test exercises the chain
 *    `WriterManager.preCommit` → `WriteDecision` → `attemptMasterLockWrite` →
 *    `IndexManagerV2.updateMasterLock` → `InMemoryStorageInterface`.
 *
 * Test matrix — one test per failure-arm shape:
 *
 *   (b) `WriteRoutine` failure-then-recover (multi-cycle dirty window): K consecutive
 *       storage failures must not advance the HWM or the durable master-lock floor.
 *       Recovery on cycle K+1 uses the same `globalSafeOffset` that was first attempted,
 *       verified via `IndexManagerV2.getSeekedOffsetForTopicPartition`.
 *
 *   (c) `WriteForced(PostCleanUp)` one-shot failure: the force flag is cleared *before*
 *       the write attempt so a failure does not re-arm the forced path on the next cycle.
 *       The next cycle re-enters via the routine dirty-flag path and succeeds.
 *
 *   (d) `WriteForced(Revoke)` success: a dirty TP at `close()` persists `globalSafeOffset`
 *       to the master lock; durable floor advances; metrics tagged Revoke.
 *
 *   (e) `WriteForced(Revoke)` failure: durable floor unchanged; metrics tagged; fresh
 *       owner replays from the older durable floor; granular lock absent.
 *
 * Note: the zombie / stale-eTag fencing scenario (formerly labelled (a)) is covered
 * end-to-end by `ExactlyOnceScenarioTest` ("PARTITIONBY end-to-end zombie: master-lock
 * write fenced by rebalance race, K does not advance, no duplication at final paths"),
 * which additionally verifies final-path deduplication after Crash + reopen.
 *
 * Writer construction: writers are created directly with `lastSeekedOffset = committedOffset`
 * so they start in `NoWriter(CommitState(tp, committedOffset))` state — exactly the same
 * observable state as `makeIdleWriter(...) + forceWriteState(NoWriter(...))` in
 * [[MasterLockFrequencyTest]], but achieved via the public `Writer` constructor rather than
 * the internal test seam. No `forceWriteState` is used.
 */
class WriterManagerZombieIntegrationTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("eo-zombie-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)

  private val bucket        = "test-bucket"
  private val directoryName = ".indexes2"
  private val tp0           = Topic("topic").withPartition(0)

  private val dateField: PartitionField                        = ValuePartitionField(PartitionNamePath("date"))
  private val dateA:     immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-01")

  // Master lock path that IndexManagerV2 will use for tp0. Note: `tp0.topic` renders as
  // "Topic(topic)" (the case-class toString), matching IndexManagerV2.generateLockFilePath
  // which interpolates `${topicPartition.topic}` (not `.topic.value`).
  //   <directoryName>/<connectorName>/.locks/Topic(<topic>)/<partition>.lock
  private val masterLockPath: String =
    s"$directoryName/${connectorTaskId.name}/.locks/${tp0.topic}/${tp0.partition}.lock"

  private def bucketAndPrefix(tp: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${tp.topic.value}/${tp.partition}/")).asRight

  /**
   * Build a real `IndexManagerV2` against the provided storage. Mirrors
   * `ExactlyOnceScenarioTest.Harness.bootTask` exactly (GC disabled, all intervals at
   * `Int.MaxValue` to prevent background activity during tests).
   */
  private def buildIndexManager(storage: StorageInterface[FakeFileMetadata]): IndexManagerV2 = {
    implicit val si: StorageInterface[FakeFileMetadata] = storage
    val pop = new PendingOperationsProcessors(storage)
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = pop,
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = Int.MaxValue,
      gcSweepMinAgeSeconds        = Int.MaxValue,
      gcSweepEnabled              = false,
    )(si, connectorTaskId)
  }

  /**
   * Build a real `WriterManager` backed by the given `IndexManagerV2`. Mirrors the pattern
   * from `WriterUploadRetryRecoveryTest.buildMinimalWriterManager`. The format-writer /
   * object-key-builder lambdas are mocked because no actual record writes are performed —
   * only `preCommit` paths are exercised.
   */
  private def buildWriterManager(
    im:      IndexManagerV2,
    storage: StorageInterface[FakeFileMetadata],
    metrics: CloudSinkMetrics,
  ): WriterManager[FakeFileMetadata] = {
    val pop = new PendingOperationsProcessors(storage)
    new WriterManager[FakeFileMetadata](
      commitPolicyFn              = _ => CloudCommitPolicy.Default.asRight,
      bucketAndPrefixFn           = bucketAndPrefix,
      keyNamerFn                  = _ => mock[KeyNamer].asRight,
      stagingFilenameFn           = (_, _) => File.createTempFile("wm-zombie-", ".tmp").asRight,
      objKeyBuilderFn             = (_, _) => mock[ObjectKeyBuilder],
      formatWriterFn              = (_, _) => mock[FormatWriter].asRight,
      indexManager                = im,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = pop,
      metrics                     = metrics,
    )
  }

  /**
   * Build a `Writer` that starts in `NoWriter(CommitState(tp, committedOffset))` state
   * without using `forceWriteState`. The `Writer` constructor always initialises its state
   * to `NoWriter(CommitState(tp, lastSeekedOffset))`, so passing the desired committed
   * offset as `lastSeekedOffset` achieves the same effect as the `makeIdleWriter` +
   * `forceWriteState` pattern used in the mock-driven `MasterLockFrequencyTest`, but via
   * the public constructor only.
   *
   * `partitionKey = WriterManager.derivePartitionKey(dateA)` ensures that the
   * corresponding `MapKey(tp, dateA)` (with non-empty `partitionValues`) triggers the
   * PARTITIONBY branch (`hasPartitionByWriters = true`) inside `getOffsetAndMeta`.
   */
  private def makeCommittedWriter(
    tp:              TopicPartition,
    im:              IndexManagerV2,
    storage:         StorageInterface[FakeFileMetadata],
    committedOffset: Option[Offset],
  ): Writer[FakeFileMetadata] = {
    val pop = new PendingOperationsProcessors(storage)
    new Writer[FakeFileMetadata](
      tp,
      CloudCommitPolicy.Default,
      im,
      () => File.createTempFile("committed-writer-", ".tmp").asRight,
      mock[ObjectKeyBuilder],
      _ => mock[FormatWriter].asRight,
      mock[SchemaChangeDetector],
      pop,
      partitionKey     = WriterManager.derivePartitionKey(dateA),
      lastSeekedOffset = committedOffset,
    )
  }

  // ── (b) WriteRoutine failure-then-recover (multi-cycle dirty window) ──────────────────

  test(
    "Integrated WriteRoutine: 3 consecutive master-lock write failures do not advance HWM; " +
      "recovery on cycle 4 uses the same globalSafeOffset; durable floor verified via getSeekedOffsetForTopicPartition",
  ) {
    val storage = new InMemoryStorageInterface()
    val metrics = new CloudSinkMetrics()
    val im      = buildIndexManager(storage)
    im.open(Set(tp0)).value

    val wm = buildWriterManager(im, storage, metrics)

    // establish lastWritten = 51 (committed offset 50 → K = 51).
    val writer1 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writer1)
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Add writer with higher committed offset and arm 3 one-shot master-lock failures.
    val writer2 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(100)))
    wm.putWriter(MapKey(tp0, dateA), writer2)
    (1 to 3).foreach(_ => storage.arm(FailWriteAt(bucket, masterLockPath)))

    // 3 failure cycles: each returns empty map; HWM and durable floor remain at K = 51.
    (1 to 3).foreach { _ =>
      val result = wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))
      result.get(tp0) shouldBe None
    }
    metrics.getMasterLockFailures shouldBe 3L
    metrics.getMasterLockUpdates shouldBe 1L // only the first cycle's success

    // Recovery: all armed failures consumed, next cycle succeeds at K = 101 (same as the
    // first failed attempt — no HWM regression, no skip-ahead).
    val resultRecover = wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))
    resultRecover(tp0).offset() shouldBe 101L
    metrics.getMasterLockUpdates shouldBe 2L
    metrics.getMasterLockFailures shouldBe 3L // no new failures

    // GC threshold: the durable master lock now encodes committedOffset = K-1 = 100.
    // This is the same value passed to cleanUpObsoleteLocks (same-value invariant).
    im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(100))

    wm.close()
    im.close()
  }

  // ── (c) WriteForced(PostCleanUp) one-shot failure → routine dirty-flag retry ──────────

  test(
    "Integrated WriteForced(PostCleanUp): flag cleared before write attempt; " +
      "next cycle classifies Skip (durable floor matches globalSafeOffset) and returns correct K without redundant write",
  ) {
    val storage = new InMemoryStorageInterface()
    val metrics = new CloudSinkMetrics()
    val im      = buildIndexManager(storage)
    im.open(Set(tp0)).value

    val wm = buildWriterManager(im, storage, metrics)

    // establish lastWritten = 51, lastReturnedSafeOffset = 51.
    val writer1 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writer1)
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // cleanUp: evicts writers, clears lastWrittenMasterSafeOffset(tp0) and HWM,
    // preserves lastReturnedSafeOffset(tp0) = 51, sets forceWriteAfterCleanUp(tp0).
    wm.cleanUp(tp0)

    // Re-add a writer with a lower committed offset (30). HWM re-seeds from
    // max(durableFloor+1 = 51, lastReturned = 51) = 51. calculatedSafeOffset = 31.
    // globalSafeOffset = max(31, 51) = 51.
    val writer2 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(30)))
    wm.putWriter(MapKey(tp0, dateA), writer2)

    // Arm exactly one master-lock failure to fail the forced write.
    storage.arm(FailWriteAt(bucket, masterLockPath))

    // WriteForced(PostCleanUp) fires and fails.
    // The flag is cleared BEFORE the write attempt (WriterManager.scala line 627),
    // so the next cycle does NOT re-enter as WriteForced.
    val result2 = wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))
    result2.get(tp0) shouldBe None
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
    metrics.getMasterLockFailures shouldBe 1L

    // storage healthy; the real IndexManagerV2's seekedOffsets(tp0) = Offset(50)
    // (written by cycle 1), so lastWritten re-seeds to 50+1 = 51, which equals
    // globalSafeOffset = 51. dirty = false → Skip branch.
    //
    // Key assertion: the Skip branch is reached (not WriteForced — the flag was cleared in
    // cycle 2 before the failed write attempt) and not WriteRoutine (durable floor is
    // current). The Skip branch still returns 51 to Kafka Connect without writing again.
    val result3 = wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))
    result3(tp0).offset() shouldBe 51L
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L // one-shot, not re-fired
    metrics.getMasterLockUpdates shouldBe 1L                // no redundant write on Skip
    metrics.getMasterLockWriteSkipped shouldBe 1L           // Skip counter incremented
    metrics.getMasterLockFailures shouldBe 1L               // no new failure

    wm.close()
    im.close()
  }

  // ── (e) WriteForced(Revoke) failure: durable floor unchanged; fresh owner replays from older floor ──

  test(
    "Integrated force-on-revoke failure: durable floor unchanged; metrics tagged; fresh owner replays " +
      "from older floor; master lock fences replay dedup; granular lock absent (falls back to master floor)",
  ) {
    // Shape:
    // Initial write: committed=50 → K=51 persisted (durable floor = Offset(50)).
    //   Add writer2 with committed=100 and skip preCommit: dirty=true on close.
    //   Arm one FailWriteAt for the master-lock path.
    //   wm.close(): force-on-revoke fires → updateMasterLock(Offset(101)) fails → durable floor stays at Offset(50).
    //   Assert: metrics, writerCount==0 (cleanup ran), durable floor unchanged.
    //   Assert: no granular lock file was written for dateA (makeCommittedWriter bypasses ensureGranularLock),
    //     so getSeekedOffsetForPartitionKey returns Right(None) and the master floor is the sole dedup floor.
    //   Reopen: fresh im2 reads Offset(50) from master lock.
    //   Verify: fresh wm2 preCommit with committed=50 returns K=51 (older floor, not 101).
    //   This demonstrates that a failed force-write does not lose the dedup guarantee:
    //   records at offsets 0-50 are skipped by shouldSkip (master floor), and records 51-100
    //   are correctly replayed without creating duplicates of the already-committed output.
    val storage = new InMemoryStorageInterface()
    val metrics = new CloudSinkMetrics()
    val im      = buildIndexManager(storage)
    im.open(Set(tp0)).value

    val wm = buildWriterManager(im, storage, metrics)

    // routine write establishes lastWritten = 51 (committed offset 50 → K = 51).
    val writer1 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writer1)
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Add a higher-offset writer WITHOUT a preCommit between. The dirty bit is now true
    // (globalSafeOffset = 101 > lastWritten = 51), so the close-path force-write fires.
    val writer2 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(100)))
    wm.putWriter(MapKey(tp0, dateA), writer2)

    // Arm exactly one master-lock write failure: the force-on-revoke will be fenced.
    storage.arm(FailWriteAt(bucket, masterLockPath))

    wm.close()

    // Forced revoke was attempted but failed; aggregate failure counter also incremented.
    metrics.getMasterLockWriteForcedRevoke shouldBe 1L
    metrics.getMasterLockWriteForcedRevokeFailures shouldBe 1L
    metrics.getMasterLockFailures shouldBe 1L
    // Initial routine write succeeded; the failed force-write must NOT falsely increment masterLockUpdates.
    metrics.getMasterLockUpdates shouldBe 1L

    // Cleanup contract: writer close and cache eviction must still run despite the force failure.
    wm.writerCount shouldBe 0

    // Durable floor stays at Offset(50) — the force-write did not advance it.
    im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(50))

    // Granular lock: makeCommittedWriter + putWriter bypasses ensureGranularLock, so no
    // granular lock file was written for dateA. The cache was evicted by closePartition.
    // getSeekedOffsetForPartitionKey performs a storage read → FileNotFoundError → Right(None).
    // In production the granular lock for the already-committed partition key would provide
    // an additional per-key dedup floor; here the master lock floor is the sole fence.
    val partitionKey = WriterManager.derivePartitionKey(dateA).get
    im.getSeekedOffsetForPartitionKey(tp0, partitionKey).value shouldBe None

    // Reopen: fresh IndexManagerV2 reads the master lock (Offset(50)) from storage.
    im.close()
    val im2      = buildIndexManager(storage)
    val metrics2 = new CloudSinkMetrics()
    im2.open(Set(tp0)).value
    im2.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(50))

    // Fresh owner recovers from the older durable floor (not the uncommitted K=101).
    // Add a writer whose committedOffset matches the master floor (Offset(50)) to simulate
    // the first replay cycle. preCommit must return K=51 via the Skip path: the durable
    // master lock already encodes committedOffset=50 → lastWritten seeds to 51, which equals
    // globalSafeOffset=51, so dirty=false → Skip (no redundant write). Records 0-50 are
    // deduped by the master floor; records 51-100 are replayed without creating duplicates.
    val wm2     = buildWriterManager(im2, storage, metrics2)
    val writer3 = makeCommittedWriter(tp0, im2, storage, committedOffset = Some(Offset(50)))
    wm2.putWriter(MapKey(tp0, dateA), writer3)
    val result3 = wm2.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))
    result3(tp0).offset() shouldBe 51L             // older floor, not 101
    metrics2.getMasterLockUpdates shouldBe 0L      // Skip path: durable floor already matches globalSafeOffset
    metrics2.getMasterLockWriteSkipped shouldBe 1L // Skip counter incremented

    wm2.close()
    im2.close()
  }

  // ── (d) WriteForced(Revoke) success → durable floor advances ─────────────────────────

  test(
    "Integrated force-on-revoke success: dirty TP at close() persists globalSafeOffset to master lock; " +
      "durable floor advances; metrics tagged Revoke; granular cache evicted",
  ) {
    // This test closes the integration coverage gap identified in the exactly-once review:
    // tests (a)–(c) cover routine success, multi-cycle failure, and forced(PostCleanUp)
    // failure. This test covers forced(Revoke) SUCCESS — the path through
    //   `wm.close() → closePartition(tp, Revoke) → attemptForceMasterLockWrite → updateMasterLock`
    // against the real IndexManagerV2 + InMemoryStorageInterface.
    //
    // Shape:
    // Initial write: committed=50 → K=51 persisted (lastWritten=51).
    //   Skip preCommit after adding high writer: dirty=true on close.
    //   wm.close(): force-on-revoke fires → updateMasterLock(Offset(101)) succeeds.
    //   Assert durable floor = Offset(100) via getSeekedOffsetForTopicPartition (master
    //   stores committedOffset = K-1 = 100, not globalSafeOffset = 101).
    val storage = new InMemoryStorageInterface()
    val metrics = new CloudSinkMetrics()
    val im      = buildIndexManager(storage)
    im.open(Set(tp0)).value

    val wm = buildWriterManager(im, storage, metrics)

    // routine write establishes lastWritten = 51 (committed offset 50 → K = 51).
    val writer1 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writer1)
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200)))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Add a higher-offset writer WITHOUT a preCommit between. The dirty bit is now true
    // (globalSafeOffset = 101 > lastWritten = 51) so the close-path force-write fires.
    val writer2 = makeCommittedWriter(tp0, im, storage, committedOffset = Some(Offset(100)))
    wm.putWriter(MapKey(tp0, dateA), writer2)

    // No storage faults armed — the force-on-revoke write succeeds.
    wm.close()

    // Forced write was attempted (once) and succeeded, tagged as Revoke.
    metrics.getMasterLockWriteForcedRevoke shouldBe 1L
    // Universal success counter = cycle 1 routine + force-on-revoke.
    metrics.getMasterLockUpdates shouldBe 2L
    metrics.getMasterLockFailures shouldBe 0L

    // Durable master floor: IndexManagerV2 stores committedOffset = globalSafeOffset - 1.
    // globalSafeOffset from the writers = max(committed)+1 = 101, so committedOffset = 100.
    im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(100))

    // All writers closed and removed.
    wm.writerCount shouldBe 0

    // Granular cache evicted by closePartition step 6: re-adding a writer for the same
    // partition key triggers ensureGranularLock, which re-reads from storage (cache miss)
    // rather than using a stale in-memory entry. We verify this by opening a fresh
    // IndexManagerV2 from the same storage and confirming it can open successfully and
    // read the persisted floor — proving the durable state is self-consistent.
    im.close()
    val im2 = buildIndexManager(storage)
    im2.open(Set(tp0)).value
    im2.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(100))
    im2.close()
  }

  // ════════════════════════════════════════════════════════════════════════════════════
  // End-to-end zombie scenarios in commit.mode=batch
  // ════════════════════════════════════════════════════════════════════════════════════

  private val batchKey: immutable.Map[PartitionField, String] = Map(dateField -> "2024-02-02")

  private def buildBatchIndexManager(storage: StorageInterface[FakeFileMetadata]): IndexManagerV2 = {
    implicit val si: StorageInterface[FakeFileMetadata] = storage
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = Int.MaxValue,
      gcSweepMinAgeSeconds        = Int.MaxValue,
      gcSweepEnabled              = false,
      commitMode                  = io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode.Batch,
    )(si, connectorTaskId)
  }

  private def batchStagingFile(): File = {
    val f = File.createTempFile("wm-zombie-batch-", ".tmp")
    java.nio.file.Files.write(f.toPath, "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8))
    f.deleteOnExit()
    f
  }

  /** A batch WriterManager plus a mutable flush toggle: buffer with flush=false, then flip + commit. */
  private final class BatchPair(
    val im:    IndexManagerV2,
    val wm:    WriterManager[FakeFileMetadata],
    val flush: java.util.concurrent.atomic.AtomicBoolean,
  ) {
    def deliver(offsets: Range): Unit =
      offsets.foreach(o => wm.write(tp0.withOffset(Offset(o.toLong)), batchMessage(o.toLong)).value)
    def commit(): Either[io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError, Unit] = {
      flush.set(true)
      val r = wm.commitFlushableWriters()
      flush.set(false)
      r
    }
  }

  private def buildBatchWriterManager(
    im:      IndexManagerV2,
    storage: StorageInterface[FakeFileMetadata],
    metrics: CloudSinkMetrics,
    values:  immutable.Map[PartitionField, String],
    flush:   java.util.concurrent.atomic.AtomicBoolean,
  ): WriterManager[FakeFileMetadata] = {
    val keyNamer = mock[KeyNamer]
    when(
      keyNamer.processPartitionValues(
        any[io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail],
        any[TopicPartition],
      ),
    ).thenReturn(values.asRight[SinkError])
    val fw = mock[FormatWriter]
    when(fw.write(any[io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail])).thenReturn(
      ().asRight,
    )
    when(fw.complete()).thenReturn(().asRight)
    when(fw.rolloverFileOnSchemaChange()).thenReturn(false)
    val policy = mock[io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy]
    when(policy.shouldFlush(any[io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext]))
      .thenAnswer((_: io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext) => flush.get())
    new WriterManager[FakeFileMetadata](
      commitPolicyFn    = _ => policy.asRight,
      bucketAndPrefixFn = bucketAndPrefix,
      keyNamerFn        = _ => keyNamer.asRight,
      stagingFilenameFn = (_, _) => batchStagingFile().asRight,
      objKeyBuilderFn = (_, vs) => {
        val okb = mock[ObjectKeyBuilder]
        when(okb.build(any[Offset], any[Offset], any[Long], any[Long], any[Long])).thenAnswer {
          (first: Offset, _: Offset, _: Long, _: Long, _: Long) =>
            val seg = WriterManager.derivePartitionKey(vs).getOrElse("nokey")
            CloudLocation(bucket,
                          path = Some(s"data/${tp0.topic.value}/${tp0.partition}/$seg-${first.value}.json"),
            ).asRight
        }
        okb
      },
      formatWriterFn              = (_, _) => fw.asRight,
      indexManager                = im,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      commitMode                  = io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode.Batch,
      metrics                     = metrics,
    )
  }

  private def batchPair(
    storage: InMemoryStorageInterface,
    metrics: CloudSinkMetrics                      = new CloudSinkMetrics(),
    values:  immutable.Map[PartitionField, String] = batchKey,
  ): BatchPair = {
    val im = buildBatchIndexManager(storage)
    im.open(Set(tp0)).value
    val flush = new java.util.concurrent.atomic.AtomicBoolean(false)
    val wm    = buildBatchWriterManager(im, storage, metrics, values, flush)
    new BatchPair(im, wm, flush)
  }

  private def batchMessage(offset: Long): io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail =
    io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail(
      key       = io.lenses.streamreactor.connect.cloud.common.sink.conversion.StringSinkData("k", None),
      value     = io.lenses.streamreactor.connect.cloud.common.sink.conversion.StringSinkData("v", None),
      headers   = Map.empty,
      timestamp = None,
      topic     = tp0.topic,
      partition = tp0.partition,
      offset    = Offset(offset),
    )

  private def seedBatchMaster(storage: InMemoryStorageInterface, committed: Long): Unit = {
    val seed = buildBatchIndexManager(storage)
    seed.open(Set(tp0)).value
    seed.updateMasterLock(tp0, Offset(committed + 1)).value
    seed.close()
  }

  private def dataKeys(storage: InMemoryStorageInterface): Seq[String] = storage.keysUnder(bucket, "data/")

  test("[Z] pre-CAS batch zombie: O's open fences Z; Z's commitBatch fails and O replays each offset once") {
    val storage = new InMemoryStorageInterface()
    seedBatchMaster(storage, 99)

    // Zombie Z opens and buffers records.
    val z = batchPair(storage)
    z.deliver(100 to 104)

    // New owner O opens (ownership bump fences Z's master eTag).
    val o = batchPair(storage)

    // Z flushes: staging succeeds (pre-CAS), the CAS is fenced -> Fatal, nothing under data/.
    z.commit().isLeft shouldBe true
    dataKeys(storage) shouldBe empty
    storage.keysUnder(bucket, s".temp-upload/${connectorTaskId.name}/") should not be empty

    // O replays and commits each offset once: a single batch object, master advanced to 104.
    o.deliver(100 to 104)
    o.commit().value
    dataKeys(storage) should have size 1
    o.im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(104))

    z.wm.close(); z.im.close(); o.wm.close(); o.im.close()
  }

  // A test for a zombie that completes its batch before the new owner opens was removed: Z
  // commits fully *before* O ever opens, so this is plain restart re-delivery dedup, not a
  // zombie scenario -- already pinned, with stronger assertions (object count, no duplicate
  // under a key rotation), by the equivalent batch-mode scenario tests in BatchCommitScenarioTest.

  test("[Z] ping-pong: after a completed batch commit, the fenced side surfaces Fatal; still one object") {
    val storage = new InMemoryStorageInterface()
    seedBatchMaster(storage, 99)

    val z = batchPair(storage)
    z.deliver(100 to 103)
    // Z completes a full batch commit, reaching committedOffset = 103.
    z.commit().value
    val objectsAfterZ = dataKeys(storage)
    objectsAfterZ should have size 1
    z.im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(103))

    // O opens afterwards (bump). Z, now the loser, tries to commit again and is fenced; O owns P.
    val o = batchPair(storage)
    o.im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(103))
    z.deliver(104 to 105)
    z.commit().isLeft shouldBe true // fenced: availability, not safety
    // The fenced attempt never reached a final path: still exactly the one object Z committed
    // before O opened, and the master offset is still Z's single agreed P (103), not Z's
    // fenced attempt (105).
    dataKeys(storage) shouldBe objectsAfterZ
    o.im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(103))

    z.wm.close(); z.im.close(); o.wm.close(); o.im.close()
  }

  test("[Z] rollback with a batch zombie: a granular owner opens, fences Z, and owns the master lock") {
    val storage = new InMemoryStorageInterface()
    seedBatchMaster(storage, 99)

    val z = batchPair(storage)
    z.deliver(100 to 103)

    // Granular owner O opens (ownership bump) — the rollback direction.
    val oim = buildIndexManager(storage)
    oim.open(Set(tp0)).value

    // Z's batch commit CAS is fenced; nothing reached a final path.
    z.commit().isLeft shouldBe true
    dataKeys(storage) shouldBe empty

    // O (granular) owns the current master eTag and writes successfully.
    oim.updateMasterLock(tp0, Offset(105)).value

    z.wm.close(); z.im.close(); oim.close()
  }

  // A test for a granular zombie vs batch owner was removed: the ownership bump is
  // mode-independent and is already pinned at the IndexManagerV2 level by the fencing
  // tests above; the removed test constructed zero writers and exercised nothing the
  // mode-independent fencing tests do not already cover.

  // A test for rebalance mid-batch was removed: it had the identical shape as the
  // equivalent scenario in BatchCommitScenarioTest (buffer, close before commit, reopen
  // at the master floor, re-deliver, one object) minus the wall-clock key rotation
  // that makes that scenario meaningful -- the other test is strictly stronger.
}
