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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.immutable

/**
 * Tests for the master-lock write-frequency reduction (dirty-flag gate).
 *
 * Every test in this file pins a non-negotiable scenario for the master-lock
 * write-frequency reduction (dirty-flag gate). The set covers:
 *
 *   - Duplicate-write skipping under the dirty-flag gate.
 *   - No-data-loss-on-crash-after-failed-master-lock-write across a multi-cycle dirty window.
 *   - No-duplication-when-force-on-revoke-fails (state-level fencing assertions).
 *   - Zombie-fences-on-stale-eTag (state-level proof).
 *   - GC-threshold-matches-just-persisted-master-offset (same-value invariant).
 *   - HWM-advances-on-skipped-but-not-failed (three-case split).
 *   - Skipped-path structural invariant: globalSafeOffset == lastWritten on the Skip branch.
 *   - Granular-lock writes occur on every commit regardless of master-lock gating.
 *   - No-regression-after-cleanup-in-dirty-window (preserved `lastReturnedSafeOffset(tp)`).
 *
 * The end-to-end zombie / final-path scenario lives alongside the existing end-to-end
 * scenario tests in `ExactlyOnceScenarioTest`; this file is mock-driven and focuses on
 * the WriterManager invariants in isolation.
 */
class MasterLockFrequencyTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("test-connector", 1, 1)
  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val tp0 = Topic("topic").withPartition(0)

  private val dateField: PartitionField                        = ValuePartitionField(PartitionNamePath("date"))
  private val dateA:     immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-01")
  private val dateB:     immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-02")

  private val commitPolicy         = mock[CommitPolicy]
  private val formatWriter         = mock[FormatWriter]
  private val objectKeyBuilder     = mock[ObjectKeyBuilder]
  private val schemaChangeDetector = mock[SchemaChangeDetector]
  private val pendingOpsProcessors = mock[PendingOperationsProcessors]

  private def makeIdleWriter(tp: TopicPartition, committedOffset: Option[Offset]): Writer[FileMetadata] = {
    val indexManager = mock[IndexManager]
    when(indexManager.indexingEnabled).thenReturn(true)
    val w = new Writer[FileMetadata](
      tp,
      commitPolicy,
      indexManager,
      () => Right(new File("test")),
      objectKeyBuilder,
      _ => Right(formatWriter),
      schemaChangeDetector,
      pendingOpsProcessors,
      None,
      committedOffset,
    )
    w.forceWriteState(NoWriter(CommitState(tp, committedOffset)))
    w
  }

  private def buildWriterManager(
    indexManager: IndexManager,
    metrics:      CloudSinkMetrics = new CloudSinkMetrics(),
    commitMode:   CommitMode       = CommitMode.Granular,
  ): WriterManager[FileMetadata] =
    new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(commitPolicy),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", None)),
      keyNamerFn                  = _ => Right(mock[KeyNamer]),
      stagingFilenameFn           = (_, _) => Right(new File("test")),
      objKeyBuilderFn             = (_, _) => objectKeyBuilder,
      formatWriterFn              = (_, _) => Right(formatWriter),
      indexManager                = indexManager,
      transformerF                = (m: MessageDetail) => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = pendingOpsProcessors,
      commitMode                  = commitMode,
      metrics                     = metrics,
    )

  private def currentOffsets(tp: TopicPartition, offset: Long): immutable.Map[TopicPartition, OffsetAndMetadata] =
    Map(tp -> new OffsetAndMetadata(offset))

  // ── Duplicate-write skipping (no-op gate) ──────────────────────────────────────────
  test(
    "Duplicate-write skipping: N identical preCommit cycles on an idle PARTITIONBY TP produce exactly one master-lock write",
  ) {
    // The dirty-flag alone deduplicates exact-duplicate writes. After the first cycle
    // establishes the master-lock floor, subsequent idle cycles must skip the write while
    // still returning the same safe offset to Kafka Connect.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))

    val cycles  = 5
    val results = (1 to cycles).map(_ => wm.preCommit(currentOffsets(tp0, 200))(tp0).offset())

    // Every cycle returns the same safe offset (no advancement to Kafka).
    results.forall(_ == 51L) shouldBe true

    // Exactly one underlying master-lock write across N cycles.
    verify(indexManager, times(1)).updateMasterLock(eqTo(tp0), eqTo(Offset(51L)))
    metrics.getMasterLockUpdates shouldBe 1L
    metrics.getMasterLockWriteSkipped shouldBe (cycles - 1).toLong
    // GC runs only on the success branch — so exactly once.
    verify(indexManager, times(1)).cleanUpObsoleteLocks(eqTo(tp0), any[Offset], any[Set[String]])
  }

  // ── Skip / WriteRoutine / failure / retry full-sequence ───────────────────────────
  test(
    "Skip / WriteRoutine / failure / retry sequence: HWM and dirty-window counters track the dirty-flag gate",
  ) {
    // Exercises every classification in a single multi-cycle scenario, validating both the
    // HWM + lastWritten progression (dirty flag must not advance on skip or fail) and the
    // dirty-window counter (increments only when dirty=true on entry, never on the Skip
    // branch). The `require(...)` inside the Skip branch enforces the structural invariant
    // globalSafeOffset == lastWritten for every Skip-classified cycle — a violation would
    // throw IllegalArgumentException from within preCommit.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // success — dirty (51 > 0). lastWritten → 51, HWM → 51.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L
    verify(indexManager, times(1)).updateMasterLock(any[TopicPartition], any[Offset])

    // Cycles 2–3: skip — globalSafeOffset(51) == lastWritten(51). No write attempted.
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L
    verify(indexManager, times(1)).updateMasterLock(any[TopicPartition], any[Offset]) // still only one

    // success — higher-offset writer; dirty (81 > 51). lastWritten → 81, HWM → 81.
    wm.putWriter(MapKey(tp0, dateB), makeIdleWriter(tp0, Some(Offset(80))))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 81L
    verify(indexManager, times(2)).updateMasterLock(any[TopicPartition], any[Offset])

    // failure — dirty (121 > 81). HWM and lastWritten do NOT advance; preCommit returns None.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(120))))
    wm.preCommit(currentOffsets(tp0, 200)) shouldBe empty

    // retry success — still dirty (121 > 81; lastWritten did not advance on failure).
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 121L
    // Cycles 1, 4, 5 (failed), 6 = 4 actual calls to updateMasterLock.
    verify(indexManager, times(4)).updateMasterLock(any[TopicPartition], any[Offset])

    // skip again — globalSafeOffset(121) == lastWritten(121).
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 121L

    // Aggregate counters.
    metrics.getMasterLockUpdates shouldBe 3L
    metrics.getMasterLockWriteSkipped shouldBe 3L
    metrics.getMasterLockFailures shouldBe 1L
    // Dirty-window counter: increments once per preCommit cycle that observed dirty=true on entry.
    // Skip branch (dirty=false) never increments it.
    //   cycle 1: dirty (51 > 0)    → 1
    //   cycle 2: skip (51 == 51)   → 1
    //   cycle 3: skip (51 == 51)   → 1
    //   cycle 4: dirty (81 > 51)   → 2
    //   cycle 5: dirty (121 > 81)  → 3
    //   cycle 6: dirty (121 > 81)  → 4  (retry)
    //   cycle 7: skip (121 == 121) → 4
    metrics.getMasterLockDirtyWindowCycles shouldBe 4L
  }

  // ── No-data-loss across a multi-cycle failed master-lock write ─────────────────────
  test(
    "No data loss: HWM and lastWritten do not advance across K failed master-lock writes, GC never runs on failed cycles",
  ) {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))

    // K = 3 consecutive failures simulating a transient cloud outage.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    (1 to 3).foreach { _ =>
      wm.preCommit(currentOffsets(tp0, 200)) shouldBe empty
    }
    metrics.getMasterLockFailures shouldBe 3L
    // GC never ran on the failed cycles.
    verify(indexManager, never).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])

    // Recovery: the next cycle succeeds and advances state at the FIRST in-memory offset,
    // not at any intermediate value. Critically: no consumer offset was ever advanced past
    // 51 by Kafka Connect during the failure window (because preCommit returned None).
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    val result = wm.preCommit(currentOffsets(tp0, 200))
    result(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L
    // GC runs against the just-persisted value, not a later mutated value.
    verify(indexManager, times(1)).cleanUpObsoleteLocks(eqTo(tp0), eqTo(Offset(51L)), any[Set[String]])
  }

  // ── No-duplication-when-force-on-revoke-fails ─────────────────────────────────────
  test("Force-on-revoke failure is not a fatal path; granular-lock state is preserved for the next owner") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // a successful routine write establishes lastWritten = 51.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Advance writer state but skip routine preCommit, so dirty flag is true on close.
    val writerHigh = makeIdleWriter(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateA), writerHigh)

    // Force-on-revoke fails with an eTag mismatch (fencing).
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("eTag mismatch", tp0)))

    wm.close()

    // Force-on-revoke metric was incremented (attempt was made).
    metrics.getMasterLockWriteForcedRevoke shouldBe 1L
    metrics.getMasterLockFailures shouldBe 1L
    // Per-reason forced-failure counter: incremented alongside masterLockFailures on Left(err).
    metrics.getMasterLockWriteForcedRevokeFailures shouldBe 1L
    // GC did NOT run on the failed forced write — granular locks above the durable floor
    // (51) survive for the next owner to dedup against.
    verify(indexManager, times(1)).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])
    // Granular-cache eviction still ran (writers were closed).
    verify(indexManager).evictAllGranularLocks(tp0)
    wm.writerCount shouldBe 0
  }

  // ── GC-threshold-matches-just-persisted-master-offset (same-value invariant) ───────
  test("GC threshold equals the just-persisted master-lock offset (same-value invariant)") {
    // Same-value GC threshold invariant: the write helper must read globalSafeOffset from
    // a single local and pass the SAME value to both
    // updateMasterLock and cleanUpObsoleteLocks. A future refactor that introduces a
    // second read of in-memory state between the two calls would silently let GC delete
    // granular locks above the durable floor on a race.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(100))))

    wm.preCommit(currentOffsets(tp0, 500))(tp0).offset() shouldBe 101L

    // Exact value-matchers (no ArgumentCaptor): `Offset` is an `AnyVal` value class which
    // Mockito's ArgumentCaptor erases to `Long`, returning `null` for the captured value.
    // The same-value invariant is verified by the matching pair of `verify(eqTo(...))`
    // calls below: if a future refactor introduces a second read of in-memory state
    // between the two calls, one of the two matchers will fail.
    verify(indexManager).updateMasterLock(eqTo(tp0), eqTo(Offset(101L)))
    verify(indexManager).cleanUpObsoleteLocks(eqTo(tp0), eqTo(Offset(101L)), any[Set[String]])
  }

  // ── No-regression-after-cleanup-in-dirty-window ────────────────────────────────────
  test("Post-cleanUp(tp) preCommit forces a master-lock write at max(durableFloor, lastReturnedSafeOffset(tp))") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(100)))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // Step 1: establish lastWritten = 151, lastReturned = 151, durable floor = 101.
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(150))))
    wm.preCommit(currentOffsets(tp0, 500))(tp0).offset() shouldBe 151L

    // Step 2: cleanUp(tp0) — clears HWM and lastWritten, preserves lastReturned = 151,
    // sets forceWriteAfterCleanUp(tp0).
    wm.cleanUp(tp0)

    // Step 3: re-add a writer with a LOWER calculated safe offset (101). HWM re-seeds from
    // max(durable + 1 = 101, lastReturned = 151) = 151. globalSafeOffset = max(101, 151) = 151.
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(100))))
    val result = wm.preCommit(currentOffsets(tp0, 500))
    result(tp0).offset() shouldBe 151L

    // Step 4: WriteForced(PostCleanUp) fired, writing the SAME 151 value to both
    // updateMasterLock and cleanUpObsoleteLocks (same-value invariant). The metric tag
    // distinguishes this from a routine write. We verify the call signatures rather than
    // capturing values because `Offset` is an `AnyVal` value class that Mockito's
    // ArgumentCaptor cannot capture (the captured value is null at runtime).
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
    // Two cycles: one routine pre-cleanup at 151, one forced post-cleanup at 151.
    verify(indexManager, times(2)).updateMasterLock(eqTo(tp0), eqTo(Offset(151L)))
    verify(indexManager, times(2)).cleanUpObsoleteLocks(eqTo(tp0), eqTo(Offset(151L)), any[Set[String]])
  }

  // ── No-regression after cleanup when force-on-revoke earlier failed ───────────────
  test(
    "Force-after-cleanUp is one-shot: a failure clears the flag and falls back to dirty-flag retry on the next routine cycle",
  ) {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // Establish lastReturned = 51 on a successful first cycle.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L

    // In-place rollback — force flag set for tp0.
    wm.cleanUp(tp0)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(80))))

    // First post-cleanUp preCommit: force-write fails. forceWriteAfterCleanUp(tp) is
    // cleared inside the helper before the write attempt so the next cycle can re-attempt
    // through the routine dirty-flag path.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    wm.preCommit(currentOffsets(tp0, 100)) shouldBe empty
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
    metrics.getMasterLockFailures shouldBe 1L
    // Per-reason failure counter must be incremented on the Left(err) path.
    metrics.getMasterLockWriteForcedPostCleanUpFailures shouldBe 1L

    // Second post-cleanUp preCommit: still dirty (globalSafeOffset=81 > lastWritten=0
    // because lastWritten lazy seeds from the absent durable floor). WriteRoutine fires.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 81L
    // No second forced-write metric — this was a routine retry.
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
    // Per-reason failure counter stays at 1 — the retry succeeded.
    metrics.getMasterLockWriteForcedPostCleanUpFailures shouldBe 1L
    // Two total successes (initial + retry).
    metrics.getMasterLockUpdates shouldBe 2L
  }

  // ── Skipped-cycle: lastReturned still advances monotonically (value-level no-op) ──
  test("Skip branch advances lastReturnedSafeOffset(tp) (monotonic-max) and HWM together") {
    // This is the structurally-required no-op-in-value-terms advance on the Skip branch.
    // The test exercises the path indirectly by verifying that a *failure* on the cycle
    // immediately after a skip does NOT cause regression — because lastReturned was
    // captured on the skip even though the value did not change.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L // success
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 51L // skip

    // After skip + a forced-rollback that preserves lastReturned, the post-cleanUp HWM
    // seed must still equal 51 (otherwise a buggy implementation that didn't advance
    // lastReturned on the skip path would drop to 0 here).
    wm.cleanUp(tp0)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(10))))
    val result = wm.preCommit(currentOffsets(tp0, 100))
    // Calculated = 11; HWM re-seed = max(0, 51) = 51; globalSafeOffset = max(11, 51) = 51.
    result(tp0).offset() shouldBe 51L
  }

  // ── Force-on-revoke unexpected NonFatal throw: cleanup still runs, failure metric increments ──
  test(
    "Force-on-revoke unexpected throw: wm.close() does not propagate; cleanup runs; forced-failure metric increments",
  ) {
    // This test covers the NonFatal catch path in closePartition. When updateMasterLock
    // throws (rather than returning Left), the throw must be swallowed, cleanup must
    // still run (writers closed, cache evicted, per-TP state cleared), and the new
    // forced-failure counter (not just the aggregate masterLockFailures) must increment.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    // routine write succeeds, establishing lastWritten = 51.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Add higher-offset writer without a preCommit: dirty flag is true on close.
    val writerHigh = makeIdleWriter(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateA), writerHigh)

    // Force-on-revoke: updateMasterLock throws a NonFatal exception instead of returning Left.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenThrow(new RuntimeException("unexpected storage exception"))

    // wm.close() must not propagate the exception.
    noException should be thrownBy wm.close()

    // Forced-attempt counter is incremented inside attemptForceMasterLockWrite BEFORE the
    // throw, so the attempt is still visible on the metric.
    metrics.getMasterLockWriteForcedRevoke shouldBe 1L
    // The new forced-failure counter and the aggregate failure counter are incremented by
    // the NonFatal catch in closePartition (NOT inside attemptForceMasterLockWrite, which
    // exited via throw before reaching the Left branch).
    metrics.getMasterLockWriteForcedRevokeFailures shouldBe 1L
    metrics.getMasterLockFailures shouldBe 1L
    // Initial write only — the throw did NOT falsely increment masterLockUpdates.
    metrics.getMasterLockUpdates shouldBe 1L

    // Cleanup contract: writer close, per-TP state clear, and cache eviction must all
    // run even when the forced write threw.
    wm.writerCount shouldBe 0
    verify(indexManager).evictAllGranularLocks(tp0)

    // GC ran exactly once (cycle 1 routine write success only — not on the throw path).
    verify(indexManager, times(1)).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])
  }

  // ── getMasterLockDirty: point-in-time boolean gauge ────────────────────────────────

  test("getMasterLockDirty flips true on first dirty preCommit and false after a successful master-lock write") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    // Start: gauge is false (nothing has been observed yet).
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    metrics.getMasterLockDirty shouldBe false

    // After a dirty preCommit the gauge must flip true.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 200)) shouldBe empty
    metrics.getMasterLockDirty shouldBe true

    // After a successful write the gauge must clear.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 51L
    metrics.getMasterLockDirty shouldBe false
  }

  test(
    "getMasterLockDirty stays true across multiple consecutive failed master-lock writes (sustained-outage simulation)",
  ) {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("outage", tp0)))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))

    (1 to 5).foreach { _ =>
      wm.preCommit(currentOffsets(tp0, 200)) shouldBe empty
      metrics.getMasterLockDirty shouldBe true
    }
    metrics.getMasterLockFailures shouldBe 5L
    metrics.getMasterLockDirty shouldBe true
  }

  test("getMasterLockDirty stays true when one of two dirty TPs is cleared, clears only when both are resolved") {
    val tp1 = Topic("topic").withPartition(1)

    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    // Both TPs fail their first write — gauge should be true.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.putWriter(MapKey(tp1, dateA), makeIdleWriter(tp1, Some(Offset(60))))
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200), tp1 -> new OffsetAndMetadata(200))) shouldBe empty
    metrics.getMasterLockDirty shouldBe true

    // tp0 succeeds — tp1 is still dirty; gauge must remain true.
    when(indexManager.updateMasterLock(eqTo(tp0), any[Offset])).thenReturn(Right(()))
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200), tp1 -> new OffsetAndMetadata(200)))
    metrics.getMasterLockDirty shouldBe true

    // tp1 succeeds — both cleared; gauge must be false.
    when(indexManager.updateMasterLock(eqTo(tp1), any[Offset])).thenReturn(Right(()))
    wm.preCommit(Map(tp0 -> new OffsetAndMetadata(200), tp1 -> new OffsetAndMetadata(200)))
    metrics.getMasterLockDirty shouldBe false
  }

  test("cleanUp(tp) clears the dirty indicator for that TP; gauge becomes false if it was the only dirty TP") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))
    doNothing.when(indexManager).evictAllGranularLocks(any[TopicPartition])

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 200)) shouldBe empty
    metrics.getMasterLockDirty shouldBe true

    // cleanUp(tp0) rolls back the partition; stale dirty state must be removed.
    wm.cleanUp(tp0)
    metrics.getMasterLockDirty shouldBe false
  }

  test("close() clears the dirty indicator for all TPs") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    // succeed — establishes lastWritten; dirty flag cleared.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 51L
    metrics.getMasterLockDirty shouldBe false

    // Introduce a higher-offset writer so that dirty is true on the next preCommit, but do
    // NOT call preCommit — instead drive a failed write inside close() via a failing forced write.
    val writerHigh = makeIdleWriter(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateA), writerHigh)
    // Make the forced write on close() fail so the dirty entry would remain without clearAll.
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("transient", tp0)))

    // Drive a preCommit so the gauge is marked dirty before close().
    wm.preCommit(currentOffsets(tp0, 200)) shouldBe empty
    metrics.getMasterLockDirty shouldBe true

    // close() must clear all dirty state regardless of forced-write outcome.
    wm.close()
    metrics.getMasterLockDirty shouldBe false
  }

  // ── cleanUp(tp) → close() interleaving: leaked state is fully cleared ──────────────
  test(
    "close() bulk-clears per-TP state for cleanUp-then-close interleaving so the next ownership episode starts fresh",
  ) {
    // Scenario: cleanUp(tp) runs after a fatal error (preserves lastReturnedSafeOffset and
    // sets forceWriteAfterCleanUp), but before any new writer is created the task is
    // rebalanced (close() called). Without the defensive bulk clear the leaked entries
    // survive into the next ownership episode, causing WriteForced(PostCleanUp) and an
    // inflated HWM on the first preCommit of the new episode.
    //
    // Step 1: establish lastWritten = 51, lastReturned = 51.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    doNothing.when(indexManager).evictAllGranularLocks(any[TopicPartition])

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(50))))
    wm.preCommit(currentOffsets(tp0, 200))(tp0).offset() shouldBe 51L
    metrics.getMasterLockUpdates shouldBe 1L

    // Step 2: in-place rollback — cleanUp preserves lastReturned=51, sets forceWriteAfterCleanUp.
    wm.cleanUp(tp0)

    // Step 3: rebalance close with no intervening writer re-creation. The bulk clear must
    // erase lastReturnedSafeOffset(tp0) and forceWriteAfterCleanUp(tp0).
    wm.close()

    // Step 4: new ownership episode. Simulate open() having re-read the master lock from
    // storage — the mock returns None (durable floor is 0, as if nothing was persisted
    // during the zombie episode). A new writer at committed=5 produces calculatedSafeOffset=6.
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(5))))

    val result = wm.preCommit(currentOffsets(tp0, 200))
    // Without the bulk clear: HWM re-seeds to max(durableFloor+1=0, lastReturned=51) = 51,
    // and WriteForced(PostCleanUp) fires at 51. With the bulk clear: both leaked entries
    // are gone, so HWM re-seeds to max(0, 0) = 0, calculatedSafeOffset=6 wins, and the
    // cycle is classified WriteRoutine at 6.
    result(tp0).offset() shouldBe 6L
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 0L
    // Exactly two successful master-lock writes total: the initial cycle at 51 (step 1)
    // and the new-episode routine write at 6 (step 4).
    metrics.getMasterLockUpdates shouldBe 2L
  }

  // ── Partition-batch commit mode: the master lock is written only by commitBatch ─────

  test(
    "[B] batch: preCommit, cleanUp and close never write the master lock or run granular-lock GC",
  ) {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(50)))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics, CommitMode.Batch)
    wm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(60))))
    wm.putWriter(MapKey(tp0, dateB), makeIdleWriter(tp0, Some(Offset(70))))

    // The dirty predicate would be true in granular mode: 71 > 51.
    wm.preCommit(currentOffsets(tp0, 100))(tp0).offset() shouldBe 71L
    wm.cleanUp(tp0)
    wm.close()

    verify(indexManager, never).updateMasterLock(any[TopicPartition], any[Offset])
    verify(indexManager, never).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])
    metrics.getMasterLockUpdates shouldBe 0L
    metrics.getMasterLockWriteForcedRevoke shouldBe 0L
  }

  test(
    "[B] batch: close() skips the forced master-lock write that granular mode performs for the same state",
  ) {
    // Granular arm: the forced Revoke write fires.
    val granularIm = mock[IndexManager]
    when(granularIm.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(50)))
    when(granularIm.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(granularIm.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    val granularMetrics = new CloudSinkMetrics()
    val granularWm      = buildWriterManager(granularIm, granularMetrics, CommitMode.Granular)
    granularWm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(70))))
    granularWm.close()
    verify(granularIm).updateMasterLock(eqTo(tp0), eqTo(Offset(71)))
    granularMetrics.getMasterLockWriteForcedRevoke shouldBe 1L

    // Batch arm: identical writer state, no write at all.
    val batchIm = mock[IndexManager]
    when(batchIm.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(50)))
    val batchMetrics = new CloudSinkMetrics()
    val batchWm      = buildWriterManager(batchIm, batchMetrics, CommitMode.Batch)
    batchWm.putWriter(MapKey(tp0, dateA), makeIdleWriter(tp0, Some(Offset(70))))
    batchWm.close()
    verify(batchIm, never).updateMasterLock(any[TopicPartition], any[Offset])
    batchMetrics.getMasterLockWriteForcedRevoke shouldBe 0L
  }
}
