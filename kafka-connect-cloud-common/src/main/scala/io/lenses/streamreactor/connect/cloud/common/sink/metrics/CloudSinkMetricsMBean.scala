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
package io.lenses.streamreactor.connect.cloud.common.sink.metrics

import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder

/**
 * Reason tag for a forced master-lock write, recorded on the
 * `masterLockWriteForced*` counters.
 */
sealed trait ForcedWriteReason
object ForcedWriteReason {
  case object Revoke      extends ForcedWriteReason
  case object Stop        extends ForcedWriteReason
  case object PostCleanUp extends ForcedWriteReason
}

trait CloudSinkMetricsMBean {

  // --- Writer map ---

  /**
   * Current number of Writer instances in the writers map. Only active (non-idle) writers
   * remain in the map; idle writers are eagerly evicted when a new writer is created.
   */
  def getWriterCount: Int

  /**
   * Cumulative count of idle (NoWriter-state) writers evicted. Idle writers are eagerly
   * evicted whenever a new writer is created, keeping the map lean and allowing granular
   * lock GC to proceed sooner.
   */
  def getIdleWriterEvictions: Long

  // --- Granular lock cache ---

  /**
   * Current number of entries in the unbounded ConcurrentHashMap granular lock cache.
   * Each entry holds an eTag and committed offset for one partition key. A large value
   * signals high cardinality and proportional heap usage.
   */
  def getGranularCacheSize: Int

  /**
   * Cumulative cache hits when looking up a granular lock (eTag + offset). A high
   * hit ratio means writers are reusing cached eTags efficiently, avoiding cloud
   * storage reads on every commit.
   */
  def getGranularCacheHits: Long

  /**
   * Cumulative cache misses, each triggering a cloud storage GET to load the granular
   * lock. A spike indicates many new or re-created partition keys (e.g. after idle-writer
   * eviction or a rebalance).
   */
  def getGranularCacheMisses: Long

  // --- GC ---

  /**
   * Current number of obsolete granular lock file paths waiting in the gcQueue for
   * background deletion. A persistently high value suggests the drain interval
   * (gc.interval.seconds) or batch size (gc.batch.size) needs tuning.
   */
  def getGcQueueDepth: Int

  /**
   * Cumulative number of obsolete granular lock paths enqueued for deletion during
   * preCommit. Shows how much GC work is being generated over the task's lifetime.
   */
  def getGcLocksEnqueued: Long

  /**
   * Cumulative number of granular lock files actually deleted by the background GC drain.
   * Compare with [[getGcLocksEnqueued]] to verify GC is keeping up; a growing gap indicates
   * the drain cannot keep pace with enqueue rate.
   */
  def getGcLocksDeleted: Long

  /**
   * Cumulative count of enqueued locks skipped at drain time because a new writer reclaimed
   * the partition key between enqueue and drain. This is expected race-condition protection
   * and indicates healthy operation, not a problem.
   */
  def getGcLocksSkippedReclaimed: Long

  /**
   * Cumulative count of enqueued locks skipped because the topic-partition was revoked
   * (rebalanced away) before the background drain ran. Expected during rebalances.
   */
  def getGcLocksSkippedRevoked: Long

  /**
   * Cumulative failed batch-delete attempts (logged at WARN). Orphaned lock files are
   * harmless and will be re-enqueued, but a sustained count may indicate storage
   * permission or connectivity issues.
   */
  def getGcDeleteFailures: Long

  /**
   * Cumulative retried delete operations across all GC drain cycles. Elevated values
   * suggest transient cloud storage errors (throttling, timeouts).
   */
  def getGcDeleteRetries: Long

  // --- Master lock ---

  /**
   * Cumulative successful eTag-conditional master-lock writes (routine and forced combined).
   * Forced writes additionally increment the per-reason `masterLockWriteForced*` counter;
   * do NOT sum `masterLockUpdates + masterLockWriteForced*` — that double-counts forced
   * successes. For decomposition formulas see docs/datalake-exactly-once-partitionby.md
   * ("Master-Lock Write-Frequency Reduction and Metrics").
   */
  def getMasterLockUpdates: Long

  /**
   * Cumulative failed master lock writes. A failure means preCommit returned no offset for
   * the partition, so Kafka does not advance the consumer offset. Persistent failures may
   * indicate a zombie task holding a conflicting eTag or cloud storage issues.
   */
  def getMasterLockFailures: Long

  /**
   * Cumulative `preCommit` cycles where the dirty-flag gate suppressed the master-lock write
   * (`globalSafeOffset` had not advanced). The ratio
   * `masterLockWriteSkipped / (masterLockWriteSkipped + masterLockUpdates + masterLockFailures)`
   * measures the cost saving.
   */
  def getMasterLockWriteSkipped: Long

  /** Cumulative `WriteForced(Revoke)` attempts (partition revocation / rebalance). Records attempts, not successes. */
  def getMasterLockWriteForcedRevoke: Long

  /** Cumulative `WriteForced(Stop)` attempts (task stop / shutdown). Records attempts, not successes. */
  def getMasterLockWriteForcedStop: Long

  /**
   * Cumulative `WriteForced(PostCleanUp)` attempts: the first `preCommit` after a `cleanUp(tp)`
   * in-place rollback forces a write to persist `globalSafeOffset`. Records attempts, not
   * successes. See docs/datalake-exactly-once-partitionby.md ("`lastReturnedSafeOffset` Role by Mode").
   */
  def getMasterLockWriteForcedPostCleanUp: Long

  /**
   * Cumulative forced `WriteForced(Revoke)` write failures — a strict subset of
   * [[getMasterLockFailures]]. Incremented for both an expected `Left(err)` return
   * from `updateMasterLock` and an unexpected `NonFatal` throw on the forced path.
   */
  def getMasterLockWriteForcedRevokeFailures: Long

  /**
   * Cumulative forced `WriteForced(Stop)` write failures — a strict subset of
   * [[getMasterLockFailures]]. Incremented for both an expected `Left(err)` return
   * from `updateMasterLock` and an unexpected `NonFatal` throw on the forced path.
   */
  def getMasterLockWriteForcedStopFailures: Long

  /**
   * Cumulative forced `WriteForced(PostCleanUp)` write failures — a strict subset of
   * [[getMasterLockFailures]]. Incremented for both an expected `Left(err)` return
   * from `updateMasterLock` and an unexpected `NonFatal` throw on the forced path.
   */
  def getMasterLockWriteForcedPostCleanUpFailures: Long

  /**
   * Cumulative `preCommit` cycles where the dirty flag was true on entry. At most one per
   * `globalSafeOffset` advance; sustained elevation indicates `updateMasterLock` failures
   * have widened the dirty window beyond a single cycle.
   */
  def getMasterLockDirtyWindowCycles: Long

  /**
   * Point-in-time indicator: `true` iff at least one topic-partition was observed
   * `dirty` (`globalSafeOffset > lastWrittenMaster`) at its most recent `preCommit`
   * and has not yet had a successful master-lock write or been cleaned up since.
   * Pairs with [[getMasterLockDirtyWindowCycles]]: the counter is a rate signal,
   * this gauge is the stuck signal — non-zero across many scrapes means the dirty
   * window is not closing (e.g. sustained cloud outage, persistent eTag conflict).
   */
  def getMasterLockDirty: Boolean

  // --- Orphan sweep ---

  /**
   * Cumulative number of orphan-sweep cycles that have executed. Useful for confirming
   * the sweep timer (gc.sweep.interval.seconds) is firing at the expected cadence.
   */
  def getSweepRuns: Long

  /**
   * Cumulative orphaned granular lock files discovered by the sweep and enqueued for
   * deletion. Shows how much stale state is accumulating from prior task runs with
   * different partition-key distributions.
   */
  def getSweepOrphansEnqueued: Long

  /**
   * Number of cloud storage GET requests consumed in the most recent sweep cycle (reset
   * each cycle). Compare against gc.sweep.max.reads to gauge budget utilisation; if
   * consistently at the cap, increase the budget or shorten the sweep interval.
   */
  def getSweepGetBudgetUsed: Int

  /**
   * Last-call snapshot of `firstBufferedOffsets.size` recorded inside
   * [[io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager.preCommit]] —
   * the number of writers currently holding the safe-offset barrier (i.e. writers in
   * Writing/Uploading state that have buffered but uncommitted records). With selective
   * commit, `globalSafeOffset` is bounded by the slowest writer's `firstBufferedOffset`;
   * this gauge surfaces that hold-back to operators. A persistently non-zero value with
   * stagnant Kafka offsets indicates a low-throughput partition key whose `flush.interval`
   * needs tuning (see the Operational Constraints section of `datalake-exactly-once-partitionby`).
   */
  def getSafeOffsetBarrierWriters: Int

  // =========================================================================
  // A. Ingest throughput
  // =========================================================================

  /** Cumulative records received across all `put` invocations. */
  def getRecordsReceivedTotal: Long

  /** Cumulative records that passed null-value filter and were forwarded to `writerManager.write`. */
  def getRecordsWrittenTotal: Long

  /** Cumulative records skipped because the value was null and `skipNullValues` is enabled. */
  def getNullRecordsSkippedTotal: Long

  /** Wall-clock epoch (ms) of the last completed `put` invocation. 0 if `put` has never run. */
  def getLastPutEpochMillis: Long

  // put() latency timer components
  def getPutTimerCount:     Long
  def getPutTimerSumMillis: Long
  def getPutTimerMaxMillis: Long

  // =========================================================================
  // B. File / commit lifecycle
  // =========================================================================

  /** Cumulative number of new files opened (NoWriter → Writing transitions). */
  def getFilesOpenedTotal: Long

  /** Cumulative successful file commits (upload + index update completed). */
  def getFilesCommittedTotal: Long

  /**
   * Cumulative failed file commit attempts.  A single file may increment this counter
   * multiple times if the transient-upload path retries before succeeding.
   */
  def getFilesFailedTotal: Long

  /** Cumulative bytes written to cloud storage across all committed files. */
  def getBytesWrittenTotal: Long

  /** Cumulative records included in all committed files. */
  def getRecordsCommittedTotal: Long

  // commit() latency timer components (seal → upload → index update)
  def getCommitTimerCount:     Long
  def getCommitTimerSumMillis: Long
  def getCommitTimerMaxMillis: Long

  // =========================================================================
  // C. Per-cloud storage SDK call timings & errors
  // =========================================================================

  // uploadFile — the hot path write to S3/GCS/Azure
  def getStorageUploadTimerCount:     Long
  def getStorageUploadTimerSumMillis: Long
  def getStorageUploadTimerMaxMillis: Long
  def getStorageUploadErrorsTotal:    Long

  // mvFile — copy-then-delete in indexed-mode commit and master-lock path
  def getStorageCopyTimerCount:     Long
  def getStorageCopyTimerSumMillis: Long
  def getStorageCopyTimerMaxMillis: Long
  def getStorageCopyErrorsTotal:    Long

  // deleteFile / deleteFiles — GC drain and indexed-mode temp-file deletion (error-only; latency not exposed)
  def getStorageDeleteErrorsTotal: Long

  // getBlobAsStringAndEtag / getBlobAsObject — granular/master lock reads and sweep loads
  def getStorageGetTimerCount:     Long
  def getStorageGetTimerSumMillis: Long
  def getStorageGetTimerMaxMillis: Long
  def getStorageGetErrorsTotal:    Long

  // listKeysRecursive / listFileMetaRecursive — orphan sweep and index manager listings (error-only; latency not exposed)
  def getStorageListErrorsTotal: Long

  // =========================================================================
  // D. Pending-operation retries & error classification
  // =========================================================================

  /**
   * Cumulative transient upload failures that will be retried by the next `recommitPending`
   * call.  Each increment corresponds to one NonFatalCloudSinkError raised from the
   * transient-upload path in `PendingOperationsProcessors`.
   */
  def getPendingOperationRetriesTotal: Long

  /**
   * Cumulative in-process retry attempts for transient failures on the commit-chain
   * Copy (`mvFile`) and Delete (`deleteFile`) steps inside `RetryingStorageInterface`.
   * Each increment corresponds to one retry attempt (not the initial attempt).
   *
   * Kept distinct from [[getPendingOperationRetriesTotal]] (upload-only) so that
   * Copy/Delete retries can be tracked separately on dashboards and alerts.
   */
  def getCommitRetriesTotal: Long

  /** Cumulative sink errors classified as Fatal (task fails immediately). */
  def getSinkErrorsFatalTotal: Long

  /** Cumulative sink errors classified as Retriable (Connect re-delivers the batch). */
  def getSinkErrorsRetriableTotal: Long

  /** Cumulative sink errors classified as NonFatal (Connect may swallow depending on policy). */
  def getSinkErrorsNonFatalTotal: Long

  // =========================================================================
  // E. Schema / skip / seek diagnostics
  // =========================================================================

  /**
   * Cumulative schema-rollover events — a new schema was detected that is incompatible
   * with the current file format, triggering a full flush of all writers for that
   * topic-partition before the new schema can be written.
   */
  def getSchemaRolloversTotal: Long

  /**
   * Cumulative duplicate records skipped because the offset had already been committed
   * (dedup by the index manager seek path).
   */
  def getDuplicateRecordsSkippedTotal: Long

  /**
   * Cumulative topic-partition assignments for which `context.offset(...)` was called
   * during `open`, seeking the consumer back to recover from a restart.
   */
  def getSeekOnOpenAppliedTotal: Long

  /**
   * Cumulative `close(partitions)` calls from Kafka Connect (one per rebalance event).
   * A persistently high rate indicates an unstable consumer group.
   */
  def getRebalanceClosesTotal: Long

  /**
   * `commit.mode=batch` only: cumulative partition-batch commits that reached the master-lock CAS
   * and completed their copy chain.
   */
  def getBatchCommitsTotal: Long

  /**
   * `commit.mode=batch` only: cumulative files moved to a final path by a successful batch commit.
   * Divided by [[getBatchCommitsTotal]] this is the average batch width (writers per Kafka partition).
   */
  def getBatchCommitFilesTotal: Long

  /**
   * `commit.mode=batch` only: cumulative batch commits that failed anywhere between staging and the
   * end of the copy chain. A sustained non-zero rate means batches are not making progress.
   */
  def getBatchCommitFailuresTotal: Long

  /**
   * `commit.mode=batch` only: cumulative topic-partitions whose legacy granular locks were purged
   * after a granular -> batch migration once the batch watermark caught up to them.
   */
  def getLegacyLocksPurgedTotal: Long

  // =========================================================================
  // F. Current state gauges
  // =========================================================================

  /**
   * Current number of writers in the `Uploading` state — i.e. files whose data has been
   * sealed locally and whose cloud upload is in progress or pending retry.
   * A persistently elevated value signals slow cloud storage or a stalled retry loop.
   */
  def getInFlightUploads: Int

  /**
   * Milliseconds elapsed since the last successful file commit.
   * Returns 0 before the first commit.  The primary liveness signal for an alert on
   * "task stuck" — pair with `getFilesCommittedTotal` rate to distinguish "busy but slow"
   * from "completely stalled".
   */
  def getMillisSinceLastCommit: Long
}

class CloudSinkMetrics() extends CloudSinkMetricsMBean {

  // --- existing fields ---

  private val writerCount         = new AtomicInteger(0)
  private val idleWriterEvictions = new LongAdder()

  private val granularCacheSize   = new AtomicInteger(0)
  private val granularCacheHits   = new LongAdder()
  private val granularCacheMisses = new LongAdder()

  private val gcQueueDepth            = new AtomicInteger(0)
  private val gcLocksEnqueued         = new LongAdder()
  private val gcLocksDeleted          = new LongAdder()
  private val gcLocksSkippedReclaimed = new LongAdder()
  private val gcLocksSkippedRevoked   = new LongAdder()
  private val gcDeleteFailures        = new LongAdder()
  private val gcDeleteRetries         = new LongAdder()

  private val masterLockUpdates                        = new LongAdder()
  private val masterLockFailures                       = new LongAdder()
  private val masterLockWriteSkipped                   = new LongAdder()
  private val masterLockWriteForcedRevoke              = new LongAdder()
  private val masterLockWriteForcedStop                = new LongAdder()
  private val masterLockWriteForcedPostCleanUp         = new LongAdder()
  private val masterLockWriteForcedRevokeFailures      = new LongAdder()
  private val masterLockWriteForcedStopFailures        = new LongAdder()
  private val masterLockWriteForcedPostCleanUpFailures = new LongAdder()
  private val masterLockDirtyWindowCycles              = new LongAdder()
  private val dirtyTopicPartitions =
    java.util.concurrent.ConcurrentHashMap.newKeySet[TopicPartition]()

  private val sweepRuns            = new LongAdder()
  private val sweepOrphansEnqueued = new LongAdder()
  private val sweepGetBudgetUsed   = new AtomicInteger(0)

  private val safeOffsetBarrierWriters = new AtomicInteger(0)

  // --- A. Ingest throughput ---

  private val recordsReceivedTotal    = new LongAdder()
  private val recordsWrittenTotal     = new LongAdder()
  private val nullRecordsSkippedTotal = new LongAdder()
  private val lastPutEpochMillis      = new AtomicLong(0L)
  private val putTimer                = new OpTimer()

  // --- B. File / commit lifecycle ---

  private val filesOpenedTotal      = new LongAdder()
  private val filesCommittedTotal   = new LongAdder()
  private val filesFailedTotal      = new LongAdder()
  private val bytesWrittenTotal     = new LongAdder()
  private val recordsCommittedTotal = new LongAdder()
  private val commitTimer           = new OpTimer()
  private val lastCommitEpochMillis = new AtomicLong(0L)

  // --- C. Storage SDK timers ---

  private val storageUploadTimer  = new OpTimer()
  private val storageUploadErrors = new LongAdder()
  private val storageCopyTimer    = new OpTimer()
  private val storageCopyErrors   = new LongAdder()
  private val storageDeleteErrors = new LongAdder()
  private val storageGetTimer     = new OpTimer()
  private val storageGetErrors    = new LongAdder()
  private val storageListErrors   = new LongAdder()

  // --- D. Retries & error classification ---

  private val pendingOperationRetriesTotal = new LongAdder()
  private val commitRetriesTotal           = new LongAdder()
  private val sinkErrorsFatalTotal         = new LongAdder()
  private val sinkErrorsRetriableTotal     = new LongAdder()
  private val sinkErrorsNonFatalTotal      = new LongAdder()

  // --- E. Schema / skip / seek diagnostics ---

  private val schemaRolloversTotal         = new LongAdder()
  private val duplicateRecordsSkippedTotal = new LongAdder()
  private val seekOnOpenAppliedTotal       = new LongAdder()
  private val rebalanceClosesTotal         = new LongAdder()
  private val batchCommitsTotal            = new LongAdder()
  private val batchCommitFilesTotal        = new LongAdder()
  private val batchCommitFailuresTotal     = new LongAdder()
  private val legacyLocksPurgedTotal       = new LongAdder()

  // --- F. Current state gauges ---

  private val inFlightUploads = new AtomicInteger(0)

  // =========================================================================
  // Getters — exposed via JMX
  // =========================================================================

  override def getWriterCount:         Int  = writerCount.get()
  override def getIdleWriterEvictions: Long = idleWriterEvictions.sum()

  override def getGranularCacheSize:   Int  = granularCacheSize.get()
  override def getGranularCacheHits:   Long = granularCacheHits.sum()
  override def getGranularCacheMisses: Long = granularCacheMisses.sum()

  override def getGcQueueDepth:            Int  = gcQueueDepth.get()
  override def getGcLocksEnqueued:         Long = gcLocksEnqueued.sum()
  override def getGcLocksDeleted:          Long = gcLocksDeleted.sum()
  override def getGcLocksSkippedReclaimed: Long = gcLocksSkippedReclaimed.sum()
  override def getGcLocksSkippedRevoked:   Long = gcLocksSkippedRevoked.sum()
  override def getGcDeleteFailures:        Long = gcDeleteFailures.sum()
  override def getGcDeleteRetries:         Long = gcDeleteRetries.sum()

  override def getMasterLockUpdates:                        Long    = masterLockUpdates.sum()
  override def getMasterLockFailures:                       Long    = masterLockFailures.sum()
  override def getMasterLockWriteSkipped:                   Long    = masterLockWriteSkipped.sum()
  override def getMasterLockWriteForcedRevoke:              Long    = masterLockWriteForcedRevoke.sum()
  override def getMasterLockWriteForcedStop:                Long    = masterLockWriteForcedStop.sum()
  override def getMasterLockWriteForcedPostCleanUp:         Long    = masterLockWriteForcedPostCleanUp.sum()
  override def getMasterLockWriteForcedRevokeFailures:      Long    = masterLockWriteForcedRevokeFailures.sum()
  override def getMasterLockWriteForcedStopFailures:        Long    = masterLockWriteForcedStopFailures.sum()
  override def getMasterLockWriteForcedPostCleanUpFailures: Long    = masterLockWriteForcedPostCleanUpFailures.sum()
  override def getMasterLockDirtyWindowCycles:              Long    = masterLockDirtyWindowCycles.sum()
  override def getMasterLockDirty:                          Boolean = !dirtyTopicPartitions.isEmpty

  override def getSweepRuns:            Long = sweepRuns.sum()
  override def getSweepOrphansEnqueued: Long = sweepOrphansEnqueued.sum()
  override def getSweepGetBudgetUsed:   Int  = sweepGetBudgetUsed.get()

  override def getSafeOffsetBarrierWriters: Int = safeOffsetBarrierWriters.get()

  // A. Ingest throughput
  override def getRecordsReceivedTotal:    Long = recordsReceivedTotal.sum()
  override def getRecordsWrittenTotal:     Long = recordsWrittenTotal.sum()
  override def getNullRecordsSkippedTotal: Long = nullRecordsSkippedTotal.sum()
  override def getLastPutEpochMillis:      Long = lastPutEpochMillis.get()
  override def getPutTimerCount:           Long = putTimer.count
  override def getPutTimerSumMillis:       Long = putTimer.sumMillis
  override def getPutTimerMaxMillis:       Long = putTimer.maxMillis

  // B. File / commit lifecycle
  override def getFilesOpenedTotal:      Long = filesOpenedTotal.sum()
  override def getFilesCommittedTotal:   Long = filesCommittedTotal.sum()
  override def getFilesFailedTotal:      Long = filesFailedTotal.sum()
  override def getBytesWrittenTotal:     Long = bytesWrittenTotal.sum()
  override def getRecordsCommittedTotal: Long = recordsCommittedTotal.sum()
  override def getCommitTimerCount:      Long = commitTimer.count
  override def getCommitTimerSumMillis:  Long = commitTimer.sumMillis
  override def getCommitTimerMaxMillis:  Long = commitTimer.maxMillis

  // C. Storage SDK
  override def getStorageUploadTimerCount:     Long = storageUploadTimer.count
  override def getStorageUploadTimerSumMillis: Long = storageUploadTimer.sumMillis
  override def getStorageUploadTimerMaxMillis: Long = storageUploadTimer.maxMillis
  override def getStorageUploadErrorsTotal:    Long = storageUploadErrors.sum()

  override def getStorageCopyTimerCount:     Long = storageCopyTimer.count
  override def getStorageCopyTimerSumMillis: Long = storageCopyTimer.sumMillis
  override def getStorageCopyTimerMaxMillis: Long = storageCopyTimer.maxMillis
  override def getStorageCopyErrorsTotal:    Long = storageCopyErrors.sum()

  override def getStorageDeleteErrorsTotal: Long = storageDeleteErrors.sum()

  override def getStorageGetTimerCount:     Long = storageGetTimer.count
  override def getStorageGetTimerSumMillis: Long = storageGetTimer.sumMillis
  override def getStorageGetTimerMaxMillis: Long = storageGetTimer.maxMillis
  override def getStorageGetErrorsTotal:    Long = storageGetErrors.sum()

  override def getStorageListErrorsTotal: Long = storageListErrors.sum()

  // D. Retries & error classification
  override def getPendingOperationRetriesTotal: Long = pendingOperationRetriesTotal.sum()
  override def getCommitRetriesTotal:           Long = commitRetriesTotal.sum()
  override def getSinkErrorsFatalTotal:         Long = sinkErrorsFatalTotal.sum()
  override def getSinkErrorsRetriableTotal:     Long = sinkErrorsRetriableTotal.sum()
  override def getSinkErrorsNonFatalTotal:      Long = sinkErrorsNonFatalTotal.sum()

  // E. Schema / skip / seek diagnostics
  override def getSchemaRolloversTotal:         Long = schemaRolloversTotal.sum()
  override def getDuplicateRecordsSkippedTotal: Long = duplicateRecordsSkippedTotal.sum()
  override def getSeekOnOpenAppliedTotal:       Long = seekOnOpenAppliedTotal.sum()
  override def getRebalanceClosesTotal:         Long = rebalanceClosesTotal.sum()
  override def getBatchCommitsTotal:            Long = batchCommitsTotal.sum()
  override def getBatchCommitFilesTotal:        Long = batchCommitFilesTotal.sum()
  override def getBatchCommitFailuresTotal:     Long = batchCommitFailuresTotal.sum()
  override def getLegacyLocksPurgedTotal:       Long = legacyLocksPurgedTotal.sum()

  // F. Current state gauges
  override def getInFlightUploads: Int = inFlightUploads.get()

  override def getMillisSinceLastCommit: Long = {
    val last = lastCommitEpochMillis.get()
    if (last == 0L) 0L else System.currentTimeMillis() - last
  }

  // =========================================================================
  // Mutators — not exposed via JMX trait
  // =========================================================================

  def setWriterCount(count: Int): Unit = writerCount.set(count)
  def incrementIdleWriterEvictions(): Unit = idleWriterEvictions.increment()

  def setGranularCacheSize(size: Int): Unit = granularCacheSize.set(size)
  def incrementGranularCacheHits():   Unit = granularCacheHits.increment()
  def incrementGranularCacheMisses(): Unit = granularCacheMisses.increment()

  def setGcQueueDepth(depth:          Int):  Unit = gcQueueDepth.set(depth)
  def incrementGcLocksEnqueued(count: Long): Unit = gcLocksEnqueued.add(count)
  def incrementGcLocksDeleted(count:  Long): Unit = gcLocksDeleted.add(count)
  def incrementGcLocksSkippedReclaimed(): Unit = gcLocksSkippedReclaimed.increment()
  def incrementGcLocksSkippedRevoked():   Unit = gcLocksSkippedRevoked.increment()
  def incrementGcDeleteFailures():        Unit = gcDeleteFailures.increment()
  def incrementGcDeleteRetries(count: Long): Unit = gcDeleteRetries.add(count)

  def incrementMasterLockUpdates():  Unit = masterLockUpdates.increment()
  def incrementMasterLockFailures(): Unit = masterLockFailures.increment()

  def incrementMasterLockWriteSkipped(): Unit = masterLockWriteSkipped.increment()

  def incrementMasterLockWriteForced(reason: ForcedWriteReason): Unit = reason match {
    case ForcedWriteReason.Revoke      => masterLockWriteForcedRevoke.increment()
    case ForcedWriteReason.Stop        => masterLockWriteForcedStop.increment()
    case ForcedWriteReason.PostCleanUp => masterLockWriteForcedPostCleanUp.increment()
  }

  def incrementMasterLockWriteForcedFailure(reason: ForcedWriteReason): Unit = reason match {
    case ForcedWriteReason.Revoke      => masterLockWriteForcedRevokeFailures.increment()
    case ForcedWriteReason.Stop        => masterLockWriteForcedStopFailures.increment()
    case ForcedWriteReason.PostCleanUp => masterLockWriteForcedPostCleanUpFailures.increment()
  }

  def incrementMasterLockDirtyWindowCycle(): Unit = masterLockDirtyWindowCycles.increment()

  def markMasterLockDirty(tp: TopicPartition): Unit = { val _ = dirtyTopicPartitions.add(tp) }
  def clearMasterLockDirty(tp: TopicPartition): Unit = { val _ = dirtyTopicPartitions.remove(tp) }
  def clearAllMasterLockDirty(): Unit = dirtyTopicPartitions.clear()

  def incrementSweepRuns(): Unit = sweepRuns.increment()
  def incrementSweepOrphansEnqueued(count: Long): Unit = sweepOrphansEnqueued.add(count)
  def setSweepGetBudgetUsed(used:          Int):  Unit = sweepGetBudgetUsed.set(used)

  def setSafeOffsetBarrierWriters(count: Int): Unit = safeOffsetBarrierWriters.set(count)

  // A. Ingest throughput mutators
  def addRecordsReceivedTotal(n: Long): Unit = recordsReceivedTotal.add(n)
  def incrementRecordsWrittenTotal():     Unit = recordsWrittenTotal.increment()
  def incrementNullRecordsSkippedTotal(): Unit = nullRecordsSkippedTotal.increment()
  def setLastPutEpochMillis(ts:     Long): Unit = lastPutEpochMillis.set(ts)
  def recordPutTimer(elapsedMillis: Long): Unit = putTimer.record(elapsedMillis)

  // B. File / commit lifecycle mutators
  def incrementFilesOpenedTotal():    Unit = filesOpenedTotal.increment()
  def incrementFilesCommittedTotal(): Unit = filesCommittedTotal.increment()
  def incrementFilesFailedTotal():    Unit = filesFailedTotal.increment()
  def addBytesWrittenTotal(n:          Long): Unit = bytesWrittenTotal.add(n)
  def addRecordsCommittedTotal(n:      Long): Unit = recordsCommittedTotal.add(n)
  def recordCommitTimer(elapsedMillis: Long): Unit = commitTimer.record(elapsedMillis)
  def setLastCommitEpochMillis(ts:     Long): Unit = lastCommitEpochMillis.set(ts)

  // C. Storage SDK mutators
  def recordStorageUpload(elapsedMillis: Long, isError: Boolean): Unit = {
    storageUploadTimer.record(elapsedMillis)
    if (isError) storageUploadErrors.increment()
  }
  def recordStorageCopy(elapsedMillis: Long, isError: Boolean): Unit = {
    storageCopyTimer.record(elapsedMillis)
    if (isError) storageCopyErrors.increment()
  }
  def recordStorageDeleteError(): Unit = storageDeleteErrors.increment()
  def recordStorageGet(elapsedMillis: Long, isError: Boolean): Unit = {
    storageGetTimer.record(elapsedMillis)
    if (isError) storageGetErrors.increment()
  }
  def recordStorageListError(): Unit = storageListErrors.increment()

  // D. Retries & error classification mutators
  def incrementPendingOperationRetriesTotal(): Unit = pendingOperationRetriesTotal.increment()
  def incrementCommitRetriesTotal():           Unit = commitRetriesTotal.increment()
  def incrementSinkErrorsFatalTotal():         Unit = sinkErrorsFatalTotal.increment()
  def incrementSinkErrorsRetriableTotal():     Unit = sinkErrorsRetriableTotal.increment()
  def incrementSinkErrorsNonFatalTotal():      Unit = sinkErrorsNonFatalTotal.increment()

  // E. Schema / skip / seek diagnostics mutators
  def incrementSchemaRolloversTotal():         Unit = schemaRolloversTotal.increment()
  def incrementDuplicateRecordsSkippedTotal(): Unit = duplicateRecordsSkippedTotal.increment()
  def incrementSeekOnOpenAppliedTotal():       Unit = seekOnOpenAppliedTotal.increment()
  def incrementRebalanceClosesTotal():         Unit = rebalanceClosesTotal.increment()

  def incrementBatchCommits():        Unit = batchCommitsTotal.increment()
  def incrementBatchCommitFailures(): Unit = batchCommitFailuresTotal.increment()
  def addBatchCommitFiles(count: Long): Unit = batchCommitFilesTotal.add(count)
  def incrementLegacyLocksPurged(): Unit = legacyLocksPurgedTotal.increment()

  // F. State gauge mutators
  def incrementInFlightUploads(): Unit = { val _ = inFlightUploads.incrementAndGet() }
  def decrementInFlightUploads(): Unit = { val _ = inFlightUploads.decrementAndGet() }
}
