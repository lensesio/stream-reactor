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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.LongAdder

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
   * Cumulative successful eTag-conditional master lock writes. Each success advances the
   * consumer-committable offset (globalSafeOffset) for a Kafka partition.
   */
  def getMasterLockUpdates: Long

  /**
   * Cumulative failed master lock writes. A failure means preCommit returned no offset for
   * the partition, so Kafka does not advance the consumer offset. Persistent failures may
   * indicate a zombie task holding a conflicting eTag or cloud storage issues.
   */
  def getMasterLockFailures: Long

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
}

class CloudSinkMetrics() extends CloudSinkMetricsMBean {

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

  private val masterLockUpdates  = new LongAdder()
  private val masterLockFailures = new LongAdder()

  private val sweepRuns            = new LongAdder()
  private val sweepOrphansEnqueued = new LongAdder()
  private val sweepGetBudgetUsed   = new AtomicInteger(0)

  // --- getters (exposed via JMX) ---

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

  override def getMasterLockUpdates:  Long = masterLockUpdates.sum()
  override def getMasterLockFailures: Long = masterLockFailures.sum()

  override def getSweepRuns:            Long = sweepRuns.sum()
  override def getSweepOrphansEnqueued: Long = sweepOrphansEnqueued.sum()
  override def getSweepGetBudgetUsed:   Int  = sweepGetBudgetUsed.get()

  // --- mutators (not exposed via JMX trait) ---

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

  def incrementSweepRuns(): Unit = sweepRuns.increment()
  def incrementSweepOrphansEnqueued(count: Long): Unit = sweepOrphansEnqueued.add(count)
  def setSweepGetBudgetUsed(used:          Int):  Unit = sweepGetBudgetUsed.set(used)
}
