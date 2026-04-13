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

  // Writer map
  def getWriterCount:         Int
  def getMaxWriters:          Int
  def getIdleWriterEvictions: Long

  // Granular lock cache
  def getGranularCacheSize:   Int
  def getGranularCacheHits:   Long
  def getGranularCacheMisses: Long

  // GC
  def getGcQueueDepth:            Int
  def getGcLocksEnqueued:         Long
  def getGcLocksDeleted:          Long
  def getGcLocksSkippedReclaimed: Long
  def getGcLocksSkippedRevoked:   Long
  def getGcDeleteFailures:        Long
  def getGcDeleteRetries:         Long

  // Master lock
  def getMasterLockUpdates:  Long
  def getMasterLockFailures: Long

  // Orphan sweep
  def getSweepRuns:            Long
  def getSweepOrphansEnqueued: Long
  def getSweepGetBudgetUsed:   Int
}

class CloudSinkMetrics(configMaxWriters: Int) extends CloudSinkMetricsMBean {

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
  override def getMaxWriters:          Int  = configMaxWriters
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

  def setWriterCount(count: Int): Unit  = writerCount.set(count)
  def incrementIdleWriterEvictions(): Unit = idleWriterEvictions.increment()

  def setGranularCacheSize(size: Int): Unit  = granularCacheSize.set(size)
  def incrementGranularCacheHits(): Unit     = granularCacheHits.increment()
  def incrementGranularCacheMisses(): Unit   = granularCacheMisses.increment()

  def setGcQueueDepth(depth: Int): Unit           = gcQueueDepth.set(depth)
  def incrementGcLocksEnqueued(count: Long): Unit  = gcLocksEnqueued.add(count)
  def incrementGcLocksDeleted(count: Long): Unit   = gcLocksDeleted.add(count)
  def incrementGcLocksSkippedReclaimed(): Unit      = gcLocksSkippedReclaimed.increment()
  def incrementGcLocksSkippedRevoked(): Unit        = gcLocksSkippedRevoked.increment()
  def incrementGcDeleteFailures(): Unit             = gcDeleteFailures.increment()
  def incrementGcDeleteRetries(count: Long): Unit   = gcDeleteRetries.add(count)

  def incrementMasterLockUpdates(): Unit  = masterLockUpdates.increment()
  def incrementMasterLockFailures(): Unit = masterLockFailures.increment()

  def incrementSweepRuns(): Unit                    = sweepRuns.increment()
  def incrementSweepOrphansEnqueued(count: Long): Unit = sweepOrphansEnqueued.add(count)
  def setSweepGetBudgetUsed(used: Int): Unit        = sweepGetBudgetUsed.set(used)
}
