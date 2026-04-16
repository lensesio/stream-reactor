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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.lang.management.ManagementFactory
import javax.management.ObjectName

class CloudSinkMetricsTest extends AnyFunSuiteLike with Matchers with BeforeAndAfterEach {

  private var metrics: CloudSinkMetrics = _

  override def beforeEach(): Unit = {
    metrics = new CloudSinkMetrics()
    super.beforeEach()
  }

  // --- Writer map gauges / counters ---

  test("WriterCount defaults to 0 and can be set") {
    metrics.getWriterCount shouldBe 0
    metrics.setWriterCount(42)
    metrics.getWriterCount shouldBe 42
  }

  test("IdleWriterEvictions increments correctly") {
    metrics.getIdleWriterEvictions shouldBe 0L
    metrics.incrementIdleWriterEvictions()
    metrics.incrementIdleWriterEvictions()
    metrics.getIdleWriterEvictions shouldBe 2L
  }

  // --- Granular cache gauges / counters ---

  test("GranularCacheSize defaults to 0 and can be set") {
    metrics.getGranularCacheSize shouldBe 0
    metrics.setGranularCacheSize(100)
    metrics.getGranularCacheSize shouldBe 100
  }

  test("GranularCacheHits and Misses increment correctly") {
    metrics.getGranularCacheHits shouldBe 0L
    metrics.getGranularCacheMisses shouldBe 0L
    metrics.incrementGranularCacheHits()
    metrics.incrementGranularCacheHits()
    metrics.incrementGranularCacheMisses()
    metrics.getGranularCacheHits shouldBe 2L
    metrics.getGranularCacheMisses shouldBe 1L
  }

  // --- GC counters ---

  test("GcQueueDepth defaults to 0 and can be set") {
    metrics.getGcQueueDepth shouldBe 0
    metrics.setGcQueueDepth(7)
    metrics.getGcQueueDepth shouldBe 7
  }

  test("GcLocksEnqueued accumulates") {
    metrics.getGcLocksEnqueued shouldBe 0L
    metrics.incrementGcLocksEnqueued(5)
    metrics.incrementGcLocksEnqueued(3)
    metrics.getGcLocksEnqueued shouldBe 8L
  }

  test("GcLocksDeleted accumulates") {
    metrics.getGcLocksDeleted shouldBe 0L
    metrics.incrementGcLocksDeleted(10)
    metrics.getGcLocksDeleted shouldBe 10L
  }

  test("GcLocksSkippedReclaimed increments") {
    metrics.getGcLocksSkippedReclaimed shouldBe 0L
    metrics.incrementGcLocksSkippedReclaimed()
    metrics.getGcLocksSkippedReclaimed shouldBe 1L
  }

  test("GcDeleteFailures increments") {
    metrics.getGcDeleteFailures shouldBe 0L
    metrics.incrementGcDeleteFailures()
    metrics.incrementGcDeleteFailures()
    metrics.getGcDeleteFailures shouldBe 2L
  }

  // --- Master lock counters ---

  test("MasterLockUpdates and Failures increment correctly") {
    metrics.getMasterLockUpdates shouldBe 0L
    metrics.getMasterLockFailures shouldBe 0L
    metrics.incrementMasterLockUpdates()
    metrics.incrementMasterLockUpdates()
    metrics.incrementMasterLockFailures()
    metrics.getMasterLockUpdates shouldBe 2L
    metrics.getMasterLockFailures shouldBe 1L
  }

  // --- Sweep counters ---

  test("SweepRuns and SweepOrphansEnqueued increment correctly") {
    metrics.getSweepRuns shouldBe 0L
    metrics.getSweepOrphansEnqueued shouldBe 0L
    metrics.incrementSweepRuns()
    metrics.incrementSweepOrphansEnqueued(12)
    metrics.getSweepRuns shouldBe 1L
    metrics.getSweepOrphansEnqueued shouldBe 12L
  }

  test("SweepGetBudgetUsed defaults to 0 and can be set") {
    metrics.getSweepGetBudgetUsed shouldBe 0
    metrics.setSweepGetBudgetUsed(500)
    metrics.getSweepGetBudgetUsed shouldBe 500
  }

  // --- JMX registration round-trip ---

  test("register and unregister via CloudSinkMetricsRegistrar") {
    val taskId = ConnectorTaskId("test-connector", 2, 1)
    val name   = new ObjectName("io.lenses.streamreactor.connect.cloud.sink:type=metrics,name=test-connector,task=1")
    val mbs    = ManagementFactory.getPlatformMBeanServer

    try {
      CloudSinkMetricsRegistrar.register(metrics, taskId)
      mbs.isRegistered(name) shouldBe true

      metrics.setWriterCount(77)
      mbs.getAttribute(name, "WriterCount") shouldBe 77

      metrics.incrementMasterLockUpdates()
      mbs.getAttribute(name, "MasterLockUpdates") shouldBe 1L
    } finally {
      CloudSinkMetricsRegistrar.unregister(taskId)
    }
    mbs.isRegistered(name) shouldBe false
  }
}
