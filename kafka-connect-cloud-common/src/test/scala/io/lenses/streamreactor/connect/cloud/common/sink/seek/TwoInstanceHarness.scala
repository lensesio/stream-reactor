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

import cats.data.Validated
import cats.implicits._
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * N real `IndexManagerV2` instances of the SAME connector sharing one `InMemoryStorageInterface`,
 * so they read and CAS the same lock files. Each instance gets its own `ConnectorTaskId` object,
 * and therefore its own random `lockUuid`, exactly like two task instances of one connector task
 * on different workers (the zombie scenario).
 *
 * GC and sweep never run in the background (all intervals `Int.MaxValue` by default) unless a test
 * drives `drainGcQueue()` / `sweepOrphanedLocks()` / `sweepAllBatchTemps()` itself. `sweepEnabled`
 * turns the sweeps on for such direct calls; `sweepSeconds` is both the sweep interval and the
 * minimum object age, so a test ages an object past it with `storage.setLastModified`.
 */
final class TwoInstanceHarness(
  instanceCount: Int        = 2,
  commitMode:    CommitMode = CommitMode.Default,
  sweepEnabled:  Boolean    = false,
  sweepSeconds:  Int        = Int.MaxValue,
) extends AutoCloseable {
  require(instanceCount >= 2, s"a multi-instance harness needs at least 2 instances, got $instanceCount")

  val bucket:            String = "test-bucket"
  val directoryFileName: String = ".indexes2"
  val connectorName:     String = "lc350-two-instance"

  val storage: InMemoryStorageInterface = new InMemoryStorageInterface()

  /** Shared wall clock (epoch millis) handed to every instance; tests set it explicitly. */
  val clock: AtomicLong = new AtomicLong(1000L)

  implicit private val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)

  /** One task id per instance: same name/taskNo (same lock paths), distinct `lockUuid`. */
  val taskIds: Vector[ConnectorTaskId] = Vector.fill(instanceCount)(ConnectorTaskId(connectorName, 1, 0))

  val instances: Vector[IndexManagerV2] = taskIds.map(buildIndexManager)

  def a: IndexManagerV2 = instances(0)
  def b: IndexManagerV2 = instances(1)

  def lockUuid(instance: Int): String = taskIds(instance).lockUuid

  def bucketAndPrefix(tp: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${tp.topic.value}/${tp.partition}/")).asRight

  def masterLockPath(tp: TopicPartition): String =
    IndexManagerV2.generateLockFilePath(taskIds.head, tp, directoryFileName)

  def granularLockPath(tp: TopicPartition, partitionKey: String): String =
    IndexManagerV2.generateGranularLockFilePath(taskIds.head, tp, partitionKey, directoryFileName)

  def sweepMarkerPath(tp: TopicPartition): String =
    IndexManagerV2.generateSweepMarkerPath(taskIds.head, tp, directoryFileName)

  /** The connector-scoped prefix batch mode stages temp objects under, built as `Writer` builds it. */
  def batchTempPrefix(tp: TopicPartition): String =
    s".temp-upload/$connectorName/${tp.topic}/${tp.partition}/"

  /** Reads a lock straight from storage (content + current eTag), bypassing every instance's cache. */
  def readLock(path: String): Either[FileLoadError, ObjectWithETag[IndexFile]] =
    storage.getBlobAsObject[IndexFile](bucket, path)

  /** Closes every instance, swallowing errors, so a failing test still releases executors. */
  override def close(): Unit =
    instances.foreach { im =>
      IndexManagerV2.IndexManagerV2TestHooks.clearPreWriteMasterLockBarrier(im)
      try im.close()
      catch { case _: Throwable => () }
    }

  private def buildIndexManager(taskId: ConnectorTaskId): IndexManagerV2 = {
    implicit val si:  StorageInterface[FakeFileMetadata] = storage
    implicit val tid: ConnectorTaskId                    = taskId
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      directoryFileName           = directoryFileName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = sweepSeconds,
      gcSweepMinAgeSeconds        = sweepSeconds,
      gcSweepEnabled              = sweepEnabled,
      clock                       = () => clock.get(),
      commitMode                  = commitMode,
    )(si, taskId)
  }
}

object TwoInstanceHarness {

  val DefaultAwaitSeconds: Long = 10L

  /**
   * A one-shot pause point for barrier hooks: the paused thread calls `pause()`, which signals
   * `reached` and blocks until the test calls `release()`. Every wait is bounded, so a broken
   * interleaving fails the test instead of hanging the build.
   */
  final class Gate {
    private val reachedLatch = new CountDownLatch(1)
    private val releaseLatch = new CountDownLatch(1)

    def pause(): Unit = {
      reachedLatch.countDown()
      if (!releaseLatch.await(DefaultAwaitSeconds, TimeUnit.SECONDS))
        throw new IllegalStateException(s"Gate was not released within ${DefaultAwaitSeconds}s")
    }

    def awaitReached(): Unit =
      if (!reachedLatch.await(DefaultAwaitSeconds, TimeUnit.SECONDS))
        throw new IllegalStateException(s"Gate was not reached within ${DefaultAwaitSeconds}s")

    def release(): Unit = releaseLatch.countDown()
  }
}
