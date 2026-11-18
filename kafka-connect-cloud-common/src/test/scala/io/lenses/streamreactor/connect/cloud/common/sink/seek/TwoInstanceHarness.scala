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
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface

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
 * `contentETags` makes the shared store derive eTags from the bytes written (see
 * `InMemoryStorageInterface`). `sweepMaxReads` is the orphan sweep's per-cycle GET budget.
 */
final class TwoInstanceHarness(
  instanceCount: Int        = 2,
  commitMode:    CommitMode = CommitMode.Default,
  sweepEnabled:  Boolean    = false,
  sweepSeconds:  Int        = Int.MaxValue,
  gcSeconds:     Int        = Int.MaxValue,
  contentETags:  Boolean    = false,
  sweepMaxReads: Int        = IndexManagerV2.DefaultGcSweepMaxReads,
  // Per-instance commit modes, by index; instances past its end use `commitMode`.
  commitModes: Seq[CommitMode] = Seq.empty,
) extends AutoCloseable {
  require(instanceCount >= 2, s"a multi-instance harness needs at least 2 instances, got $instanceCount")

  val bucket:            String = "test-bucket"
  val directoryFileName: String = ".indexes2"
  val connectorName:     String = "ownership-two-instance"

  val storage: InMemoryStorageInterface = new InMemoryStorageInterface(contentETags = contentETags)

  /** Shared wall clock (epoch millis) handed to every instance; tests set it explicitly. */
  val clock: AtomicLong = new AtomicLong(1000L)

  implicit private val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)

  /** One task id per instance: same name/taskNo (same lock paths), distinct `lockUuid`. */
  val taskIds: Vector[ConnectorTaskId] = Vector.fill(instanceCount)(ConnectorTaskId(connectorName, 1, 0))

  /** One metrics registry per instance, so a test can read what an instance counted. */
  val metrics: Vector[CloudSinkMetrics] = Vector.fill(instanceCount)(new CloudSinkMetrics())

  val instances: Vector[IndexManagerV2] = taskIds.zip(metrics).zipWithIndex.map {
    case ((id, m), n) => buildIndexManager(id, m, commitModes.lift(n).getOrElse(commitMode))
  }

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

  private def buildIndexManager(
    taskId:          ConnectorTaskId,
    instanceMetrics: CloudSinkMetrics,
    mode:            CommitMode,
  ): IndexManagerV2 = {
    implicit val si:  StorageInterface[FakeFileMetadata] = storage
    implicit val tid: ConnectorTaskId                    = taskId
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      directoryFileName           = directoryFileName,
      gcIntervalSeconds           = gcSeconds,
      gcSweepIntervalSeconds      = sweepSeconds,
      gcSweepMinAgeSeconds        = sweepSeconds,
      gcSweepEnabled              = sweepEnabled,
      gcSweepMaxReads             = sweepMaxReads,
      clock                       = () => clock.get(),
      commitMode                  = mode,
      metrics                     = instanceMetrics,
    )(si, taskId)
  }
}
