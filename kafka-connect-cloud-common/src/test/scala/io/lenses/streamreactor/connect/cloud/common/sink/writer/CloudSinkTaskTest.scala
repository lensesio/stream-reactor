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

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig
import io.lenses.streamreactor.common.errors._
import io.lenses.streamreactor.common.util.JarManifest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CloudCommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoIndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.errors.RetriableException
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.util.Try

/**
 * Integration-level tests for the behaviours that `CloudSinkTask` is responsible for at its
 * public API boundary, exercised through the lower-level building blocks that `CloudSinkTask`
 * delegates to.
 *
 * Three distinct concerns are verified:
 *
 *   1. **Error-policy routing** – a missing local staging file during a live commit must surface
 *      as `FatalConnectException` through all three `error.policy` values (RETRY, NOOP, THROW).
 *      Under RETRY it must NOT be wrapped in `RetriableException` (that would schedule an
 *      in-process retry without the file, ultimately losing data); under NOOP it must NOT be
 *      swallowed with a warning log; under THROW it must NOT be downgraded to a plain
 *      `ConnectException`.
 *
 *   2. **Rollback chain** – `handleErrors` must call `WriterManager.cleanUp(tp)` for every
 *      fatal TP before rethrowing.  After `cleanUp`, the writer is evicted from the manager and
 *      `preCommit` must not return an offset for that TP.
 *
 *   3. **Mixed-batch isolation** – when one TP in a batch is fatal and another succeeds,
 *      `cleanUp` is scoped to the failing TP only.  The healthy TP's writer survives and
 *      `preCommit` advances normally for it.
 *
 * Assembly strategy: `WriterManager` is built with stub lambda arguments (no cloud-provider
 * config, no KCQL, no real FormatWriter), and real `Writer` instances are injected via the
 * package-private `putWriter` helper.
 *
 * `runPut` creates a real `ConcreteTestSinkTask` (a concrete subclass of `CloudSinkTask` that
 * implements the abstract cloud-provider methods with stubs) and calls the REAL production
 * `handleErrors` / `handleTry` path.  This means changes to `CloudSinkTask.handleErrors` in
 * production will immediately break these tests.  The `private[sink]` visibility modifier on
 * `handleErrors` (and `writerManager`) enables the access from this package without exposing
 * them in the public API.
 */
class CloudSinkTaskTest
    extends AnyFunSuiteLike
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with EitherValues {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("cst-test", 1, 0)
  private implicit val locValidator: CloudLocationValidator =
    (location: CloudLocation) => cats.data.Validated.fromEither(Right(location))

  private val bucket = "test-bucket"
  private val tpA    = Topic("topic-a").withPartition(0)
  private val tpB    = Topic("topic-b").withPartition(0)

  test("live-commit NonExistingFileError throws FatalConnectException under error.policy=RETRY (not RetriableException)") {
    val wm     = buildWMWithUploadingWriter(tpA, nonExistentFile())
    val thrown = runPut(wm, RetryErrorPolicy())
    thrown shouldBe a[FatalConnectException]
    thrown should not be a[RetriableException]
  }

  test("live-commit NonExistingFileError throws FatalConnectException under error.policy=NOOP (not silently swallowed)") {
    val wm     = buildWMWithUploadingWriter(tpA, nonExistentFile())
    val thrown = runPut(wm, NoopErrorPolicy())
    thrown shouldBe a[FatalConnectException]
  }

  test(
    "live-commit NonExistingFileError throws FatalConnectException as-is under error.policy=THROW (not wrapped in plain ConnectException)",
  ) {
    val wm     = buildWMWithUploadingWriter(tpA, nonExistentFile())
    val thrown = runPut(wm, ThrowErrorPolicy())
    // FatalConnectException extends ConnectException; assert the exact marker subclass is preserved
    thrown.getClass shouldBe classOf[FatalConnectException]
    // Pin the cause chain: FatalConnectException → IllegalStateException(missing.getPath)
    Option(thrown.getCause) match {
      case Some(cause: IllegalStateException) =>
        cause.getMessage shouldBe "/nonexistent/cst-test/staging.tmp"
        Option(cause.getCause) shouldBe None
      case other =>
        fail(s"Expected cause to be IllegalStateException, was: $other")
    }
  }

  test("live-commit Fatal triggers rollback chain and clears WriterManager writer state") {
    // Build directly so we can capture the WM-level IndexManager and verify eviction.
    val pop    = new PendingOperationsProcessors(new InMemoryStorageInterface())
    val idxMgr = stubWriterIndexManager()
    val writer = buildUploadingWriter(tpA, nonExistentFile(), pop, idxMgr, Offset(99), Offset(150))
    val im     = stubWmIndexManager()
    val wm     = buildMinimalWriterManager(im)
    wm.putWriter(MapKey(tpA, Map.empty), writer)
    wm.writerCount shouldBe 1

    intercept[FatalConnectException] { runPutThrow(wm, RetryErrorPolicy()) }

    wm.writerCount shouldBe 0
    verify(im, times(1)).evictAllGranularLocks(tpA)
  }

  test("live-commit Fatal does NOT advance preCommit offset (indexes.enabled=true)") {
    val wm = buildWMWithUploadingWriter(tpA, nonExistentFile())

    intercept[FatalConnectException] { runPutThrow(wm, RetryErrorPolicy()) }

    val returned = wm.preCommit(Map(tpA -> new OffsetAndMetadata(200L)))
    returned.keySet should not contain tpA
    returned.contains(tpA) shouldBe false
  }

  test("live-commit Fatal does NOT advance preCommit offset (indexes.enabled=false / NoIndexManager mode)") {
    // NoIndexManager: single-op [Upload] chain with no lock writes.
    val pop    = new PendingOperationsProcessors(new InMemoryStorageInterface())
    val writer = buildUploadingWriter(tpA, nonExistentFile(), pop, new NoIndexManager(), Offset(99), Offset(150))
    val wm     = buildMinimalWriterManager(stubWmIndexManager())
    wm.putWriter(MapKey(tpA, Map.empty), writer)

    intercept[FatalConnectException] { runPutThrow(wm, RetryErrorPolicy()) }

    val returned = wm.preCommit(Map(tpA -> new OffsetAndMetadata(200L)))
    returned.keySet should not contain tpA
    returned.contains(tpA) shouldBe false
  }

  test("mixed-batch partial failure: rollback isolated to fatal TPs only (indexed mode)") {
    // writerA (tpA): upload will fail with NonExistingFileError → FatalCloudSinkError
    val popA    = new PendingOperationsProcessors(new InMemoryStorageInterface())
    val idxMgrA = stubWriterIndexManager()
    val writerA = buildUploadingWriter(tpA, nonExistentFile(), popA, idxMgrA, Offset(99), Offset(150))

    // writerB (tpB): upload will succeed (real temp file + in-memory storage)
    val storageB  = new InMemoryStorageInterface()
    val popB      = new PendingOperationsProcessors(storageB)
    val idxMgrB   = stubWriterIndexManager()
    val tempFileB = makeTempFile("records-for-tpB")
    val writerB   = buildUploadingWriter(tpB, tempFileB, popB, idxMgrB, Offset(99), Offset(150))

    // Capture the WM-level IndexManager so we can Mockito-verify TP-scoped eviction.
    val im = stubWmIndexManager()
    val wm = buildMinimalWriterManager(im)
    wm.putWriter(MapKey(tpA, Map.empty), writerA)
    wm.putWriter(MapKey(tpB, Map.empty), writerB)
    wm.writerCount shouldBe 2

    intercept[FatalConnectException] { runPutThrow(wm, RetryErrorPolicy()) }

    // writerA evicted; writerB survives
    wm.writerCount shouldBe 1

    val returned = wm.preCommit(Map(
      tpA -> new OffsetAndMetadata(200L),
      tpB -> new OffsetAndMetadata(200L),
    ))
    returned.keySet should not contain tpA
    returned.contains(tpA) shouldBe false
    returned should contain key tpB
    // writerB committed offset 150 → preCommit reports 151
    returned(tpB).offset() shouldBe 151L

    // Granular lock cache eviction must be TP-scoped: failing TP evicted exactly once,
    // non-failing TP never evicted. Catches future refactors that widen rollback past
    // error.topicPartitions() (e.g. evict all writers regardless of TP).
    verify(im, times(1)).evictAllGranularLocks(tpA)
    verify(im, never).evictAllGranularLocks(tpB)
  }

  test(
    "mid-chain Copy failure (FatalCloudSinkError) under error.policy=RETRY throws FatalConnectException, NOT RetriableException",
  ) {
    // Use a storage where Upload succeeds (temp blob lands) but mvFile always fails.
    val storage = new MidChainCopyFailingStorage()
    val pop     = new PendingOperationsProcessors(storage)
    val idxMgr  = stubWriterIndexManager()
    val tmpFile = makeTempFile("mid-chain-copy-test-records")
    val writer  = buildUploadingWriter(tpA, tmpFile, pop, idxMgr, Offset(99), Offset(150))
    val wm      = buildMinimalWriterManager(stubWmIndexManager())
    wm.putWriter(MapKey(tpA, Map.empty), writer)

    val thrown = runPut(wm, RetryErrorPolicy())

    thrown shouldBe a[FatalConnectException]
    thrown should not be a[RetriableException]

    // The Upload succeeded before Copy failed: the temp blob must be present in storage.
    // Crash-recovery on restart will resume the chain from the persisted PendingState.
    val blobKeys = storage.snapshot(bucket).keys
    blobKeys.exists(_.contains(".temp-upload/")) shouldBe true
  }

  /**
   * Drives `wm.recommitPending()` through the REAL production `CloudSinkTask.handleErrors`
   * and `ErrorHandler.handleTry` path.  A concrete `ConcreteTestSinkTask` is created per
   * call; its `writerManager` field (package-private within `sink`) is set to `wm` so the
   * production `rollback` / `cleanUp` chain operates on the same manager the test inspects.
   *
   * Returns the exception rethrown by the error policy, or `null` if none.
   */
  private def runPut(
    wm:         WriterManager[FakeFileMetadata],
    policy:     ErrorPolicy,
    maxRetries: Int = 5,
  ): Throwable = {
    val task = new ConcreteTestSinkTask
    task.initialize(maxRetries, policy)
    task.writerManager    = wm              // private[sink] — subpackage access
    task.connectorTaskId  = connectorTaskId // public
    var thrown: Throwable = null
    try {
      task.handleTry(Try {
        task.handleErrors(wm.recommitPending()) // private[sink] — subpackage access
      })
    } catch { case t: Throwable => thrown = t }
    thrown
  }

  /** Like `runPut` but rethrows so tests can use `intercept[]`. */
  private def runPutThrow(wm: WriterManager[FakeFileMetadata], policy: ErrorPolicy): Unit = {
    val t = runPut(wm, policy)
    if (t != null) throw t
  }

  /**
   * Convenience: create a `WriterManager` containing a single uploading writer for `tp`
   * whose upload will fail because `file` does not exist on disk.
   */
  private def buildWMWithUploadingWriter(tp: TopicPartition, file: File): WriterManager[FakeFileMetadata] = {
    val pop    = new PendingOperationsProcessors(new InMemoryStorageInterface())
    val idxMgr = stubWriterIndexManager()
    val writer = buildUploadingWriter(tp, file, pop, idxMgr, Offset(99), Offset(150))
    val wm     = buildMinimalWriterManager(stubWmIndexManager())
    wm.putWriter(MapKey(tp, Map.empty), writer)
    wm
  }

  /**
   * Build a `Writer` pre-set to `Uploading` state.
   *
   * @param file            Staging file (may or may not exist; determines success/failure).
   * @param pop             `PendingOperationsProcessors` owning the upload storage.
   * @param idxMgr          `IndexManager` used by `Writer.commit`'s `fnIndexUpdate`.
   * @param committedOff    Last durably committed offset stored in `CommitState`.
   * @param uncommittedOff  Offset being committed by this `Uploading` operation.
   */
  private def buildUploadingWriter(
    tp:             TopicPartition,
    file:           File,
    pop:            PendingOperationsProcessors,
    idxMgr:         IndexManager,
    committedOff:   Offset,
    uncommittedOff: Offset,
  ): Writer[FakeFileMetadata] = {
    val objectKeyBuilder = mock[ObjectKeyBuilder]
    when(objectKeyBuilder.build(any[Offset], any[Long], any[Long])).thenReturn(
      CloudLocation(bucket, path = Some(s"data/${tp.topic.value}/${tp.partition}/final-${uncommittedOff.value}.jsonl")).asRight,
    )
    val formatWriter = mock[FormatWriter]
    when(formatWriter.complete()).thenReturn(().asRight)

    val writer = new Writer[FakeFileMetadata](
      tp,
      mock[CommitPolicy],
      idxMgr,
      stagingFilenameFn = () => file.asRight,
      objectKeyBuilder,
      formatWriterFn    = _ => formatWriter.asRight,
      mock[SchemaChangeDetector],
      pop,
      partitionKey     = None,
      lastSeekedOffset = Some(committedOff),
    )
    writer.forceWriteState(
      Uploading(
        CommitState(tp, Some(committedOff)),
        file,
        firstBufferedOffset     = Offset(committedOff.value + 1),
        uncommittedOffset       = uncommittedOff,
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 100L,
      ),
    )
    writer
  }

  /**
   * Build a minimal `WriterManager` whose lambda arguments are stubs.
   * Real writers are always injected via `putWriter`; the lambda functions are never called.
   */
  private def buildMinimalWriterManager(wmIdxMgr: IndexManager): WriterManager[FakeFileMetadata] =
    new WriterManager[FakeFileMetadata](
      commitPolicyFn              = _ => CloudCommitPolicy.Default.asRight,
      bucketAndPrefixFn           = _ => CloudLocation(bucket, Some("data/")).asRight,
      keyNamerFn                  = _ => mock[KeyNamer].asRight,
      stagingFilenameFn           = (_, _) => new File("/tmp/stub").asRight,
      objKeyBuilderFn             = (_, _) => mock[ObjectKeyBuilder],
      formatWriterFn              = (_, _) => mock[FormatWriter].asRight,
      indexManager                = wmIdxMgr,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = new PendingOperationsProcessors(new InMemoryStorageInterface()),
    )

  /**
   * `IndexManager` stub for use as the `WriterManager`'s shared index manager.
   * This instance is consulted only for bookkeeping in `getOffsetAndMeta` and `cleanUp`.
   */
  private def stubWmIndexManager(): IndexManager = {
    val im = mock[IndexManager]
    when(im.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(im.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(im.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    when(im.evictAllGranularLocks(any[TopicPartition])).thenAnswer((_: org.mockito.invocation.InvocationOnMock) => ())
    im
  }

  /**
   * `IndexManager` stub for use inside individual `Writer` instances.
   * This instance is used by `Writer.commit`'s `fnIndexUpdate`.
   */
  private def stubWriterIndexManager(): IndexManager = {
    val im = mock[IndexManager]
    when(im.indexingEnabled).thenReturn(true)
    when(im.update(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]])).thenReturn(
      Right(Some(Offset(150))),
    )
    when(
      im.updateForPartitionKey(
        any[TopicPartition],
        any[String],
        any[Option[Offset]],
        any[Option[PendingState]],
      ),
    ).thenReturn(Right(Some(Offset(150))))
    when(im.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    im
  }

  private def nonExistentFile(): File = new File("/nonexistent/cst-test/staging.tmp")

  private def makeTempFile(content: String): File = {
    val f = Files.createTempFile("cst-record-", ".tmp").toFile
    Files.write(f.toPath, content.getBytes(StandardCharsets.UTF_8))
    f.deleteOnExit()
    f
  }

  /**
   * Storage where `uploadFile` succeeds (blob lands in-memory) but `mvFile` always fails with a
   * `FileMoveError`, simulating a mid-chain Copy failure after the temp blob has been uploaded.
   * This pins the contract that a mid-chain Copy error is classified as `FatalCloudSinkError`
   * and, under `error.policy=RETRY`, propagates as `FatalConnectException` (not `RetriableException`).
   */
  private class MidChainCopyFailingStorage extends InMemoryStorageInterface() {
    override def mvFile(
      oldBucket: String,
      oldPath:   String,
      newBucket: String,
      newPath:   String,
      maybeEtag: Option[String],
    ): Either[FileMoveError, Unit] =
      Left(FileMoveError(new RuntimeException("transient mid-chain Copy failure"), oldPath, newPath))
  }

  /**
   * Minimal marker type satisfying `CC <: ConnectionConfig`.
   * No methods; `ConnectionConfig` is a marker interface with no members.
   */
  private class StubCC extends ConnectionConfig

  /**
   * Concrete `CloudSinkTask` whose abstract cloud-provider methods all throw
   * `UnsupportedOperationException` — they are never called because `start()` is
   * never invoked in these tests.  The only production methods exercised are
   * `handleErrors` (package-private in `sink`) and `handleTry` (from `ErrorHandler`).
   */
  private class ConcreteTestSinkTask
      extends CloudSinkTask[FakeFileMetadata, CloudSinkConfig[StubCC], StubCC, Unit](
        "test-connector",
        "",
        JarManifest.fromUrl(new URL("file:///not-a-jar")),
      ) {

    override def createClient(config: StubCC): Either[Throwable, Unit] = Right(())

    override def createStorageInterface(
      connectorTaskId: ConnectorTaskId,
      config:          CloudSinkConfig[StubCC],
      cloudClient:     Unit,
    ): StorageInterface[FakeFileMetadata] =
      throw new UnsupportedOperationException("not used in tests")

    override def convertPropsToConfig(
      connectorTaskId: ConnectorTaskId,
      props:           Map[String, String],
    ): Either[Throwable, CloudSinkConfig[StubCC]] =
      Left(new UnsupportedOperationException("not used in tests"))
  }
}
