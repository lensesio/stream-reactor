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
import io.circe.Encoder
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StringSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoOverwriteExistingObject
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailWriteAt
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig
import io.lenses.streamreactor.common.errors._
import io.lenses.streamreactor.common.errors.RetriableIntegrityException
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
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.ForcedWriteReason
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CloudCommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoIndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexFile
import io.lenses.streamreactor.connect.cloud.common.sink.seek.TwoInstanceHarness
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.OnWriteAt
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.apache.kafka.connect.errors.ConnectException
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.connect.errors.RetriableException
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.Assertion
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.IOException
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
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

  test(
    "live-commit NonExistingFileError throws FatalConnectException under error.policy=RETRY (not RetriableException)",
  ) {
    val wm     = buildWMWithUploadingWriter(tpA, nonExistentFile())
    val thrown = runPut(wm, RetryErrorPolicy())
    thrown shouldBe a[FatalConnectException]
    thrown should not be a[RetriableException]
  }

  test(
    "live-commit NonExistingFileError throws FatalConnectException under error.policy=NOOP (not silently swallowed)",
  ) {
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

    intercept[FatalConnectException](runPutThrow(wm, RetryErrorPolicy()))

    wm.writerCount shouldBe 0
    verify(im, times(1)).evictAllGranularLocks(tpA)
  }

  test("live-commit Fatal does NOT advance preCommit offset (indexes.enabled=true)") {
    val wm = buildWMWithUploadingWriter(tpA, nonExistentFile())

    intercept[FatalConnectException](runPutThrow(wm, RetryErrorPolicy()))

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

    intercept[FatalConnectException](runPutThrow(wm, RetryErrorPolicy()))

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

    intercept[FatalConnectException](runPutThrow(wm, RetryErrorPolicy()))

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

  // ── Unswallowable (integrity-sensitive) error policy matrix ────────────────────────────────
  //
  // These tests pin the contract that a `NonFatalCloudSinkError(swallowable=false)` (as returned
  // by `IndexManagerV2` transient branches) surfaces as `RetriableIntegrityException` through
  // `handleErrors` and then is handled correctly by each error policy:
  //   RETRY  → RetriableException(cause=RetriableIntegrityException)  — KC re-delivers the batch
  //   NOOP   → RetriableIntegrityException re-thrown as-is             — fail-fast, no silent skip
  //   THROW  → RetriableIntegrityException re-thrown as-is             — fail-fast

  test("unswallowable IndexManager error under error.policy=RETRY wraps in RetriableException") {
    val thrown = runHandleErrors(RetryErrorPolicy(), maxRetries = 5)
    thrown shouldBe a[RetriableException]
    Option(thrown.getCause) match {
      case Some(_: RetriableIntegrityException) => succeed
      case other => fail(s"Expected cause RetriableIntegrityException, got: $other")
    }
  }

  test(
    "unswallowable IndexManager error under error.policy=NOOP rethrows RetriableIntegrityException (not swallowed)",
  ) {
    val thrown = runHandleErrors(NoopErrorPolicy())
    thrown shouldBe a[RetriableIntegrityException]
  }

  test(
    "unswallowable IndexManager error under error.policy=THROW rethrows RetriableIntegrityException (not ConnectException)",
  ) {
    val thrown = runHandleErrors(ThrowErrorPolicy())
    thrown shouldBe a[RetriableIntegrityException]
  }

  // ── Granular-lock take-over failures at the error-policy boundary ─────────────────────────
  //
  // The errors below are produced by a real take-over between two index-manager instances that
  // share storage, then driven through the production handleErrors -> handleTry -> policy chain.

  /**
   * A owns `key` under an older epoch, B opens the partition, and `beforeEachTakeOver` runs before
   * each of B's take-over writes (`takeOverWrites` of them). Returns the error B's load ends with.
   */
  private def failedTakeOver(takeOverWrites: Int)(beforeEachTakeOver: TwoInstanceHarness => Int => Unit): SinkError = {
    val h   = new TwoInstanceHarness()
    val key = "date=2024-01-01"
    try {
      h.clock.set(1000L)
      h.a.open(Set(tpA)).value
      h.a.ensureGranularLock(tpA, key).value
      h.a.updateForPartitionKey(tpA, key, Some(Offset(5)), None).value
      h.b.open(Set(tpA)).value
      (1 to takeOverWrites).foreach { n =>
        val _ = h.storage.arm(
          OnWriteAt(
            h.bucket,
            h.granularLockPath(tpA, key),
            () => beforeEachTakeOver(h)(n),
            { case idx: IndexFile => idx.owner == h.lockUuid(1); case _ => false },
          ),
        )
      }
      h.b.getSeekedOffsetForPartitionKey(tpA, key).left.value
    } finally h.close()
  }

  private def runHandleErrorsWith(error: SinkError, policy: ErrorPolicy): Throwable = {
    val task = new ConcreteTestSinkTask
    task.initialize(5, policy)
    task.writerManager   = buildMinimalWriterManager(stubWmIndexManager())
    task.connectorTaskId = connectorTaskId
    var thrown: Throwable = null
    try {
      task.handleTry(Try {
        task.handleErrors(Left(error))
      })
    } catch { case t: Throwable => thrown = t }
    thrown
  }

  test("a granular-lock take-over that runs out of attempts is re-delivered under error.policy=RETRY") {
    val error = failedTakeOver(takeOverWrites = 3) { h => n =>
      val _ = h.a.updateForPartitionKey(tpA, "date=2024-01-01", Some(Offset(5L + n)), None).value
    }

    runHandleErrorsWith(error, RetryErrorPolicy()) shouldBe a[RetriableException]
  }

  test("a granular-lock take-over whose re-read fails is re-delivered under error.policy=RETRY") {
    val error = failedTakeOver(takeOverWrites = 1) { h => _ =>
      val path = h.granularLockPath(tpA, "date=2024-01-01")
      h.storage.deleteFiles(h.bucket, Seq(path)).value
    }

    runHandleErrorsWith(error, RetryErrorPolicy()) shouldBe a[RetriableException]
  }

  // ── Lock-load failures through a real WriterManager, delivered as Connect delivers ──────────
  //
  // One partition, PARTITIONBY on a single key. A previous owner left the master lock at 5 and the
  // key's granular lock under an older epoch. Records are delivered one per put() through the real
  // WriterManager / IndexManagerV2 and the production handleErrors -> handleTry -> policy chain,
  // and each put is followed by a preCommit, as Connect does. Every record is flushed on its own,
  // so each committed record has its own final object, named after its offset.

  private val loadKey:        String                      = "K"
  private val loadKeyField:   ValuePartitionField         = ValuePartitionField(PartitionNamePath("k"))
  private val loadKeyValues:  Map[PartitionField, String] = Map(loadKeyField -> loadKey)
  private def loadKeySegment: String                      = WriterManager.derivePartitionKey(loadKeyValues).get
  private def finalObject(first: Long): String =
    s"data/${tpA.topic.value}/${tpA.partition}/$loadKeySegment-$first.json"
  private def finalObjects(h: TwoInstanceHarness): Set[String] =
    h.storage.keysUnder(h.bucket, "data/").filter(_.endsWith(".json")).toSet

  private def seedLock(h: TwoInstanceHarness, path: String, idx: IndexFile): Unit = {
    implicit val enc: Encoder[IndexFile] = IndexFile.indexFileEncoder
    val _ = h.storage.writeBlobToFile(h.bucket, path, NoOverwriteExistingObject(idx)).value
  }

  /** The previous owner's master lock: floor 5, epoch 500 (older than any instance's). */
  private def seedPreviousOwner(h: TwoInstanceHarness): Unit =
    seedLock(h, h.masterLockPath(tpA), IndexFile("previous-owner", Some(Offset(5)), None, Some(500L)))

  /**
   * Connect, reduced to one partition: delivers the next available offset at or after the
   * consumer position in its own put(), then calls preCommit. A `RetriableException` re-delivers
   * the same record to the same task. Any other exception fails the task: the next incarnation
   * (the harness's next instance, sharing storage) opens the partition and the consumer resumes
   * from the offset open() seeks to. `available` lists the offsets the topic still holds.
   */
  private final class ConnectLoop(h: TwoInstanceHarness, policy: ErrorPolicy, maxRetries: Int) {
    val thrown:              scala.collection.mutable.Buffer[Throwable] = scala.collection.mutable.Buffer.empty
    var incarnation:         Int                                        = -1
    var lastCommittedOffset: Option[Long]                               = None

    private var task: ConcreteTestSinkTask            = _
    private var wm:   WriterManager[FakeFileMetadata] = _

    private def startIncarnation(): Long = {
      incarnation += 1
      val im   = h.instances(incarnation)
      val seek = im.open(Set(tpA)).value.get(tpA).flatten.map(_.value)
      wm   = buildRealWriterManager(h, im)
      task = new ConcreteTestSinkTask
      task.initialize(maxRetries, policy)
      task.writerManager   = wm
      task.connectorTaskId = connectorTaskId
      seek.orElse(lastCommittedOffset).getOrElse(0L)
    }

    def run(available: Seq[Long]): Unit = {
      var position = startIncarnation()
      var puts     = 0
      while (available.exists(_ >= position)) {
        puts += 1
        require(puts <= 20, s"no progress after $puts puts; thrown=$thrown")
        val record = available.filter(_ >= position).min
        var error: Throwable = null
        try {
          val _ =
            task.handleTry(Try(task.handleErrors(wm.write(tpA.withOffset(Offset(record)), loadKeyMessage(record)))))
        } catch { case t: Throwable => error = t }
        error match {
          case null =>
            position = record + 1
            wm.preCommit(Map(tpA -> new OffsetAndMetadata(position))).get(tpA).foreach { om =>
              lastCommittedOffset = Some(om.offset())
            }
          case retriable: RetriableException => thrown += retriable
          case failed =>
            thrown += failed
            position = startIncarnation()
        }
      }
    }
  }

  private def loadKeyMessage(offset: Long): MessageDetail =
    MessageDetail(
      key       = StringSinkData("k", None),
      value     = StringSinkData("v", None),
      headers   = Map.empty,
      timestamp = None,
      topic     = tpA.topic,
      partition = tpA.partition,
      offset    = Offset(offset),
    )

  /** A real WriterManager over `im`, flushing every record into its own final object. */
  private def buildRealWriterManager(h: TwoInstanceHarness, im: IndexManager): WriterManager[FakeFileMetadata] = {
    val keyNamer = mock[KeyNamer]
    when(keyNamer.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(loadKeyValues.asRight[SinkError])
    val formatWriter = mock[FormatWriter]
    when(formatWriter.write(any[MessageDetail])).thenReturn(().asRight)
    when(formatWriter.complete()).thenReturn(().asRight)
    when(formatWriter.rolloverFileOnSchemaChange()).thenReturn(false)
    val flushEveryRecord = mock[CommitPolicy]
    when(flushEveryRecord.shouldFlush(any[CommitContext])).thenReturn(true)

    new WriterManager[FakeFileMetadata](
      commitPolicyFn    = _ => flushEveryRecord.asRight,
      bucketAndPrefixFn = h.bucketAndPrefix,
      keyNamerFn        = _ => keyNamer.asRight,
      stagingFilenameFn = (_, _) => makeTempFile("payload").asRight,
      objKeyBuilderFn = (_, _) => {
        val okb = mock[ObjectKeyBuilder]
        when(okb.build(any[Offset], any[Offset], any[Long], any[Long], any[Long])).thenAnswer {
          (first: Offset, _: Offset, _: Long, _: Long, _: Long) =>
            CloudLocation(h.bucket, path = Some(finalObject(first.value))).asRight
        }
        okb
      },
      formatWriterFn              = (_, _) => formatWriter.asRight,
      indexManager                = im,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = new PendingOperationsProcessors(h.storage),
    )
  }

  /**
   * Every one of the three take-over writes for the key fails transiently, then writes recover.
   * Under RETRY with `maxRetries` the record that loaded the key is re-delivered to the same task,
   * the next load takes the key over afresh, and each record is committed once.
   */
  private def spentTakeOverBudgetUnderRetry(maxRetries: Int): Assertion = {
    val h = new TwoInstanceHarness(instanceCount = 3)
    try {
      seedPreviousOwner(h)
      seedLock(h,
               h.granularLockPath(tpA, loadKeySegment),
               IndexFile("previous-owner", Some(Offset(5)), None, Some(500L)),
      )
      (1 to 3).foreach { _ =>
        val _ = h.storage.arm(
          FailWriteAt(
            h.bucket,
            h.granularLockPath(tpA, loadKeySegment),
            Some(UploadFailedError(new java.net.SocketTimeoutException("Read timed out"), new File("lock"))),
          ),
        )
      }

      val connect = new ConnectLoop(h, RetryErrorPolicy(), maxRetries)
      connect.run(available = Seq(6L, 7L))

      connect.thrown.map(_.getClass) shouldBe Seq(classOf[RetriableException])
      connect.incarnation shouldBe 0
      h.metrics(0).getGranularLockOwnershipBumpRaces shouldBe 3L
      h.metrics(0).getGranularLockOwnershipBumps shouldBe 1L
      finalObjects(h) shouldBe Set(finalObject(6), finalObject(7))
      connect.lastCommittedOffset shouldBe Some(8L)
      h.readLock(h.granularLockPath(tpA, loadKeySegment)).value.wrappedObject shouldBe
        IndexFile(h.lockUuid(0), Some(Offset(7)), None, Some(1000L))
    } finally h.close()
  }

  test(
    "a take-over whose writes all fail transiently is retried, not fatal, under error.policy=RETRY and commits once writes recover",
  ) {
    spentTakeOverBudgetUnderRetry(maxRetries = 5)
  }

  test(
    "a take-over whose writes all fail transiently is retried under error.policy=RETRY with unlimited retries and commits once writes recover",
  ) {
    spentTakeOverBudgetUnderRetry(maxRetries = -1)
  }

  /**
   * Drives a `Left(NonFatalCloudSinkError.unswallowable(...))` through the REAL production
   * `CloudSinkTask.handleErrors` → `handleTry` → `ErrorPolicy` chain.
   *
   * Returns the exception rethrown by the error policy, or `null` if none.
   */
  private def runHandleErrors(policy: ErrorPolicy, maxRetries: Int = 5): Throwable = {
    val task = new ConcreteTestSinkTask
    task.initialize(maxRetries, policy)
    task.connectorTaskId = connectorTaskId
    val unswallowableErr =
      Left(NonFatalCloudSinkError.unswallowable("simulated transient granular lock read timeout", None))
    var thrown: Throwable = null
    try {
      task.handleTry(scala.util.Try {
        task.handleErrors(unswallowableErr)
      })
    } catch { case t: Throwable => thrown = t }
    thrown
  }

  // ── BatchCloudSinkError batch-matrix tests ─────────────────────────────────────────────────
  // Pins CloudSinkTask.handleErrors when fed a BatchCloudSinkError.
  // The single-error variants above already cover FatalCloudSinkError and NonFatalCloudSinkError.
  // These tests cover the three distinct *batch* shapes.

  test(
    "BatchCloudSinkError with fatal + unswallowable non-fatal throws FatalConnectException under all policies (fatal precedence); cleanUp scoped to fatal TPs only; no RetriableIntegrityException leaked",
  ) {
    val fatalErr   = FatalCloudSinkError("fatal-for-tpA", tpA)
    val unswallErr = NonFatalCloudSinkError.unswallowable("integrity-error-for-tpB", None)
    val batchError = BatchCloudSinkError(fatal = Set(fatalErr), nonFatal = Set(unswallErr))

    Seq(RetryErrorPolicy(), NoopErrorPolicy(), ThrowErrorPolicy()).foreach { policy =>
      val im = stubWmIndexManager()
      val wm = buildMinimalWriterManager(im)

      val thrown = runHandleErrorsWithBatch(batchError, wm, policy)

      withClue(s"policy=$policy: ") {
        thrown shouldBe a[FatalConnectException]
        thrown should not be a[RetriableException]
        thrown should not be a[RetriableIntegrityException]

        // cleanUp is scoped to FATAL TPs only (tpA). tpB (non-fatal) must NOT be cleaned up.
        verify(im, times(1)).evictAllGranularLocks(tpA)
        verify(im, never).evictAllGranularLocks(tpB)
      }
    }
  }

  test(
    "BatchCloudSinkError with only unswallowable non-fatal entries throws RetriableIntegrityException; under RETRY wraps in RetriableException; under NOOP/THROW rethrown as-is; cleanUp NOT called",
  ) {
    val u1         = NonFatalCloudSinkError.unswallowable("u1", None)
    val u2         = NonFatalCloudSinkError.unswallowable("u2", None)
    val batchError = BatchCloudSinkError(fatal = Set.empty, nonFatal = Set(u1, u2))

    val retryIm  = stubWmIndexManager()
    val retryWm  = buildMinimalWriterManager(retryIm)
    val retryErr = runHandleErrorsWithBatch(batchError, retryWm, RetryErrorPolicy(), maxRetries = 5)
    retryErr shouldBe a[RetriableException]
    Option(retryErr.getCause) match {
      case Some(_: RetriableIntegrityException) => succeed
      case other => fail(s"Expected RetriableIntegrityException cause, got: $other")
    }
    verify(retryIm, never).evictAllGranularLocks(any[TopicPartition])

    val noopIm  = stubWmIndexManager()
    val noopWm  = buildMinimalWriterManager(noopIm)
    val noopErr = runHandleErrorsWithBatch(batchError, noopWm, NoopErrorPolicy())
    noopErr shouldBe a[RetriableIntegrityException]
    verify(noopIm, never).evictAllGranularLocks(any[TopicPartition])

    val throwIm  = stubWmIndexManager()
    val throwWm  = buildMinimalWriterManager(throwIm)
    val throwErr = runHandleErrorsWithBatch(batchError, throwWm, ThrowErrorPolicy())
    throwErr shouldBe a[RetriableIntegrityException]
    verify(throwIm, never).evictAllGranularLocks(any[TopicPartition])
  }

  test(
    "BatchCloudSinkError with only swallowable non-fatal entries: under NOOP swallowed; under RETRY/THROW throws ConnectException (not Fatal); cleanUp NOT called; subsequent put proceeds normally",
  ) {
    val s1         = NonFatalCloudSinkError("s1", None, swallowable = true)
    val s2         = NonFatalCloudSinkError("s2", None, swallowable = true)
    val batchError = BatchCloudSinkError(fatal = Set.empty, nonFatal = Set(s1, s2))

    // NOOP: should be swallowed — null thrown (no exception)
    val noopIm  = stubWmIndexManager()
    val noopWm  = buildMinimalWriterManager(noopIm)
    val noopErr = runHandleErrorsWithBatch(batchError, noopWm, NoopErrorPolicy())
    noopErr shouldBe null
    verify(noopIm, never).evictAllGranularLocks(any[TopicPartition])

    // RETRY: ConnectException, NOT FatalConnectException, NOT RetriableIntegrityException
    val retryIm  = stubWmIndexManager()
    val retryWm  = buildMinimalWriterManager(retryIm)
    val retryErr = runHandleErrorsWithBatch(batchError, retryWm, RetryErrorPolicy())
    if (retryErr != null) {
      retryErr should not be a[FatalConnectException]
      retryErr should not be a[RetriableIntegrityException]
    }
    verify(retryIm, never).evictAllGranularLocks(any[TopicPartition])

    // THROW: ditto
    val throwIm  = stubWmIndexManager()
    val throwWm  = buildMinimalWriterManager(throwIm)
    val throwErr = runHandleErrorsWithBatch(batchError, throwWm, ThrowErrorPolicy())
    if (throwErr != null) {
      throwErr should not be a[FatalConnectException]
      throwErr should not be a[RetriableIntegrityException]
    }
    verify(throwIm, never).evictAllGranularLocks(any[TopicPartition])
  }

  /**
   * Drives a pre-built `BatchCloudSinkError` through the REAL production
   * `CloudSinkTask.handleErrors` → `handleTry` → `ErrorPolicy` chain.
   *
   * Returns the exception rethrown by the error policy, or `null` if none.
   */
  private def runHandleErrorsWithBatch(
    batchError: BatchCloudSinkError,
    wm:         WriterManager[FakeFileMetadata],
    policy:     ErrorPolicy,
    maxRetries: Int = 5,
  ): Throwable = {
    val task = new ConcreteTestSinkTask
    task.initialize(maxRetries, policy)
    task.writerManager   = wm
    task.connectorTaskId = connectorTaskId
    var thrown: Throwable = null
    try {
      task.handleTry(scala.util.Try {
        task.handleErrors(Left(batchError))
      })
    } catch { case t: Throwable => thrown = t }
    thrown
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
    task.writerManager   = wm              // private[sink] — subpackage access
    task.connectorTaskId = connectorTaskId // public
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
    when(objectKeyBuilder.build(any[Offset], any[Offset], any[Long], any[Long], any[Long])).thenReturn(
      CloudLocation(bucket,
                    path = Some(s"data/${tp.topic.value}/${tp.partition}/final-${uncommittedOff.value}.jsonl"),
      ).asRight,
    )
    val formatWriter = mock[FormatWriter]
    when(formatWriter.complete()).thenReturn(().asRight)

    val writer = new Writer[FakeFileMetadata](
      tp,
      mock[CommitPolicy],
      idxMgr,
      stagingFilenameFn = () => file.asRight,
      objectKeyBuilder,
      formatWriterFn = _ => formatWriter.asRight,
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
        recordCount             = 1L,
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

  // ── Persistent retry exhaustion + staging-file preservation ──────────────────────────────────
  // docs/datalake-exactly-once-partitionby.md

  test(
    "Persistent transient UploadFailedError exhausts RetryErrorPolicy budget (maxRetries=3): first two puts throw RetriableException; third throws ConnectException; staging file preserved throughout",
  ) {
    // Storage that always returns UploadFailedError (transient, swallowable=true).
    val storage     = new AlwaysTransientUploadFailingStorage()
    val pop         = new PendingOperationsProcessors(storage)
    val stagingFile = makeTempFile("retry-exhaustion-records")
    stagingFile.exists() shouldBe true

    val idxMgr = stubWriterIndexManager()
    val writer = buildUploadingWriter(tpA, stagingFile, pop, idxMgr, Offset(99), Offset(150))
    val im     = stubWmIndexManager()
    val wm     = buildMinimalWriterManager(im)
    wm.putWriter(MapKey(tpA, Map.empty), writer)

    // Single task instance — retry counter persists across put() calls.
    val task = new ConcreteTestSinkTask
    task.initialize(3, RetryErrorPolicy())
    task.writerManager   = wm
    task.connectorTaskId = connectorTaskId

    def doPut(): Throwable = {
      var thrown: Throwable = null
      try {
        task.handleTry(scala.util.Try {
          task.handleErrors(wm.recommitPending())
        })
      } catch { case t: Throwable => thrown = t }
      thrown
    }

    // Call 1: retries=3→2 → RetriableException
    val err1 = doPut()
    err1 shouldBe a[RetriableException]
    err1 should not be a[FatalConnectException]
    stagingFile.exists() shouldBe true

    // Call 2: retries=2→1 → RetriableException
    val err2 = doPut()
    err2 shouldBe a[RetriableException]
    stagingFile.exists() shouldBe true

    // Call 3: retries=1→0 → ConnectException (exhausted; NOT RetriableException, NOT FatalConnectException)
    val err3 = doPut()
    err3 should not be null
    err3 should not be a[RetriableException]
    err3 should not be a[FatalConnectException]
    err3 shouldBe a[ConnectException]

    // Staging file is preserved at exhaustion — the writer did NOT roll back (swallowable=true).
    stagingFile.exists() shouldBe true

    // On restart, IndexManagerV2.open would seek the consumer back to the master-lock offset.
    // That path is already pinned by WriterUploadRetryRecoveryTest + IndexManagerV2Test:
    // the WriterManager.cleanUp contract (called by Connect after task failure) evicts writers
    // while preserving seekedOffsets + topicPartitionToETags (LC-2451). IndexManagerV2.open
    // then replays the master-lock offset as the consume-from floor.
    val _ = stagingFile.delete()
  }

  /**
   * Storage where `uploadFile` always returns a transient `UploadFailedError`,
   * simulating a persistent cloud I/O error where the staging file is present locally
   * but the upload never completes.  Results in `NonFatalCloudSinkError(swallowable=true)`
   * (not `NonExistingFileError`, which would escalate to Fatal).
   */
  private class AlwaysTransientUploadFailingStorage extends InMemoryStorageInterface() {
    override def uploadFile(
      source: UploadableFile,
      bucket: String,
      path:   String,
    ): Either[UploadError, String] =
      UploadFailedError(new RuntimeException("transient upload failure"), new File(path)).asLeft
  }

  // ── recommitPending across polls + empty poll + distinct tempFileUuid ────────────────────────
  // docs/datalake-sinks-write-pipeline.md

  test(
    "second poll calls recommitPending first, retries the previously-failed upload, and then the writer transitions to NoWriter",
  ) {
    val storage = new OneShotTransientUploadFailingStorage()
    val pop     = new PendingOperationsProcessors(storage)
    val tmpFile = makeTempFile("poll-retry-records")

    val idxMgr = stubWriterIndexManager()
    val writer = buildUploadingWriter(tpA, tmpFile, pop, idxMgr, Offset(99), Offset(150))
    val wm     = buildMinimalWriterManager(stubWmIndexManager())
    wm.putWriter(MapKey(tpA, Map.empty), writer)

    // Poll 1: upload fails transiently → RetriableException; writer stays Uploading.
    val thrown1 = runPut(wm, RetryErrorPolicy())
    thrown1 shouldBe a[RetriableException]
    writer.currentWriteState shouldBe a[Uploading]

    // Poll 2: recommitPending runs first; one-shot failure exhausted → upload succeeds; writer → NoWriter.
    val thrown2 = runPut(wm, RetryErrorPolicy())
    thrown2 shouldBe null // succeeded
    writer.currentWriteState shouldBe a[NoWriter]

    // Payload landed at the final path.
    storage.snapshot(bucket).keys.exists(_.contains("final-")) shouldBe true
    val _ = tmpFile.delete()
  }

  test(
    "put(emptyList) still triggers recommitPending; a writer in Uploading completes its pending upload even when the record list is empty",
  ) {
    val storage = new OneShotTransientUploadFailingStorage()
    val pop     = new PendingOperationsProcessors(storage)
    val tmpFile = makeTempFile("empty-poll-records")

    val idxMgr = stubWriterIndexManager()
    val writer = buildUploadingWriter(tpA, tmpFile, pop, idxMgr, Offset(99), Offset(150))
    val wm     = buildMinimalWriterManager(stubWmIndexManager())
    wm.putWriter(MapKey(tpA, Map.empty), writer)

    // "Put" 1 (empty-equivalent): recommitPending fails; writer stays Uploading.
    val task = new ConcreteTestSinkTask
    task.initialize(5, RetryErrorPolicy())
    task.writerManager   = wm
    task.connectorTaskId = connectorTaskId
    var thrown: Throwable = null
    try {
      task.handleTry(scala.util.Try {
        task.handleErrors(wm.recommitPending())
      })
    } catch { case t: Throwable => thrown = t }
    thrown shouldBe a[RetriableException]
    writer.currentWriteState shouldBe a[Uploading]

    // "Put" 2 (empty list): recommitPending alone drives the pending upload to completion.
    try {
      task.handleTry(scala.util.Try {
        task.handleErrors(wm.recommitPending()) // empty batch → only recommitPending runs
      })
    } catch { case t: Throwable => thrown = t }
    thrown = null // reset — second call should not throw
    writer.currentWriteState shouldBe a[NoWriter]
    val _ = tmpFile.delete()
  }

  test(
    "each Writer.commit attempt generates a distinct .temp-upload/<uuid> path (regression guard for tempFileUuid uniqueness)",
  ) {
    // Two consecutive commit calls on the same Uploading writer must each generate a distinct
    // temp UUID, preventing the second upload from clobbering the first's temp path.
    val storage = new RecordingUploadPathsStorage()
    val pop     = new PendingOperationsProcessors(storage)
    val tmpFile = makeTempFile("uuid-test-records")

    val idxMgr = stubWriterIndexManager()
    val writer = buildUploadingWriter(tpA, tmpFile, pop, idxMgr, Offset(99), Offset(150))
    val wm     = buildMinimalWriterManager(stubWmIndexManager())
    wm.putWriter(MapKey(tpA, Map.empty), writer)

    // First commit: upload succeeds → writer → NoWriter; temp path uuid1 recorded.
    val result1 = wm.recommitPending()
    result1 shouldBe a[Right[_, _]]
    storage.recordedUploadPaths should have size 1

    // Force writer back to Uploading with a fresh staging file to drive a second commit.
    val tmpFile2 = makeTempFile("uuid-test-records-2")
    writer.forceWriteState(
      Uploading(
        CommitState(tpA, Some(Offset(99))),
        tmpFile2,
        firstBufferedOffset     = Offset(100),
        uncommittedOffset       = Offset(151),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 100L,
        recordCount             = 1L,
      ),
    )

    // Second commit: must use a DIFFERENT UUID for the temp path.
    val result2 = wm.recommitPending()
    result2 shouldBe a[Right[_, _]]
    storage.recordedUploadPaths should have size 2

    val uuid1 = extractUuid(storage.recordedUploadPaths(0))
    val uuid2 = extractUuid(storage.recordedUploadPaths(1))
    uuid1 should not be empty
    uuid2 should not be empty
    uuid1 should not equal uuid2

    val _ = tmpFile.delete()
    val _ = tmpFile2.delete()
  }

  private def extractUuid(path: String): String = {
    // Temp paths look like .temp-upload/<topic>/<partition>/<uuid><suffix>
    val tempUploadSegment = ".temp-upload/"
    val idx               = path.indexOf(tempUploadSegment)
    if (idx < 0) ""
    else {
      val rest = path.substring(idx + tempUploadSegment.length)
      // The UUID is after <topic>/<partition>/
      rest.split("/").drop(2).headOption.getOrElse("").takeWhile(_ != '.')
    }
  }

  /** One-shot transient upload failure: first upload call returns UploadFailedError; subsequent calls succeed. */
  private class OneShotTransientUploadFailingStorage extends InMemoryStorageInterface() {
    private val hasFailedOnce = new java.util.concurrent.atomic.AtomicBoolean(false)

    override def uploadFile(
      source: UploadableFile,
      bucket: String,
      path:   String,
    ): Either[UploadError, String] =
      if (hasFailedOnce.compareAndSet(false, true))
        UploadFailedError(new RuntimeException("one-shot transient upload failure"), new File(path)).asLeft
      else
        super.uploadFile(source, bucket, path)
  }

  /** Storage that records all attempted upload paths before delegating to in-memory. */
  private class RecordingUploadPathsStorage extends InMemoryStorageInterface() {
    val recordedUploadPaths: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer.empty

    override def uploadFile(
      source: UploadableFile,
      bucket: String,
      path:   String,
    ): Either[UploadError, String] = {
      recordedUploadPaths += path
      super.uploadFile(source, bucket, path)
    }
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
  test(
    "REPRO: IOException from FormatWriter.write throws FatalConnectException (not RetriableException) under error.policy=RETRY",
  ) {
    val ioException    = new IOException("No space left on device")
    val ioFormatWriter = mock[FormatWriter]
    when(ioFormatWriter.write(any[MessageDetail])).thenReturn(Left(ioException))
    when(ioFormatWriter.getPointer).thenReturn(0L)
    when(ioFormatWriter.rolloverFileOnSchemaChange()).thenReturn(false)
    when(ioFormatWriter.complete()).thenReturn(().asRight)

    val tmpFile = makeTempFile("repro-ioexception-write")
    tmpFile.exists() shouldBe true

    val idxMgr = stubWriterIndexManager()
    val writer = new Writer[FakeFileMetadata](
      tpA,
      mock[CommitPolicy],
      idxMgr,
      stagingFilenameFn = () => tmpFile.asRight,
      mock[ObjectKeyBuilder],
      formatWriterFn = _ => ioFormatWriter.asRight,
      mock[SchemaChangeDetector],
      new PendingOperationsProcessors(new InMemoryStorageInterface()),
      partitionKey     = None,
      lastSeekedOffset = Some(Offset(99)),
    )
    writer.forceWriteState(
      Writing(
        CommitState(tpA, Some(Offset(99))),
        ioFormatWriter,
        tmpFile,
        firstBufferedOffset     = Offset(100),
        uncommittedOffset       = Offset(100),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 1L,
      ),
    )

    val im = stubWmIndexManager()
    val wm = buildMinimalWriterManager(im)
    wm.putWriter(MapKey(tpA, Map.empty), writer)
    wm.writerCount shouldBe 1

    val msgDetail = MessageDetail(
      key       = NullSinkData(None),
      value     = NullSinkData(None),
      headers   = Map.empty,
      timestamp = Some(Instant.ofEpochMilli(1000L)),
      topic     = tpA.topic,
      partition = tpA.partition,
      offset    = Offset(101),
    )

    val task = new ConcreteTestSinkTask
    task.initialize(5, RetryErrorPolicy())
    task.writerManager   = wm
    task.connectorTaskId = connectorTaskId

    val thrown = {
      var t: Throwable = null
      try {
        task.handleTry(Try {
          task.handleErrors(writer.write(msgDetail))
        })
      } catch { case ex: Throwable => t = ex }
      t
    }

    thrown shouldBe a[FatalConnectException]
    thrown should not be a[RetriableException]

    wm.writerCount shouldBe 0
    verify(im, times(1)).evictAllGranularLocks(tpA)
    val _ = tmpFile.delete()
  }

  // close(partitions) must hand the IndexManager exactly those partitions, so a batch temp sweep
  // that is already mid-pass stops deleting under them, and nothing else is released.
  test("close(partitions) releases exactly those partitions on the IndexManager") {
    val im   = mock[IndexManager]
    val task = new ConcreteTestSinkTask
    task.connectorTaskId = connectorTaskId
    task.indexManager    = im

    task.close(java.util.Arrays.asList(tpA.toKafka, tpB.toKafka))

    verify(im, times(1)).closePartitions(Set(tpA, tpB))
  }

  // ── a fenced partition fails the task on the next put() ─────────

  private def fencedPut(
    policy:     ErrorPolicy,
    maxRetries: Int,
    records:    java.util.Collection[org.apache.kafka.connect.sink.SinkRecord],
  ): (Throwable, WriterManager[FakeFileMetadata], ConcreteTestSinkTask) = {
    val task = new ConcreteTestSinkTask
    task.initialize(maxRetries, policy)
    val wm = mock[WriterManager[FakeFileMetadata]]
    val im = mock[IndexManager]
    when(im.fencedPartitions).thenReturn(Set(tpA))
    when(im.fencingReason(tpA)).thenReturn(Some("lock k epoch 9 > held epoch 3"))
    task.writerManager   = wm
    task.indexManager    = im
    task.connectorTaskId = connectorTaskId
    (intercept[Throwable](task.put(records)), wm, task)
  }

  private val oneRecord: java.util.Collection[org.apache.kafka.connect.sink.SinkRecord] =
    java.util.List.of(new org.apache.kafka.connect.sink.SinkRecord(tpA.topic.value, 0, null, null, null, "v", 7L))

  // The fence check runs before `handleTry`, so no error policy ever sees the failure: NOOP would
  // swallow it, RETRY would re-deliver it (forever with unlimited retries). The untouched retry
  // count shows it never went through the policy. THROW is not listed: it rethrows either way.
  Seq[(String, ErrorPolicy, Int, java.util.Collection[org.apache.kafka.connect.sink.SinkRecord])](
    ("an empty put() under NOOP", NoopErrorPolicy(), 5, java.util.List.of[org.apache.kafka.connect.sink.SinkRecord]()),
    ("a non-empty put() under NOOP", NoopErrorPolicy(), 5, oneRecord),
    ("a non-empty put() under RETRY with unlimited retries", RetryErrorPolicy(), Int.MaxValue, oneRecord),
  ).foreach {
    case (name, policy, maxRetries, records) =>
      test(s"$name on a fenced partition throws FatalConnectException before any work, bypassing the policy") {
        val (thrown, wm, task) = fencedPut(policy, maxRetries, records)

        thrown shouldBe a[FatalConnectException]
        thrown should not be a[RetriableIntegrityException]
        thrown.getMessage should include(tpA.toString)
        thrown.getMessage should include("lock k epoch 9 > held epoch 3")
        thrown.getMessage should include(connectorTaskId.lockUuid)
        org.mockito.Mockito.verifyNoInteractions(wm)
        task.getErrorTrackerRetries shouldBe maxRetries
      }
  }

  test("put() on a task with no fenced partitions does not throw for fencing") {
    val task = new ConcreteTestSinkTask
    task.initialize(5, NoopErrorPolicy())
    val wm = mock[WriterManager[FakeFileMetadata]]
    val im = mock[IndexManager]
    when(im.fencedPartitions).thenReturn(Set.empty[TopicPartition])
    when(wm.recommitPending()).thenReturn(().asRight)
    when(wm.commitFlushableWriters()).thenReturn(().asRight)
    task.writerManager   = wm
    task.indexManager    = im
    task.connectorTaskId = connectorTaskId

    task.put(java.util.List.of[org.apache.kafka.connect.sink.SinkRecord]())

    verify(wm).recommitPending()
  }

  // ── close(partitions) is exception-safe (real-path close tests: CooperativeRevokeCloseTest) ─────────

  test("close() still releases the closed partitions when closing the writers throws") {
    val task = new ConcreteTestSinkTask
    task.initialize(5, NoopErrorPolicy())
    val wm = mock[WriterManager[FakeFileMetadata]]
    val im = mock[IndexManager]
    when(wm.close(any[Set[TopicPartition]], any[ForcedWriteReason])).thenThrow(new IllegalStateException(
      "writer close failed",
    ))
    task.writerManager   = wm
    task.indexManager    = im
    task.connectorTaskId = connectorTaskId

    intercept[IllegalStateException](task.close(java.util.List.of(tpB.toKafka)))

    verify(wm).close(Set(tpB), ForcedWriteReason.Revoke)
    verify(im).closePartitions(Set(tpB))
  }

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
