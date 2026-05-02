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
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CloudCommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.TopicPartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexFilenames
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexManagerV1
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Integration tests for the upload-retry data-loss fix in
 * [[io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors]].
 *
 * Wires a real `Writer` against a real `PendingOperationsProcessors` and a real (subclassed)
 * `InMemoryStorageInterface`, with the index manager mocked because its update/CAS
 * concerns are orthogonal to the bug under test (covered by `IndexManagerV2Test` and the
 * `ExactlyOnceScenarioTest` end-to-end harness).
 *
 * Two distinct cancellation contexts:
 *
 *   - Live commit (`Writer.commit` -> `processPendingOperations` with
 *     `escalateOnCancel=true`): A `NonExistingFileError` on Upload escalates to
 *     `FatalCloudSinkError`. The writer stays in `Uploading` (the for-comprehension
 *     short-circuits before `toNoWriter`); the production disposal path is
 *     `BatchCloudSinkError -> handleErrors(rollBack=true) -> WriterManager.cleanUp(tp)
 *     -> Writer.close()` which deletes the already-missing staging file harmlessly,
 *     followed by task restart and `IndexManagerV2.open` seeking back from the master lock.
 *   - Dead-worker recovery (`IndexManagerV2.open`, `loadGranularLock`,
 *     `resolveAndCacheGranularLock` with the default `escalateOnCancel=false`):
 *     `NonExistingFileError` gracefully clears the stale `PendingState` via
 *     `fnIndexUpdate(_, oldCommittedOffset, None)`. (Tested in `IndexManagerV2Test`.)
 *
 * The recommit entry point ALSO passes `escalateOnCancel=true` because
 * `WriterCommitManager.commitPending` invokes `Writer.commit` for any writer with
 * `hasPendingUpload=true`. A missing staging file detected during recommit therefore
 * Fatals exactly like the live commit path.
 */
class WriterUploadRetryRecoveryTest
    extends AnyFunSuiteLike
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with EitherValues {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("retry-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => cats.data.Validated.fromEither(Right(location))

  private val bucket            = "test-bucket"
  private val finalPath         = "data/topic-x/0/final-100.jsonl"
  private val topicPartition    = Topic("topic-x").withPartition(0)
  private val firstBufferedOff  = Offset(100)
  private val uncommittedOffset = Offset(150)
  private val finalKeyLocation  = CloudLocation(bucket = bucket, path = Some(finalPath))

  test(
    "transient upload failure leaves writer in Uploading; recommitPending recovers without data loss",
  ) {
    runScenario(partitionKey = None)
  }

  test(
    "missing local file (live commit) escalates to FatalCloudSinkError; writer stays in Uploading for cleanUp to dispose",
  ) {
    runCancelledUploadScenario(partitionKey = None)
  }

  test("Writer.commit always passes escalateOnCancel=true to processPendingOperations (non-PARTITIONBY)") {
    structuralEscalateOnCancelAssertion(partitionKey = None)
  }

  test("Writer.commit always passes escalateOnCancel=true to processPendingOperations (PARTITIONBY)") {
    structuralEscalateOnCancelAssertion(partitionKey = Some("date=2026-05-01"))
  }

  test(
    "recommitPending after staging-dir wipe escalates to FatalCloudSinkError (SECOND live-escalate entry point)",
  ) {
    val storage = new AlwaysMissingFileStorage()
    val pop     = new PendingOperationsProcessors(storage)

    val missingFile = new File("/nonexistent/recommit-test/staging.tmp")
    missingFile.exists() shouldBe false

    val indexManager = mock[IndexManager]
    when(indexManager.indexingEnabled).thenReturn(true)
    when(indexManager.update(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]])).thenReturn(
      Some(Offset(99)).asRight[SinkError],
    )

    val formatWriter = mock[FormatWriter]
    when(formatWriter.complete()).thenReturn(().asRight)

    val objectKeyBuilder = mock[ObjectKeyBuilder]
    when(objectKeyBuilder.build(any[Offset], any[Long], any[Long])).thenReturn(finalKeyLocation.asRight)

    val writer = new Writer[FakeFileMetadata](
      topicPartition,
      mock[CommitPolicy],
      indexManager,
      stagingFilenameFn = () => missingFile.asRight,
      objectKeyBuilder,
      formatWriterFn    = _ => formatWriter.asRight,
      mock[SchemaChangeDetector],
      pop,
      partitionKey     = None,
      lastSeekedOffset = Some(Offset(99)),
    )
    writer.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(Offset(99))),
        missingFile,
        firstBufferedOffset     = firstBufferedOff,
        uncommittedOffset       = uncommittedOffset,
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 100L,
      ),
    )

    val wmIdxMgr = mock[IndexManager]
    when(wmIdxMgr.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(wmIdxMgr.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(wmIdxMgr.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    when(wmIdxMgr.evictAllGranularLocks(any[TopicPartition])).thenAnswer((_: org.mockito.invocation.InvocationOnMock) => ())

    val wm = buildMinimalWriterManager(wmIdxMgr, pop)
    wm.putWriter(MapKey(topicPartition, Map.empty), writer)

    val result = wm.recommitPending()

    // The recommit path goes through WriterCommitManager.commitPending →
    // Writer.commit(escalateOnCancel=true) → processPendingOperations(escalateOnCancel=true)
    // → NonExistingFileError on Upload → FatalCloudSinkError.
    result.isLeft shouldBe true
    val batch = result.left.value.asInstanceOf[BatchCloudSinkError]
    batch.fatal should not be empty
    batch.rollBack() shouldBe true
    batch.topicPartitions() should contain(topicPartition)

    val fatalErr = batch.fatal.head
    fatalErr shouldBe a[FatalCloudSinkError]
    fatalErr.exception.exists(_.isInstanceOf[IllegalStateException]) shouldBe true

    // Writer remains in Uploading — escalateOnCancel=true short-circuits before toNoWriter.
    writer.currentWriteState shouldBe a[Uploading]
    writer.hasPendingUpload shouldBe true
  }

  test(
    "recommitPending Fatal on pk1 preserves successfully-committing sibling pk2 on the same TP",
  ) {
    // pk1: AlwaysMissingFileStorage → FatalCloudSinkError
    val storagePk1     = new AlwaysMissingFileStorage()
    val popPk1         = new PendingOperationsProcessors(storagePk1)
    val missingFilePk1 = new File("/nonexistent/sibling-test/pk1-staging.tmp")

    // pk2: real in-memory storage → upload succeeds
    val storagePk2  = new InMemoryStorageInterface()
    val popPk2      = new PendingOperationsProcessors(storagePk2)
    val tempFilePk2 = createTempFileWithPayload("sibling-pk2-records")

    val pk1 = "date=2026-05-01"
    val pk2 = "date=2026-05-02"

    val idxMgr = mock[IndexManager]
    when(idxMgr.indexingEnabled).thenReturn(true)
    when(idxMgr.update(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]])).thenReturn(
      Some(uncommittedOffset).asRight[SinkError],
    )
    when(
      idxMgr.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]),
    ).thenReturn(Some(uncommittedOffset).asRight[SinkError])

    val formatWriter = mock[FormatWriter]
    when(formatWriter.complete()).thenReturn(().asRight)

    val finalKeyPk1 = CloudLocation(bucket, path = Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/pk1-final.jsonl"))
    val finalKeyPk2 = CloudLocation(bucket, path = Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/pk2-final.jsonl"))

    val okbPk1 = mock[ObjectKeyBuilder]
    when(okbPk1.build(any[Offset], any[Long], any[Long])).thenReturn(finalKeyPk1.asRight)
    val okbPk2 = mock[ObjectKeyBuilder]
    when(okbPk2.build(any[Offset], any[Long], any[Long])).thenReturn(finalKeyPk2.asRight)

    val writerPk1 = new Writer[FakeFileMetadata](
      topicPartition,
      mock[CommitPolicy],
      idxMgr,
      stagingFilenameFn = () => missingFilePk1.asRight,
      okbPk1,
      formatWriterFn    = _ => formatWriter.asRight,
      mock[SchemaChangeDetector],
      popPk1,
      partitionKey     = Some(pk1),
      lastSeekedOffset = Some(Offset(99)),
    )
    writerPk1.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(Offset(99))),
        missingFilePk1,
        firstBufferedOffset     = firstBufferedOff,
        uncommittedOffset       = uncommittedOffset,
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 100L,
      ),
    )

    val writerPk2 = new Writer[FakeFileMetadata](
      topicPartition,
      mock[CommitPolicy],
      idxMgr,
      stagingFilenameFn = () => tempFilePk2.asRight,
      okbPk2,
      formatWriterFn    = _ => formatWriter.asRight,
      mock[SchemaChangeDetector],
      popPk2,
      partitionKey     = Some(pk2),
      lastSeekedOffset = Some(Offset(99)),
    )
    // CRITICAL: writerPk2 must be in Writing state, NOT Uploading.
    // The point of this test is to pin the commitWritersWithFilter step-2 expand-to-all-writers
    // semantic: step 1 selects only writerPk1 (hasPendingUpload=true); step 2 expands to ALL
    // writers on that TP, which brings in writerPk2 (Writing, hasPendingUpload=false).
    // If writerPk2 were pre-set to Uploading, it would match the step-1 filter directly and
    // the test would pass even under a regression that narrows the filter to direct-match-only.
    writerPk2.forceWriteState(
      Writing(
        CommitState(topicPartition, Some(Offset(99))),
        formatWriter,
        tempFilePk2,
        firstBufferedOffset     = firstBufferedOff,
        uncommittedOffset       = uncommittedOffset,
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 100L,
      ),
    )

    // Share one WM-level index manager for both writers so cleanUp resolves correctly.
    val wmIdxMgr = mock[IndexManager]
    when(wmIdxMgr.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(wmIdxMgr.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(wmIdxMgr.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))
    when(wmIdxMgr.evictAllGranularLocks(any[TopicPartition])).thenAnswer((_: org.mockito.invocation.InvocationOnMock) => ())

    // Use a shared InMemoryStorageInterface for the WriterManager-level pop (new writer creation
    // only; existing writers carry their own pop). Both are in-memory.
    val sharedStorage = new InMemoryStorageInterface()
    val wm            = buildMinimalWriterManager(wmIdxMgr, new PendingOperationsProcessors(sharedStorage))

    // Use TopicPartitionField as a map key discriminator (it's a case object so equality is by identity).
    // Different string values make the two MapKeys distinct while sharing the same topicPartition.
    wm.putWriter(MapKey(topicPartition, Map(TopicPartitionField -> pk1)), writerPk1)
    wm.putWriter(MapKey(topicPartition, Map(TopicPartitionField -> pk2)), writerPk2)
    wm.writerCount shouldBe 2

    val result = wm.recommitPending()

    // pk1 Fatal is surfaced; pk2 committed successfully so result is Left (BatchCloudSinkError)
    result.isLeft shouldBe true
    val batch = result.left.value.asInstanceOf[BatchCloudSinkError]
    batch.fatal should not be empty
    batch.fatal.head shouldBe a[FatalCloudSinkError]

    // pk2's payload landed at its final path in cloud storage.
    storagePk2.snapshot(bucket).keys should contain(finalKeyPk2.path.get)

    // After simulating CloudSinkTask.rollback: cleanUp is TP-scoped — evicts BOTH writers.
    batch.topicPartitions().foreach(wm.cleanUp)
    wm.writerCount shouldBe 0

    // pk2's final-path object persists after cleanUp (no data loss on restart).
    storagePk2.snapshot(bucket).keys should contain(finalKeyPk2.path.get)
  }

  test(
    "recommitPending Fatal on pk1: pk2's success persists durably; restart-side seek + shouldSkip honour the granular lock",
  ) {
    // Restart-contract counterpart of the test above.  Drives the SAME multi-pk recommit
    // shape (writer-A in Uploading with missing staging file, writer-B in Writing with a
    // real file), but now end-to-end against a SINGLE shared `InMemoryStorageInterface`
    // and a REAL `IndexManagerV2` so writer-B's `updateForPartitionKey` writes the
    // granular lock to durable cloud storage.  After the Fatal + cleanUp cycle, a fresh
    // `IndexManagerV2` is constructed against the same storage and asked for pk2's seek
    // floor; the surviving granular lock must point a fresh writer at the right offset
    // for `Writer.shouldSkip` to dedup re-delivered records.
    //
    // What this test pins beyond the in-batch-isolation test above:
    //   1. `updateForPartitionKey` for pk-writing actually round-trips through cloud storage
    //      (the existing test mocked the IndexManager so this was never exercised).
    //   2. `cleanUp(tp)` does NOT touch the durable cloud-side granular lock — only the
    //      in-memory cache + writer map.
    //   3. A fresh `IndexManagerV2` on a "restart" reads back the pre-Fatal `committedOffset`
    //      for pk-writing via `getSeekedOffsetForPartitionKey`.
    //   4. A fresh `Writer` for pk-writing built with that lastSeekedOffset correctly
    //      dedups offsets <= committed via `shouldSkip` — closing the no-loss /
    //      no-duplication contract end-to-end.
    val sharedStorage  = new SelectivelyMissingFileStorage(missingFile =
      new File("/nonexistent/restart-test/pk1-staging.tmp"),
    )
    val missingFilePk1 = sharedStorage.missingFile
    val tempFilePk2    = createTempFileWithPayload("restart-pk2-records")
    val pop            = new PendingOperationsProcessors(sharedStorage)

    val pk1 = "date=2026-05-01"
    val pk2 = "date=2026-05-02"

    val directoryFileName = ".indexes2"
    val bucketAndPrefix: TopicPartition => Either[SinkError, CloudLocation] =
      _ => CloudLocation(bucket, Some("data/")).asRight

    val v1Filenames = new IndexFilenames(directoryFileName + "-v1")
    // Both index managers need the shared storage as an implicit -- declare it locally so
    // the IndexManagerV1 + IndexManagerV2 constructors below pick it up the same way the
    // production WriterManagerCreator wiring does.
    implicit val sharedStorageImplicit: StorageInterface[FakeFileMetadata] = sharedStorage
    val realIm = new IndexManagerV2(
      bucketAndPrefix,
      new IndexManagerV1(v1Filenames, bucketAndPrefix),
      pop,
      directoryFileName,
      gcIntervalSeconds      = Int.MaxValue,
      gcSweepIntervalSeconds = Int.MaxValue,
      gcSweepEnabled         = false,
    )

    try {
      // Initialise master lock + seed granular eTags in the cache so updateForPartitionKey
      // can pass the eTag-CAS preconditions on first write.
      realIm.open(Set(topicPartition)).isRight shouldBe true
      realIm.ensureGranularLock(topicPartition, pk1).isRight shouldBe true
      realIm.ensureGranularLock(topicPartition, pk2).isRight shouldBe true

      val formatWriter = mock[FormatWriter]
      when(formatWriter.complete()).thenReturn(().asRight)

      val finalKeyPk1 = CloudLocation(
        bucket,
        path = Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/pk1-final.jsonl"),
      )
      val finalKeyPk2 = CloudLocation(
        bucket,
        path = Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/pk2-final.jsonl"),
      )
      val okbPk1 = mock[ObjectKeyBuilder]
      when(okbPk1.build(any[Offset], any[Long], any[Long])).thenReturn(finalKeyPk1.asRight)
      val okbPk2 = mock[ObjectKeyBuilder]
      when(okbPk2.build(any[Offset], any[Long], any[Long])).thenReturn(finalKeyPk2.asRight)

      val writerPk1 = new Writer[FakeFileMetadata](
        topicPartition,
        mock[CommitPolicy],
        realIm,
        stagingFilenameFn = () => missingFilePk1.asRight,
        okbPk1,
        formatWriterFn    = _ => formatWriter.asRight,
        mock[SchemaChangeDetector],
        pop,
        partitionKey     = Some(pk1),
        lastSeekedOffset = Some(Offset(99)),
      )
      writerPk1.forceWriteState(
        Uploading(
          CommitState(topicPartition, Some(Offset(99))),
          missingFilePk1,
          firstBufferedOffset     = firstBufferedOff,
          uncommittedOffset       = uncommittedOffset,
          earliestRecordTimestamp = 1L,
          latestRecordTimestamp   = 100L,
        ),
      )

      val writerPk2 = new Writer[FakeFileMetadata](
        topicPartition,
        mock[CommitPolicy],
        realIm,
        stagingFilenameFn = () => tempFilePk2.asRight,
        okbPk2,
        formatWriterFn    = _ => formatWriter.asRight,
        mock[SchemaChangeDetector],
        pop,
        partitionKey     = Some(pk2),
        lastSeekedOffset = Some(Offset(99)),
      )
      // CRITICAL: writerPk2 starts in `Writing`, NOT `Uploading` -- mirrors the
      // in-batch-isolation test for the same `commitWritersWithFilter` step-2
      // expand-to-all-writers reason documented there.
      writerPk2.forceWriteState(
        Writing(
          CommitState(topicPartition, Some(Offset(99))),
          formatWriter,
          tempFilePk2,
          firstBufferedOffset     = firstBufferedOff,
          uncommittedOffset       = uncommittedOffset,
          earliestRecordTimestamp = 1L,
          latestRecordTimestamp   = 100L,
        ),
      )

      val wm = buildMinimalWriterManager(realIm, pop)
      wm.putWriter(MapKey(topicPartition, Map(TopicPartitionField -> pk1)), writerPk1)
      wm.putWriter(MapKey(topicPartition, Map(TopicPartitionField -> pk2)), writerPk2)
      wm.writerCount shouldBe 2

      val result = wm.recommitPending()

      result.isLeft shouldBe true
      val batch = result.left.value.asInstanceOf[BatchCloudSinkError]
      batch.fatal should not be empty
      batch.fatal.head shouldBe a[FatalCloudSinkError]
      batch.topicPartitions() should contain(topicPartition)

      // pk2's final-path object exists in the shared storage.
      sharedStorage.snapshot(bucket).keys should contain(finalKeyPk2.path.get)

      batch.topicPartitions().foreach(wm.cleanUp)
      wm.writerCount shouldBe 0

      // The pre-restart IndexManagerV2 still has pk2's eTag/offset cached, but the test
      // verifies durability through cloud-side state, not through the in-memory cache.
      // Drop the pre-restart IM so its caches cannot influence the restart-side reads.
      realIm.close()

      val restartIm = new IndexManagerV2(
        bucketAndPrefix,
        new IndexManagerV1(v1Filenames, bucketAndPrefix),
        new PendingOperationsProcessors(sharedStorage),
        directoryFileName,
        gcIntervalSeconds      = Int.MaxValue,
        gcSweepIntervalSeconds = Int.MaxValue,
        gcSweepEnabled         = false,
      )

      try {
        // Master lock present, no PendingState (master-lock advance is driven by preCommit
        // which was not invoked in this test); on restart we just read it back.
        restartIm.open(Set(topicPartition)).isRight shouldBe true

        // Restart-side seek for pk2 reads the granular lock from cloud and surfaces
        // writer-B's pre-Fatal uncommittedOffset.  This is the no-data-loss anchor for
        // the surviving pk: a fresh Writer built with this lastSeekedOffset will dedup
        // re-delivered records exactly at the floor that pk2's commit advanced to.
        val seekedPk2 = restartIm.getSeekedOffsetForPartitionKey(topicPartition, pk2)
        seekedPk2 shouldBe Right(Some(uncommittedOffset))

        // pk1's granular lock was advanced to `lastSeekedOffset` by the best-effort
        // `fnIndexUpdate(tp, lastSeekedOffset, None)` inside `escalateLiveCancel` -- the
        // PendingState is CLEARED so a fresh `IndexManagerV2.open` does NOT have to drive
        // dead-worker recovery for pk1.  No data was uploaded to cloud for pk1 (the Upload
        // step itself failed), so on restart Kafka will re-deliver pk1's records starting
        // from whatever offset Connect had committed at preCommit time.  `shouldSkip`
        // floors them at `Some(Offset(99))` (the lastSeekedOffset) -- offsets <= 99 are
        // dedup-skipped, offsets > 99 are reprocessed.  This is the fail-fast contract:
        // forward progress on the next batch is guaranteed and re-delivered records that
        // are already durable elsewhere are not double-written.
        val seekedPk1 = restartIm.getSeekedOffsetForPartitionKey(topicPartition, pk1)
        seekedPk1 shouldBe Right(Some(Offset(99)))

        // Build a fresh Writer for pk2 with `lastSeekedOffset = Some(uncommittedOffset)`.
        // Its only role is to exercise `Writer.shouldSkip`, which uses the lastSeekedOffset
        // (when in NoWriter state) as the dedup floor. No upload is performed.
        val shimIdxMgr = mock[IndexManager]
        when(shimIdxMgr.indexingEnabled).thenReturn(true)
        val freshWriterPk2 = new Writer[FakeFileMetadata](
          topicPartition,
          mock[CommitPolicy],
          shimIdxMgr,
          stagingFilenameFn = () => createTempFileWithPayload("shim").asRight,
          mock[ObjectKeyBuilder],
          formatWriterFn    = _ => mock[FormatWriter].asRight,
          mock[SchemaChangeDetector],
          new PendingOperationsProcessors(new InMemoryStorageInterface()),
          partitionKey     = Some(pk2),
          lastSeekedOffset = seekedPk2.value,
        )

        // Re-delivered records up to and including the persisted committed offset are
        // skipped (no duplication), and the next offset is processed normally
        // (no data loss for forward progress).
        freshWriterPk2.shouldSkip(uncommittedOffset) shouldBe true
        freshWriterPk2.shouldSkip(Offset(uncommittedOffset.value + 1)) shouldBe false
      } finally restartIm.close()
    } finally
      try realIm.close()
      catch { case _: Throwable => () }
  }

  private def runScenario(partitionKey: Option[String]): Unit = {
    val storage = new OneShotUploadFailingStorage(new RuntimeException("Unexpected end of file from server"))
    val pop     = new PendingOperationsProcessors(storage)

    val tempFile = createTempFileWithPayload("buffered records contents")
    tempFile.exists() shouldBe true

    val indexManager = mock[IndexManager]
    when(indexManager.indexingEnabled).thenReturn(true)
    // fnIndexUpdate is called only AFTER Upload succeeds. We allow any update from
    // the recommit path and capture nothing -- the post-commit storage state is what
    // we assert on.
    when(indexManager.update(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]])).thenReturn(
      Some(uncommittedOffset).asRight[SinkError],
    )
    when(
      indexManager.updateForPartitionKey(
        any[TopicPartition],
        any[String],
        any[Option[Offset]],
        any[Option[PendingState]],
      ),
    ).thenReturn(
      Some(uncommittedOffset).asRight[SinkError],
    )

    val formatWriter         = mock[FormatWriter]
    when(formatWriter.complete()).thenReturn(().asRight)
    val schemaChangeDetector = mock[SchemaChangeDetector]
    val commitPolicy         = mock[CommitPolicy]

    val objectKeyBuilder = mock[ObjectKeyBuilder]
    when(objectKeyBuilder.build(any[Offset], any[Long], any[Long])).thenReturn(finalKeyLocation.asRight)

    val writer = new Writer[FakeFileMetadata](
      topicPartition,
      commitPolicy,
      indexManager,
      stagingFilenameFn = () => tempFile.asRight,
      objectKeyBuilder,
      formatWriterFn    = _ => formatWriter.asRight,
      schemaChangeDetector,
      pop,
      partitionKey     = partitionKey,
      lastSeekedOffset = Some(Offset(99)),
    )
    // Drop the writer straight into Writing state -- avoids depending on the FormatWriter
    // mock for the write path (which is orthogonal to the bug under test).
    writer.forceWriteState(
      Writing(
        CommitState(topicPartition, Some(Offset(99))),
        formatWriter,
        tempFile,
        firstBufferedOffset     = firstBufferedOff,
        uncommittedOffset       = uncommittedOffset,
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 100L,
      ),
    )

    storage.armOneShot()

    val firstCommit = writer.commit

    firstCommit.left.value shouldBe a[NonFatalCloudSinkError]
    // rollBack() must be false -- this is what gates CloudSinkTask.handleErrors against
    // calling WriterManager.cleanUp, which would have deleted the temp file.
    firstCommit.left.value.rollBack() shouldBe false

    writer.currentWriteState shouldBe a[Uploading]
    writer.hasPendingUpload shouldBe true
    tempFile.exists() shouldBe true

    // No bytes landed in cloud storage on the failed attempt: neither at any
    // .temp-upload/ path nor at the final path. (The Upload was the FIRST step, so
    // nothing else in the chain ran.)
    storage.keysUnder(bucket, "data/").toList shouldBe Nil
    storage.keysUnder(bucket, ".temp-upload/").toList shouldBe Nil

    val secondCommit = writer.commit

    secondCommit shouldBe ().asRight
    writer.currentWriteState shouldBe a[NoWriter]
    writer.hasPendingUpload shouldBe false
    // Local temp file is deleted on the successful commit's hygiene step.
    tempFile.exists() shouldBe false

    // The buffered payload now lives at the final path -- and the temp uuid leftover
    // from the second commit was cleaned up by the chain's Delete step.
    val persisted = new String(storage.snapshot(bucket)(finalPath).bytes, StandardCharsets.UTF_8)
    persisted shouldBe "buffered records contents"
    storage.keysUnder(bucket, ".temp-upload/").toList shouldBe Nil
    ()
  }

  /**
   * Verifies the live commit cancellation path (missing local staging file): Writer.commit
   * must return Left(FatalCloudSinkError). The writer stays in `Uploading` (the
   * for-comprehension short-circuits before `toNoWriter`); the production disposal path is
   * `BatchCloudSinkError -> handleErrors(rollBack=true) -> WriterManager.cleanUp(tp) ->
   * Writer.close()` (which deletes the already-missing staging file harmlessly), followed
   * by task restart and `IndexManagerV2.open` seeking back from the master lock.
   */
  private def runCancelledUploadScenario(partitionKey: Option[String]): Unit = {
    val oldCommittedOffset = Offset(99)
    val uncommittedOff     = Offset(150)

    // Storage whose uploadFile immediately returns NonExistingFileError for any path,
    // simulating the tmpDir having been deleted.
    val storage = new AlwaysMissingFileStorage()
    val pop     = new PendingOperationsProcessors(storage)

    // A non-existent file -- the tmpDir was deleted, so no local file exists.
    val missingFile = new File("/nonexistent/path/staging.tmp")
    missingFile.exists() shouldBe false

    val indexManager = mock[IndexManager]
    when(indexManager.indexingEnabled).thenReturn(true)
    // Best-effort fnIndexUpdate is invoked to clear the stale PendingState pre-Fatal.
    // Return Right so the best-effort succeeds; even on Left, the Fatal would still fire.
    when(indexManager.update(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]])).thenReturn(
      Some(oldCommittedOffset).asRight[SinkError],
    )
    when(
      indexManager.updateForPartitionKey(
        any[TopicPartition],
        any[String],
        any[Option[Offset]],
        any[Option[PendingState]],
      ),
    ).thenReturn(
      Some(oldCommittedOffset).asRight[SinkError],
    )

    val formatWriter         = mock[FormatWriter]
    when(formatWriter.complete()).thenReturn(().asRight)
    val schemaChangeDetector = mock[SchemaChangeDetector]
    val commitPolicy         = mock[CommitPolicy]

    val objectKeyBuilder = mock[ObjectKeyBuilder]
    when(objectKeyBuilder.build(any[Offset], any[Long], any[Long])).thenReturn(finalKeyLocation.asRight)

    val writer = new Writer[FakeFileMetadata](
      topicPartition,
      commitPolicy,
      indexManager,
      stagingFilenameFn = () => missingFile.asRight,
      objectKeyBuilder,
      formatWriterFn    = _ => formatWriter.asRight,
      schemaChangeDetector,
      pop,
      partitionKey     = partitionKey,
      lastSeekedOffset = Some(oldCommittedOffset),
    )

    // Place writer in Uploading state with the old committedOffset and uncommittedOffset=150.
    writer.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(oldCommittedOffset)),
        missingFile,
        firstBufferedOffset     = firstBufferedOff,
        uncommittedOffset       = uncommittedOff,
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 100L,
      ),
    )

    val result = writer.commit

    // Live commit cancellation: NonExistingFileError on Upload escalates to Fatal.
    result.left.value shouldBe a[FatalCloudSinkError]
    val fatal = result.left.value.asInstanceOf[FatalCloudSinkError]
    fatal.message should include("topic-x-0")
    fatal.message should include("/nonexistent/path/staging.tmp")
    fatal.exception.exists(_.isInstanceOf[IllegalStateException]) shouldBe true

    // Writer remains in Uploading -- the for-comprehension short-circuited before toNoWriter.
    // BatchCloudSinkError.rollBack() == true triggers WriterManager.cleanUp(tp) -> Writer.close()
    // which deletes the (already-missing) staging file harmlessly. On restart, IndexManagerV2.open
    // seeks the consumer back to committedOffset_old from the master lock.
    writer.currentWriteState shouldBe a[Uploading]
    writer.hasPendingUpload shouldBe true

    // Nothing landed in cloud storage -- the upload was cancelled before any bytes were written.
    storage.keysUnder(bucket, "data/").toList shouldBe Nil
    storage.keysUnder(bucket, ".temp-upload/").toList shouldBe Nil
    ()
  }

  /**
   * Structural test: pins that Writer.commit always passes escalateOnCancel=true to
   * processPendingOperations. This guards against a future refactor that accidentally
   * inverts the flag default and re-activates the dead-code graceful-clear branch on
   * a single-op [Upload] chain (which would silently drop records).
   */
  private def structuralEscalateOnCancelAssertion(partitionKey: Option[String]): Unit = {
    val storage = new InMemoryStorageInterface()
    val pop     = mock[PendingOperationsProcessors]
    val tempFile = createTempFileWithPayload("payload")
    val indexManager = mock[IndexManager]
    when(indexManager.indexingEnabled).thenReturn(true)

    val formatWriter = mock[FormatWriter]
    when(formatWriter.complete()).thenReturn(().asRight)
    val schemaChangeDetector = mock[SchemaChangeDetector]
    val commitPolicy         = mock[CommitPolicy]
    val objectKeyBuilder = mock[ObjectKeyBuilder]
    when(objectKeyBuilder.build(any[Offset], any[Long], any[Long])).thenReturn(finalKeyLocation.asRight)

    when(
      pop.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
        any[Boolean],
        any[Option[String]],
        any[Option[File]],
      ),
    ).thenReturn(Some(uncommittedOffset).asRight[SinkError])

    val writer = new Writer[FakeFileMetadata](
      topicPartition,
      commitPolicy,
      indexManager,
      stagingFilenameFn = () => tempFile.asRight,
      objectKeyBuilder,
      formatWriterFn    = _ => formatWriter.asRight,
      schemaChangeDetector,
      pop,
      partitionKey     = partitionKey,
      lastSeekedOffset = Some(Offset(99)),
    )
    writer.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(Offset(99))),
        tempFile,
        firstBufferedOffset     = firstBufferedOff,
        uncommittedOffset       = uncommittedOffset,
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 100L,
      ),
    )

    val _ = writer.commit

    val escalateCaptor = ArgumentCaptor.forClass(classOf[Boolean])
    val pkCaptor       = ArgumentCaptor.forClass(classOf[Option[String]])
    val fileCaptor     = ArgumentCaptor.forClass(classOf[Option[File]])
    verify(pop).processPendingOperations(
      any[TopicPartition],
      any[Option[Offset]],
      any[PendingState],
      any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
      escalateCaptor.capture(),
      pkCaptor.capture(),
      fileCaptor.capture(),
    )
    escalateCaptor.getValue shouldBe true
    pkCaptor.getValue shouldBe partitionKey
    fileCaptor.getValue shouldBe Some(tempFile)
    val _ = storage // silence unused-warning
    ()
  }

  private def createTempFileWithPayload(payload: String): File = {
    val f = Files.createTempFile("writer-upload-retry-", ".tmp").toFile
    Files.write(f.toPath, payload.getBytes(StandardCharsets.UTF_8))
    f.deleteOnExit()
    f
  }

  /**
   * Build a minimal `WriterManager` for use in recommitPending tests.
   * Real writers are always injected via `putWriter`; the lambda functions are never called.
   */
  private def buildMinimalWriterManager(
    wmIdxMgr: IndexManager,
    pop:      PendingOperationsProcessors,
  ): WriterManager[FakeFileMetadata] =
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
      pendingOperationsProcessors = pop,
    )

  /**
   * One-shot upload-failure injection that does NOT depend on a specific cloud path
   * (the production [[io.lenses.streamreactor.connect.cloud.common.sink.writer.Writer]]
   * generates a fresh `tempFileUuid` on every commit, so the destination path varies).
   */
  private final class OneShotUploadFailingStorage(cause: Throwable) extends InMemoryStorageInterface {
    private val failNext = new AtomicBoolean(false)

    def armOneShot(): Unit = failNext.set(true)

    override def uploadFile(
      source: UploadableFile,
      bucket: String,
      path:   String,
    ): Either[UploadError, String] =
      if (failNext.compareAndSet(true, false)) {
        val sourceFile = source.validate.toEither.fold(_ => new File(path), identity)
        UploadFailedError(cause, sourceFile).asLeft
      } else {
        super.uploadFile(source, bucket, path)
      }
  }

  /**
   * Storage that always returns [[io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError]]
   * on uploadFile, simulating the case where the local staging directory has been deleted.
   */
  private final class AlwaysMissingFileStorage extends InMemoryStorageInterface {
    override def uploadFile(
      source: UploadableFile,
      bucket: String,
      path:   String,
    ): Either[UploadError, String] =
      NonExistingFileError(new File(path)).asLeft
  }

  /**
   * Storage that selectively fails [[uploadFile]] only when the source matches a specific
   * "missing" file path, and delegates to the in-memory [[InMemoryStorageInterface]]
   * implementation for every other operation (including all other uploads, eTag-CAS
   * granular-lock writes, copies, deletes, ...).
   *
   * Used by the restart-contract test to drive a single shared storage instance that
   * (a) fails the writer-A `Upload` step deterministically and (b) lets the writer-B
   * `Upload`, `Copy`, `Delete`, and `IndexManagerV2.updateForPartitionKey` operations
   * succeed end-to-end so the resulting cloud-side state can be re-read by a fresh
   * [[IndexManagerV2]] on the simulated restart.
   */
  private final class SelectivelyMissingFileStorage(val missingFile: File) extends InMemoryStorageInterface {
    override def uploadFile(
      source: UploadableFile,
      bucket: String,
      path:   String,
    ): Either[UploadError, String] =
      if (source.file.getAbsolutePath == missingFile.getAbsolutePath)
        NonExistingFileError(missingFile).asLeft
      else
        super.uploadFile(source, bucket, path)
  }
}
