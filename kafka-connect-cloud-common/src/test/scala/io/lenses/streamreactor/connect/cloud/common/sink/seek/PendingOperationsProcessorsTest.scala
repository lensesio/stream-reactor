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

import cats.data.NonEmptyList
import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
class PendingOperationsProcessorsTest
    extends AnyFunSuiteLike
    with MockitoSugar
    with ArgumentMatchersSugar
    with Matchers
    with EitherValues
    with BeforeAndAfter {
  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("pop-test", 1, 0)

  private val storageInterface: StorageInterface[TestFileMetadata] = mock[StorageInterface[TestFileMetadata]]
  private val tempLocalFile  = mock[File]
  private val tempRemoteFile = "my-temp-remote-file"

  private val topicPartition = Topic("topic1").withPartition(0)

  private val fnIndexUpdate: (
    TopicPartition,
    Option[Offset],
    Option[PendingState],
  ) => Either[SinkError, Option[Offset]] =
    mock[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]]

  private val pendingOperationsProcessors = new PendingOperationsProcessors(storageInterface)

  before {
    reset(storageInterface, fnIndexUpdate, tempLocalFile)

    when(fnIndexUpdate.apply(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, lastCommittedOffset: Option[Offset], _: Option[PendingState]) =>
        lastCommittedOffset.asRight[SinkError],
      )
  }

  test("processPendingOperations should process all operations and update the index successfully") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("source1", tempLocalFile, "dest1"),
        DeleteOperation("source2", tempRemoteFile, "etag2"),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], any[String], any[String]))
      .thenReturn(Right("etag1"))
    when(storageInterface.deleteFile(any[String], any[String], any[String]))
      .thenReturn(Right(()))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result shouldBe Right(Some(Offset(100)))

    val inOrderVerifier = Mockito.inOrder(storageInterface, fnIndexUpdate)
    inOrderVerifier.verify(storageInterface).uploadFile(any[UploadableFile], anyString(), anyString())
    inOrderVerifier.verify(storageInterface).deleteFile(anyString(), anyString(), any[String])
    inOrderVerifier.verify(fnIndexUpdate).apply(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]])
  }

  test(
    "processPendingOperations should clear pending state via fnIndexUpdate when last op upload fails due to missing files",
  ) {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("source1", tempLocalFile, "dest1"),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result shouldBe Right(Some(Offset(50)))

    val inOrderVerifier = Mockito.inOrder(storageInterface, fnIndexUpdate)
    inOrderVerifier.verify(storageInterface).uploadFile(any[UploadableFile], anyString(), anyString())
    // last op succeeded → pending state cleared in storage via fnIndexUpdate(tp, newOffset, None)
    inOrderVerifier.verify(fnIndexUpdate).apply(topicPartition, Some(Offset(50)), None)
  }

  test("processPendingOperations should NOT clear pending state when CopyOperation fails with both paths missing") {
    // S1 contract: mvFile now returns Left(FileMoveError) when both source and
    // destination are missing (instead of silently succeeding). The CopyOperation
    // processor must propagate that Left and must NOT call fnIndexUpdate to clear
    // the pending state -- otherwise the index would advance with no destination
    // object having been written (silent data loss).
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        CopyOperation("bucket1", "missing-temp", "missing-dest", "etag-placeholder"),
        DeleteOperation("bucket1", "missing-temp", "etag-placeholder"),
      ),
    )

    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(
        Left(FileMoveError(new IllegalStateException("both missing"), "missing-temp", "missing-dest")),
      )

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result.isLeft shouldBe true
    verify(storageInterface).mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]])
    verifyNoInteractions(fnIndexUpdate)
  }

  test("processPendingOperations should skip further operations if delete fails") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        DeleteOperation("source1", "bucket1", "etag1"),
        CopyOperation("source2", "bucket1", "dest2", "etag2"),
      ),
    )

    when(storageInterface.deleteFile(anyString(), anyString(), any[String]))
      .thenReturn(Left(FileDeleteError(new Exception("Delete failed"), "source1")))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    // After centralising classification in processOperations, the Fatal message uses
    // FileDeleteError.message() (rich) rather than the inner exception message (sparse).
    result.left.value.message() shouldBe a[String]
    result.left.value.message() should include("Unable to resume processOperations: error deleting file (")
    result.left.value.message() should include("Delete failed")

    verify(storageInterface).deleteFile(anyString(), anyString(), any[String])
    verifyNoInteractions(fnIndexUpdate)
  }

  test(
    "processPendingOperations should propagate transient Upload NonFatal as-is in multi-step chain (no escalation, no fnIndexUpdate)",
  ) {
    // Pins the fix for the exactly-once upload-retry data-loss bug. When the head op
    // is an Upload in a multi-step chain and the storage layer returns a transient
    // network error (UploadFailedError -> NonFatalCloudSinkError), processOperations
    // must NOT escalate to Fatal (escalateOnCancel only applies to NonExistingFileError).
    // Escalation would trigger CloudSinkTask.rollback -> WriterManager.cleanUp -> Writer.close,
    // which deletes the local temp file before recommitPending can retry the upload.
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
        CopyOperation("bucket1", tempRemoteFile, "dest2", "etag-placeholder"),
        DeleteOperation("bucket1", tempRemoteFile, "etag-placeholder"),
      ),
    )

    val networkError = new RuntimeException("Unexpected end of file from server")
    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(UploadFailedError(networkError, tempLocalFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result.left.value shouldBe a[NonFatalCloudSinkError]
    val nonFatal = result.left.value.asInstanceOf[NonFatalCloudSinkError]
    nonFatal.exception shouldBe Some(networkError)

    verify(storageInterface).uploadFile(any[UploadableFile], anyString(), anyString())
    // Critical: no Copy or Delete are attempted, and the index is NOT updated --
    // the writer must remain in Uploading state for recommitPending to retry.
    verify(storageInterface, Mockito.never()).mvFile(anyString(),
                                                     anyString(),
                                                     anyString(),
                                                     anyString(),
                                                     any[Option[String]],
    )
    verify(storageInterface, Mockito.never()).deleteFile(anyString(), anyString(), any[String])
    verifyNoInteractions(fnIndexUpdate)
  }

  test(
    "processPendingOperations gracefully clears pending state when first Upload fails with NonExistingFileError (dead-worker recovery, default escalateOnCancel=false)",
  ) {
    // The graceful-clear path is taken when the local staging file is missing
    // during a dead-worker recovery (previous task crashed and the file lived on a
    // different host's disk). It MUST still run regardless of position in the chain
    // so the index is rolled back to the last committed offset. Live commits use
    // escalateOnCancel=true and escalate to FatalCloudSinkError instead.
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
        CopyOperation("bucket1", tempRemoteFile, "dest2", "etag-placeholder"),
        DeleteOperation("bucket1", tempRemoteFile, "etag-placeholder"),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result shouldBe Right(Some(Offset(50)))

    val inOrderVerifier = Mockito.inOrder(storageInterface, fnIndexUpdate)
    inOrderVerifier.verify(storageInterface).uploadFile(any[UploadableFile], anyString(), anyString())
    // Graceful clear of the stale PendingState; chain is abandoned with the OLD committedOffset preserved.
    inOrderVerifier.verify(fnIndexUpdate).apply(topicPartition, Some(Offset(50)), None)
    verify(storageInterface, Mockito.never()).mvFile(anyString(),
                                                     anyString(),
                                                     anyString(),
                                                     anyString(),
                                                     any[Option[String]],
    )
    verify(storageInterface, Mockito.never()).deleteFile(anyString(), anyString(), any[String])
  }

  test("processPendingOperations should escalate mid-chain Copy failure to Fatal") {
    // After Upload succeeded, the data is durably in cloud storage at .temp-upload/<uuid>
    // and the granular/master lock has a PendingState referencing that uuid. A Copy
    // failure in the Some(furtherOps) branch must still escalate to Fatal so the
    // existing cleanUp + crash-recovery path (ensureGranularLock -> resolveAndCacheGranularLock
    // / IndexManagerV2.open) can replay the chain on the next put or restart.
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        CopyOperation("bucket1", tempRemoteFile, "dest1", "etag1"),
        DeleteOperation("bucket1", tempRemoteFile, "etag1"),
      ),
    )

    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(Left(FileMoveError(new RuntimeException("transient cloud error"), tempRemoteFile, "dest1")))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result.left.value shouldBe a[FatalCloudSinkError]
    result.left.value.message() should include(
      "Unable to resume processOperations: error moving file from (my-temp-remote-file) to (dest1)",
    )
    result.left.value.message() should include("transient cloud error")
    verify(storageInterface).mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]])
    verifyNoInteractions(fnIndexUpdate)
  }

  // New tests for escalateOnCancel=true (live commit path)

  test("with escalateOnCancel=true, NonExistingFile on Upload escalates to Fatal with rich message") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
        CopyOperation("bucket1", tempRemoteFile, "dest2", "etag-placeholder"),
        DeleteOperation("bucket1", tempRemoteFile, "etag-placeholder"),
      ),
    )

    when(tempLocalFile.getPath).thenReturn("/tmp/staging/missing-staging.tmp")
    when(tempLocalFile.getAbsolutePath).thenReturn("/tmp/staging/missing-staging.tmp")
    when(tempLocalFile.getParentFile).thenReturn(null)

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
      escalateOnCancel = true,
      partitionKey     = Some("date=2026-05-01"),
      stagingFile      = Some(tempLocalFile),
    )

    result.left.value shouldBe a[FatalCloudSinkError]
    val fatal = result.left.value.asInstanceOf[FatalCloudSinkError]
    fatal.message should include("topic1-0")
    fatal.message should include("/tmp/staging/missing-staging.tmp")
    fatal.message should include("pendingOffset=100")
    fatal.message should include("date=2026-05-01")
    fatal.exception.exists(_.isInstanceOf[IllegalStateException]) shouldBe true

    // best-effort PendingState clear
    verify(fnIndexUpdate).apply(topicPartition, Some(Offset(50)), None)
    verify(storageInterface, Mockito.never()).mvFile(anyString(),
                                                     anyString(),
                                                     anyString(),
                                                     anyString(),
                                                     any[Option[String]],
    )
    verify(storageInterface, Mockito.never()).deleteFile(anyString(), anyString(), any[String])
  }

  test("with escalateOnCancel=true, fnIndexUpdate failure does NOT mask the Fatal") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
        CopyOperation("bucket1", tempRemoteFile, "dest2", "etag-placeholder"),
        DeleteOperation("bucket1", tempRemoteFile, "etag-placeholder"),
      ),
    )

    when(tempLocalFile.getPath).thenReturn("/tmp/staging/missing.tmp")
    when(tempLocalFile.getAbsolutePath).thenReturn("/tmp/staging/missing.tmp")
    when(tempLocalFile.getParentFile).thenReturn(null)

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))

    // fnIndexUpdate fails (e.g. stale-eTag CAS failure)
    when(fnIndexUpdate.apply(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]]))
      .thenReturn(Left(FatalCloudSinkError("CAS mismatch", topicPartition)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
      escalateOnCancel = true,
      partitionKey     = None,
      stagingFile      = Some(tempLocalFile),
    )

    result.left.value shouldBe a[FatalCloudSinkError]
    val fatal = result.left.value.asInstanceOf[FatalCloudSinkError]
    fatal.exception.exists(_.isInstanceOf[IllegalStateException]) shouldBe true
    fatal.exception.get.getMessage should include("/tmp/staging/missing.tmp")
    fatal.message should include("/tmp/staging/missing.tmp")

    // Cache-no-refresh-on-failure invariant: after the fnIndexUpdate CAS failure,
    // PendingOperationsProcessors must NOT re-read any blob from storage (no cache re-sync).
    // Only uploadFile was called (once, returning NonExistingFileError).
    verify(storageInterface, Mockito.times(1)).uploadFile(any[UploadableFile], anyString(), anyString())
    verify(storageInterface, Mockito.never()).getBlobAsStringAndEtag(anyString(), anyString())
    verify(storageInterface, Mockito.never()).mvFile(anyString(),
                                                     anyString(),
                                                     anyString(),
                                                     anyString(),
                                                     any[Option[String]],
    )
    verify(storageInterface, Mockito.never()).deleteFile(anyString(), anyString(), any[String])
  }

  test("escalateOnCancel=true does NOT change Copy/Delete error classification (mid-chain Copy still Fatal)") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        CopyOperation("bucket1", tempRemoteFile, "dest1", "etag1"),
        DeleteOperation("bucket1", tempRemoteFile, "etag1"),
      ),
    )

    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(Left(FileMoveError(new RuntimeException("transient cloud error"), tempRemoteFile, "dest1")))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
      escalateOnCancel = true,
    )

    result.left.value shouldBe a[FatalCloudSinkError]
  }

  test("escalateOnCancel=true does NOT change Copy/Delete error classification (last-op Delete still NonFatal)") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        DeleteOperation("source1", tempRemoteFile, "etag1"),
      ),
    )

    when(storageInterface.deleteFile(anyString(), anyString(), any[String]))
      .thenReturn(Left(FileDeleteError(new Exception("Delete failed"), tempRemoteFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
      escalateOnCancel = true,
    )

    result.left.value shouldBe a[NonFatalCloudSinkError]
  }

  test("with escalateOnCancel=true, single-op [Upload] (NoIndexManager path) escalates Fatal on NonExistingFileError") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
      ),
    )

    when(tempLocalFile.getPath).thenReturn("/tmp/staging/single.tmp")
    when(tempLocalFile.getAbsolutePath).thenReturn("/tmp/staging/single.tmp")
    when(tempLocalFile.getParentFile).thenReturn(null)

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
      escalateOnCancel = true,
      stagingFile      = Some(tempLocalFile),
    )

    result.left.value shouldBe a[FatalCloudSinkError]
    val fatal = result.left.value.asInstanceOf[FatalCloudSinkError]
    fatal.message should include("/tmp/staging/single.tmp")
    fatal.message should include("topic1-0")

    verify(fnIndexUpdate).apply(topicPartition, Some(Offset(50)), None)
  }

  test("escalateOnCancel=false on single-op [Upload] gracefully clears (counterpart of NoIndexManager Fatal)") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
      escalateOnCancel = false,
    )

    result shouldBe Right(Some(Offset(50)))
    verify(fnIndexUpdate).apply(topicPartition, Some(Offset(50)), None)
  }

  test(
    "escalateOnCancel=true on PARTITIONBY first commit for a brand-new partition key (committedOffset=None)",
  ) {
    // When a writer is created for a NEW partition key, the granular lock is initialised
    // with IndexFile(lockOwner, committedOffset=None, pendingState=None). If that writer's
    // first commit() hits NonExistingFileError, the live-cancel best-effort fnIndexUpdate
    // writes (None, None) -- semantically a no-op but eTag-bumping. This must be idempotent
    // and Fatal must still fire.
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
        CopyOperation("bucket1", tempRemoteFile, "dest2", "etag-placeholder"),
        DeleteOperation("bucket1", tempRemoteFile, "etag-placeholder"),
      ),
    )

    when(tempLocalFile.getPath).thenReturn("/tmp/staging/newpk.tmp")
    when(tempLocalFile.getAbsolutePath).thenReturn("/tmp/staging/newpk.tmp")
    when(tempLocalFile.getParentFile).thenReturn(null)

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))

    when(fnIndexUpdate.apply(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]]))
      .thenReturn(Right(None))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      committedOffset  = None, // brand-new pk, never committed
      pendingState     = pendingState,
      fnIndexUpdate    = fnIndexUpdate,
      escalateOnCancel = true,
      partitionKey     = Some("date=2026-05-01"),
      stagingFile      = Some(tempLocalFile),
    )

    result.left.value shouldBe a[FatalCloudSinkError]
    val fatal = result.left.value.asInstanceOf[FatalCloudSinkError]
    fatal.message should include("date=2026-05-01")
    fatal.message should include("/tmp/staging/newpk.tmp")

    verify(fnIndexUpdate).apply(topicPartition, None, None)
  }

  test(
    "escalateOnCancel=true on PARTITIONBY new pk: fnIndexUpdate failure does NOT mask the Fatal",
  ) {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
        CopyOperation("bucket1", tempRemoteFile, "dest2", "etag-placeholder"),
        DeleteOperation("bucket1", tempRemoteFile, "etag-placeholder"),
      ),
    )

    when(tempLocalFile.getPath).thenReturn("/tmp/staging/newpk-cas-fail.tmp")
    when(tempLocalFile.getAbsolutePath).thenReturn("/tmp/staging/newpk-cas-fail.tmp")
    when(tempLocalFile.getParentFile).thenReturn(null)

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))
    when(fnIndexUpdate.apply(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]]))
      .thenReturn(Left(FatalCloudSinkError("granular CAS mismatch", topicPartition)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      committedOffset  = None,
      pendingState     = pendingState,
      fnIndexUpdate    = fnIndexUpdate,
      escalateOnCancel = true,
      partitionKey     = Some("date=2026-05-01"),
      stagingFile      = Some(tempLocalFile),
    )

    result.left.value shouldBe a[FatalCloudSinkError]
    result.left.value.exception().exists(_.isInstanceOf[IllegalStateException]) shouldBe true
  }

  // pending-operations edge cases

  test(
    "single-op [Upload] chain + transient UploadFailedError returns NonFatalCloudSinkError, not Fatal (NoIndexManager path regression guard)",
  ) {
    // In the NoIndexManager shape a Writer's entire chain is just [Upload]. When that
    // single Upload step hits a transient network error (UploadFailedError), the last-op
    // None-branch must classify it as NonFatal so recommitPending can retry from the
    // still-on-disk staging file. Escalating to Fatal here would delete the staging file
    // via WriterManager.cleanUp → Writer.close, causing permanent record loss.
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
      ),
    )

    val networkError = new RuntimeException("connection reset")
    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(UploadFailedError(networkError, tempLocalFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result.left.value shouldBe a[NonFatalCloudSinkError]
    result.left.value.asInstanceOf[NonFatalCloudSinkError].exception shouldBe Some(networkError)
    verifyNoInteractions(fnIndexUpdate)
  }

  test(
    "successful Upload propagates the new eTag to the subsequent CopyOperation and DeleteOperation (not the original placeholder)",
  ) {
    // After Upload succeeds the storage returns a fresh eTag for the temp blob.
    // updateEtag() must thread that eTag into all following CopyOperation / DeleteOperation
    // entries so the cloud provider can use it for optimistic-concurrency checks. If the
    // original placeholder eTag were used instead, Copy/Delete could be rejected.
    val originalEtag = "etag-A-placeholder"
    val newEtag      = "etag-B-from-upload"

    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
        CopyOperation("bucket1", tempRemoteFile, "dest-final", originalEtag),
        DeleteOperation("bucket1", tempRemoteFile, originalEtag),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Right(newEtag))
    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(Right(()))
    when(storageInterface.deleteFile(anyString(), anyString(), any[String]))
      .thenReturn(Right(()))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result shouldBe Right(Some(Offset(100)))

    // CopyOperation passes op.eTag.some as the last arg to mvFile; it must carry the new eTag.
    // DeleteOperation passes op.eTag directly to deleteFile.
    verify(storageInterface).mvFile(anyString(), anyString(), anyString(), anyString(), eqTo(Some(newEtag)))
    verify(storageInterface).deleteFile(anyString(), anyString(), eqTo(newEtag))
  }

  test(
    "escalateOnCancel=true does NOT change last-op Copy error classification (Fatal, asymmetric with last-op Delete)",
  ) {
    // Copy is not a live-commit-cancellation signal, so escalateOnCancel (which only gates on
    // UploadOperation + NonExistingFileError) is irrelevant here either way. But a tail
    // CopyOperation failure (reachable in CommitMode.Batch, whose chain is Copy-only, and on
    // legacy-lock recovery) escalates to Fatal exactly like a mid-chain Copy failure: the
    // data is already durable at .temp-upload/<uuid> and the lock's PendingState still
    // references it, so crash recovery can resume -- unlike a last-op Delete, which is
    // hygiene-only and stays NonFatal.
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        CopyOperation("bucket1", tempRemoteFile, "dest-final", "etag1"),
      ),
    )

    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(Left(FileMoveError(new RuntimeException("mvFile-error"), tempRemoteFile, "dest-final")))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
      escalateOnCancel = true,
    )

    result.left.value shouldBe a[FatalCloudSinkError]
    verifyNoInteractions(fnIndexUpdate)
  }

  test(
    "single-op [Upload] success (NoIndexManager path) calls fnIndexUpdate with pendingOffset and returns Right(pendingOffset)",
  ) {
    // Defensive correctness test: verifies that the None-branch success case for an
    // UploadOperation calls fnIndexUpdate(tp, pendingOffset, None) — advancing the
    // committed offset to the pending value and clearing PendingState from the index.
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Right("etag-upload"))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result shouldBe Right(Some(Offset(100)))
    verify(fnIndexUpdate).apply(topicPartition, Some(Offset(100)), None)
  }

  // ─── OS edge cases — 0-byte staging file + AccessDeniedException simulation ──────────────────

  /**
   * Upload of a 0-byte staging file (empty time-flush with no records).
   *
   * When the commit policy fires on a time threshold before any records have been buffered
   * into the local staging file, the file is 0 bytes. The Upload operation should succeed
   * (the connector does not gate on payload size), and the full [Upload, Copy, Delete] chain
   * completes normally. `processPendingOperations` must not crash or return an error.
   */
  test(
    "Upload of a 0-byte staging file completes the chain; processPendingOperations does not crash",
  ) {
    // Create a real 0-byte temp file (the UploadableFile wrapper reads it from disk).
    val zeroByteFile = java.io.File.createTempFile("pop-d27b-zero-", ".tmp")
    zeroByteFile.deleteOnExit()
    // 0-byte: do NOT write any content

    val pendingState = PendingState(
      pendingOffset = Offset(80),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", zeroByteFile, tempRemoteFile),
      ),
    )

    // storageInterface.uploadFile accepts the 0-byte file and returns a fresh eTag.
    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Right("etag-zero"))
    // fnIndexUpdate is already stubbed in before{} to return the incoming offset.

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(70)),
      pendingState,
      fnIndexUpdate,
    )

    // Chain completes successfully — 0-byte payload does not cause a crash.
    result shouldBe Right(Some(Offset(80)))
    verify(fnIndexUpdate).apply(topicPartition, Some(Offset(80)), None)
  }

  /**
   * UploadFailedError (simulating an OS-locked or access-denied staging file)
   * is classified as NonFatalCloudSinkError (not Fatal), allowing recommitPending to retry.
   *
   * This mirrors what happens when `storageInterface.uploadFile` throws an AccessDeniedException
   * or returns Left(UploadFailedError): PendingOperationsProcessors wraps it in NonFatal so the
   * Kafka Connect framework retries the pending write on the next poll without losing the local file.
   */
  test(
    "UploadFailedError (AccessDeniedException simulation) on Upload is classified as NonFatalCloudSinkError, not Fatal",
  ) {
    val pendingState = PendingState(
      pendingOffset = Offset(90),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
      ),
    )

    // Simulate AccessDeniedException at the storage layer: returns Left(UploadFailedError).
    val accessDenied = UploadFailedError(
      new java.io.IOException("Permission denied — staging file locked by OS"),
      new File(tempRemoteFile),
    )
    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(accessDenied))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(80)),
      pendingState,
      fnIndexUpdate,
    )

    // NonFatal — recommitPending can retry from the still-on-disk local file.
    result.isLeft shouldBe true
    result.left.value shouldBe a[NonFatalCloudSinkError]

    // fnIndexUpdate must NOT have been called (offset was not advanced).
    Mockito.verify(fnIndexUpdate, Mockito.never).apply(
      any[TopicPartition],
      any[Option[Offset]],
      any[Option[PendingState]],
    )
  }

  // ─── Stage E gap closure — final granular IndexUpdate fails after all storage ops succeed ────

  /**
   * Stage E in the pipeline failure mosaic: Upload + Copy + Delete all succeed, but the
   * FINAL `fnIndexUpdate` (clear PendingState, advance committedOffset) returns Left.
   *
   * Pins the success branch in [[PendingOperationsProcessors.processOperations]] last-op
   * `case (_, Right(_))` (line 230). The chain's bytes are durable at the final path; the
   * `.temp-upload/<uuid>` blob has been deleted; only the granular-lock CAS clear failed.
   * The connector must propagate the `Left` as-is (NOT mask it with success), so that
   * `committedOffset` does not advance in memory and the next preCommit will not surface
   * a new offset for this TP. A subsequent attempt or restart will replay safely because
   * `shouldSkip` filters records past the granular lock and the master lock acts as the
   * durable seek floor.
   */
  test(
    "Stage E: final fnIndexUpdate failure after Upload+Copy+Delete success propagates Left; committedOffset does not advance",
  ) {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("bucket1", tempLocalFile, tempRemoteFile),
        CopyOperation("bucket1", tempRemoteFile, "dest-final", "etag-placeholder"),
        DeleteOperation("bucket1", tempRemoteFile, "etag-placeholder"),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Right("etag-from-upload"))
    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(Right(()))
    when(storageInterface.deleteFile(anyString(), anyString(), any[String]))
      .thenReturn(Right(()))

    // fnIndexUpdate: succeed on the two intermediate calls (third arg = Some(pendingState.copy(...))),
    // fail on the FINAL call (third arg = None) which corresponds to the last-op success branch
    // at PendingOperationsProcessors.scala line 230.
    val casFailure = NonFatalCloudSinkError("granular lock CAS failed (eTag mismatch)", None)
    when(fnIndexUpdate.apply(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer { (_: TopicPartition, lastCommitted: Option[Offset], pending: Option[PendingState]) =>
        pending match {
          case Some(_) => lastCommitted.asRight[SinkError]
          case None    => Left(casFailure)
        }
      }

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    // Left propagated as-is — NonFatal, not masked with success.
    result.left.value shouldBe a[NonFatalCloudSinkError]
    result.left.value.message() should include("granular lock CAS failed")

    // All three storage ops ran in order; only the final fnIndexUpdate (third arg = None) failed.
    val inOrderVerifier = Mockito.inOrder(storageInterface, fnIndexUpdate)
    inOrderVerifier.verify(storageInterface).uploadFile(any[UploadableFile], anyString(), anyString())
    // First fnIndexUpdate after Upload: chain still has [Copy, Delete] pending.
    inOrderVerifier.verify(fnIndexUpdate).apply(eqTo(topicPartition), eqTo(Some(Offset(50))), any[Some[PendingState]])
    inOrderVerifier.verify(storageInterface).mvFile(anyString(),
                                                    anyString(),
                                                    anyString(),
                                                    anyString(),
                                                    any[Option[String]],
    )
    // Second fnIndexUpdate after Copy: chain still has [Delete] pending.
    inOrderVerifier.verify(fnIndexUpdate).apply(eqTo(topicPartition), eqTo(Some(Offset(50))), any[Some[PendingState]])
    inOrderVerifier.verify(storageInterface).deleteFile(anyString(), anyString(), any[String])
    // FINAL fnIndexUpdate after Delete: clears PendingState (third arg = None) and advances offset to 100.
    // This is the one we forced to fail.
    inOrderVerifier.verify(fnIndexUpdate).apply(topicPartition, Some(Offset(100)), None)
  }

  // ─── Gap 3 — transient Copy fails fast (no in-process retry, mirror of transient-Upload retry) ──

  /**
   * Pins the deliberate dichotomy between Upload retry and Copy retry behaviour.
   *
   * When a transient cloud error (e.g. throttling, network reset) is returned by `mvFile`
   * mid-chain, `processPendingOperations` must:
   *  1. Return `Left(FatalCloudSinkError)` immediately, NOT a `NonFatal` that recommitPending
   *     would retry in-process.
   *  2. Make a single `mvFile` call (no in-process retry loop).
   *
   * Contrast with Upload: a transient `UploadFailedError` in the same shape returns
   * `NonFatalCloudSinkError`, leaving the writer in `Uploading` so the local staging file
   * survives and `recommitPending` can rebuild a fresh chain on the next put. See
   * `WriterUploadRetryRecoveryTest` "transient upload failure leaves writer in Uploading;
   * recommitPending recovers without data loss" (line 108) and
   * `PendingOperationsProcessorsTest` "should propagate transient Upload NonFatal as-is in
   * multi-step chain" (line 194).
   *
   * Why the dichotomy is intentional: an Upload failure leaves no cloud-side state from the
   * failed attempt, so a fresh in-process retry with a new `tempFileUuid` is safe. A Copy
   * failure mid-chain leaves the durable `.temp-upload/<uuid>` blob plus a granular lock
   * with `PendingState=[Copy, Delete]`. Recovery is the existing crash + reopen replay
   * (`ExactlyOnceScenarioTest` line 149) or LC-2451 in-place rollback (line 398), which
   * reuses the existing temp blob — this avoids the orphaned-temp-blob accumulation an
   * in-process Copy retry loop would produce.
   */
  test(
    "transient Copy failure escalates Fatal immediately (single mvFile call, no in-process retry; mirror dichotomy with transient Upload)",
  ) {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        CopyOperation("bucket1", tempRemoteFile, "dest-final", "etag1"),
        DeleteOperation("bucket1", tempRemoteFile, "etag1"),
      ),
    )

    val transientCloudError = new RuntimeException("connection reset by peer")
    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(Left(FileMoveError(transientCloudError, tempRemoteFile, "dest-final")))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    // Fatal — recovery path is restart+replay, NOT in-process recommitPending.
    result.left.value shouldBe a[FatalCloudSinkError]
    result.left.value.message() should include("Unable to resume processOperations")

    // Single mvFile call: no in-process retry loop. (Contrast: Upload would NOT escalate Fatal
    // and recommitPending would attempt a second uploadFile on the next put with a new
    // tempFileUuid; here, escalating Fatal triggers cleanUp which closes the writer.)
    verify(storageInterface, Mockito.times(1)).mvFile(anyString(),
                                                      anyString(),
                                                      anyString(),
                                                      anyString(),
                                                      any[Option[String]],
    )
    // No follow-on Delete (chain abandoned at Copy failure).
    verify(storageInterface, Mockito.never()).deleteFile(anyString(), anyString(), any[String])
    // No fnIndexUpdate: granular lock retains PendingState=[Copy, Delete] for crash-recovery replay.
    verifyNoInteractions(fnIndexUpdate)
  }

}
