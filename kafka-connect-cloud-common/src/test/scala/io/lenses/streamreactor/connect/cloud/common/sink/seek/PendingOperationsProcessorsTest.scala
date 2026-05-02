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
    verify(storageInterface, Mockito.never()).mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]])
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
    verify(storageInterface, Mockito.never()).mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]])
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

}
