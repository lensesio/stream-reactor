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

import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectProtection
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.storage._
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.InputStream
import java.time.Instant

/**
 * Unit tests for [[StorageInterfaceWithMetrics]].
 *
 * We use a hand-rolled stub rather than Mockito because [[StorageInterface]] has
 * type-parameterised and implicit-parameterised methods that are difficult to mock
 * generically. The stub always returns the configured `successResult` or `failureResult`.
 */
class StorageInterfaceWithMetricsTest
    extends AnyFunSuiteLike
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  private var metrics: CloudSinkMetrics = _

  override def beforeEach(): Unit = {
    metrics = new CloudSinkMetrics()
    super.beforeEach()
  }

  // -------------------------------------------------------------------------
  // Stub StorageInterface that lets each test configure success/failure
  // -------------------------------------------------------------------------

  private class StubFileMetadata extends FileMetadata {
    override def file:         String  = "stub"
    override def lastModified: Instant = Instant.EPOCH
  }

  private def makeUploadFile(): UploadableFile = {
    val f = File.createTempFile("metrics-test", ".bin")
    f.deleteOnExit()
    UploadableFile(f)
  }

  private class StubStorageInterface(
    uploadResult:         Either[UploadError, String]                                             = Right("etag-123"),
    mvResult:             Either[FileMoveError, Unit]                                             = Right(()),
    deleteResult:         Either[FileDeleteError, Unit]                                           = Right(()),
    deleteFilesRes:       Either[FileDeleteError, Unit]                                           = Right(()),
    getBlobResult:        Either[FileLoadError, (String, String)]                                 = Right(("content", "etag-abc")),
    listKeysResult:       Either[FileListError, Option[ListOfKeysResponse[StubFileMetadata]]]     = Right(None),
    listMetaResult:       Either[FileListError, Option[ListOfMetadataResponse[StubFileMetadata]]] = Right(None),
    throwOnUpload:        Option[Throwable]                                                       = None,
    throwOnMv:            Option[Throwable]                                                       = None,
    throwOnDelete:        Option[Throwable]                                                       = None,
    throwOnDeleteFiles:   Option[Throwable]                                                       = None,
    deleteIfMatchOutcome: DeleteOutcome                                                           = DeleteOutcome.Deleted,
    throwOnDeleteIfMatch: Option[Throwable]                                                       = None,
    throwOnGetBlob:       Option[Throwable]                                                       = None,
    throwOnListKeys:      Option[Throwable]                                                       = None,
    throwOnListMeta:      Option[Throwable]                                                       = None,
  ) extends StorageInterface[StubFileMetadata] {

    private def orThrow[A](maybeThrow: Option[Throwable], value: => A): A =
      maybeThrow.fold(value)(t => throw t)

    override def system(): String = "stub"
    override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] =
      orThrow(throwOnUpload, uploadResult)
    override def close(): Unit = ()
    override def pathExists(bucket: String, path: String): Either[PathError, Boolean] = Right(false)
    override def list(
      bucket:     String,
      prefix:     Option[String],
      lastFile:   Option[StubFileMetadata],
      numResults: Int,
    ): Either[FileListError, Option[ListOfKeysResponse[StubFileMetadata]]] =
      Right(None)
    override def listFileMetaRecursive(
      bucket: String,
      prefix: Option[String],
    ): Either[FileListError, Option[ListOfMetadataResponse[StubFileMetadata]]] =
      orThrow(throwOnListMeta, listMetaResult)
    override def listKeysRecursive(
      bucket: String,
      prefix: Option[String],
    ): Either[FileListError, Option[ListOfKeysResponse[StubFileMetadata]]] =
      orThrow(throwOnListKeys, listKeysResult)
    override def seekToFile(bucket: String, fileName: String, lastModified: Option[Instant]): Option[StubFileMetadata] =
      None
    override def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream] =
      Right(InputStream.nullInputStream())
    override def getBlobAsString(bucket:        String, path: String): Either[FileLoadError, String] = Right("")
    override def getBlobAsStringAndEtag(bucket: String, path: String): Either[FileLoadError, (String, String)] =
      orThrow(throwOnGetBlob, getBlobResult)
    override def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata] =
      Right(ObjectMetadata(0L, Instant.EPOCH))
    override def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit] =
      Right(())
    override def writeBlobToFile[O](
      bucket:           String,
      path:             String,
      objectProtection: ObjectProtection[O],
    )(
      implicit
      encoder: io.circe.Encoder[O],
    ): Either[UploadError, ObjectWithETag[O]] =
      Left(UploadFailedError(new RuntimeException("not implemented in stub"), new File(".")))
    override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] =
      orThrow(throwOnDeleteFiles, deleteFilesRes)
    override def deleteFile(bucket: String, file: String, eTag: String): Either[FileDeleteError, Unit] =
      orThrow(throwOnDelete, deleteResult)
    override def deleteFilesIfMatch(bucket: String, files: Seq[ConditionalDelete]): Seq[DeleteOutcome] =
      orThrow(throwOnDeleteIfMatch, files.map(_ => deleteIfMatchOutcome))
    override def mvFile(
      oldBucket: String,
      oldPath:   String,
      newBucket: String,
      newPath:   String,
      maybeEtag: Option[String],
    ): Either[FileMoveError, Unit] = orThrow(throwOnMv, mvResult)
    override def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit] = Right(())
    override def touchFile(bucket:                  String, path: String): Either[FileTouchError, Unit]  = Right(())
  }

  // -------------------------------------------------------------------------
  // uploadFile
  // -------------------------------------------------------------------------

  test("uploadFile success: timer count increments, no error counted") {
    val stub      = new StubStorageInterface(uploadResult = Right("etag"))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.uploadFile(makeUploadFile(), "bucket", "key")

    metrics.getStorageUploadTimerCount shouldBe 1L
    metrics.getStorageUploadErrorsTotal shouldBe 0L
  }

  test("uploadFile failure: timer count and error counter both increment") {
    val err       = UploadFailedError(new RuntimeException("boom"), new File("."))
    val stub      = new StubStorageInterface(uploadResult = Left(err))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.uploadFile(makeUploadFile(), "bucket", "key")

    metrics.getStorageUploadTimerCount shouldBe 1L
    metrics.getStorageUploadErrorsTotal shouldBe 1L
  }

  test("multiple uploadFile calls accumulate count and sum") {
    val stub      = new StubStorageInterface()
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    (1 to 5).foreach(_ => decorator.uploadFile(makeUploadFile(), "b", "k"))

    metrics.getStorageUploadTimerCount shouldBe 5L
    metrics.getStorageUploadTimerSumMillis should be >= 0L
  }

  // -------------------------------------------------------------------------
  // mvFile (copy)
  // -------------------------------------------------------------------------

  test("mvFile success: copy timer count increments, no error") {
    val stub      = new StubStorageInterface()
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.mvFile("b", "src", "b", "dst", None)

    metrics.getStorageCopyTimerCount shouldBe 1L
    metrics.getStorageCopyErrorsTotal shouldBe 0L
  }

  test("mvFile failure: copy error counter increments") {
    val stub      = new StubStorageInterface(mvResult = Left(FileMoveError(new RuntimeException("failed"), "src", "dst")))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.mvFile("b", "src", "b", "dst", None)

    metrics.getStorageCopyTimerCount shouldBe 1L
    metrics.getStorageCopyErrorsTotal shouldBe 1L
  }

  // -------------------------------------------------------------------------
  // deleteFile + deleteFiles
  // -------------------------------------------------------------------------

  test("deleteFile success: no error recorded") {
    val stub      = new StubStorageInterface()
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.deleteFile("b", "f", "etag")

    metrics.getStorageDeleteErrorsTotal shouldBe 0L
  }

  test("deleteFile failure: delete error counter increments") {
    val stub      = new StubStorageInterface(deleteResult = Left(FileDeleteError(new RuntimeException("del-fail"), "f")))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.deleteFile("b", "f", "etag")

    metrics.getStorageDeleteErrorsTotal shouldBe 1L
  }

  test("deleteFilesIfMatch passes outcomes through; a refusal on version or a missing object is no storage error") {
    Seq(DeleteOutcome.Deleted, DeleteOutcome.NotFound, DeleteOutcome.PreconditionFailed).foreach { outcome =>
      val decorator = new StorageInterfaceWithMetrics(new StubStorageInterface(deleteIfMatchOutcome = outcome), metrics)
      decorator.deleteFilesIfMatch("b", Seq(ConditionalDelete("f1", "e1"), ConditionalDelete("f2", "e2"))) shouldBe
        Seq(outcome, outcome)
    }
    metrics.getStorageDeleteErrorsTotal shouldBe 0L
  }

  test("deleteFilesIfMatch with failed objects records one delete error per call") {
    val failed    = DeleteOutcome.Failed(FileDeleteError(new RuntimeException("denied"), "f"))
    val decorator = new StorageInterfaceWithMetrics(new StubStorageInterface(deleteIfMatchOutcome = failed), metrics)
    decorator.deleteFilesIfMatch("b", Seq(ConditionalDelete("f1", "e1"), ConditionalDelete("f2", "e2"))) shouldBe
      Seq(failed, failed)
    metrics.getStorageDeleteErrorsTotal shouldBe 1L
  }

  test("deleteFilesIfMatch thrown exception: delete error counter still recorded, exception rethrown") {
    val boom = new RuntimeException("connection refused")
    val decorator =
      new StorageInterfaceWithMetrics(new StubStorageInterface(throwOnDeleteIfMatch = Some(boom)), metrics)
    intercept[RuntimeException](decorator.deleteFilesIfMatch("b", Seq(ConditionalDelete("f", "e")))) shouldBe boom
    metrics.getStorageDeleteErrorsTotal shouldBe 1L
  }

  test("deleteFiles failure: delete error counter increments") {
    val stub =
      new StubStorageInterface(deleteFilesRes = Left(FileDeleteError(new RuntimeException("batch-fail"), "f1,f2")))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.deleteFiles("b", Seq("f1", "f2"))

    metrics.getStorageDeleteErrorsTotal shouldBe 1L
  }

  // -------------------------------------------------------------------------
  // getBlobAsStringAndEtag (get)
  // -------------------------------------------------------------------------

  test("getBlobAsStringAndEtag success: get timer count increments, no error") {
    val stub      = new StubStorageInterface()
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.getBlobAsStringAndEtag("b", "p")

    metrics.getStorageGetTimerCount shouldBe 1L
    metrics.getStorageGetErrorsTotal shouldBe 0L
  }

  test("getBlobAsStringAndEtag failure: get error counter increments") {
    val stub      = new StubStorageInterface(getBlobResult = Left(GeneralFileLoadError(new RuntimeException("r"), "p")))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.getBlobAsStringAndEtag("b", "p")

    metrics.getStorageGetTimerCount shouldBe 1L
    metrics.getStorageGetErrorsTotal shouldBe 1L
  }

  // -------------------------------------------------------------------------
  // listKeysRecursive + listFileMetaRecursive (list)
  // -------------------------------------------------------------------------

  test("listKeysRecursive success: no error recorded") {
    val stub      = new StubStorageInterface()
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.listKeysRecursive("b", None)

    metrics.getStorageListErrorsTotal shouldBe 0L
  }

  test("listFileMetaRecursive failure: list error counter increments") {
    val stub =
      new StubStorageInterface(listMetaResult = Left(FileListError(new RuntimeException("list-fail"), "b", None)))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.listFileMetaRecursive("b", None)

    metrics.getStorageListErrorsTotal shouldBe 1L
  }

  // -------------------------------------------------------------------------
  // Uninstrumented methods delegate transparently
  // -------------------------------------------------------------------------

  test("system() delegates to underlying implementation") {
    val stub      = new StubStorageInterface()
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    decorator.system() shouldBe "stub"
  }

  test("pathExists is not instrumented (no timer change)") {
    val stub      = new StubStorageInterface()
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)
    val _         = decorator.pathExists("b", "p")

    // None of the instrumented counters should move
    metrics.getStorageUploadTimerCount shouldBe 0L
    metrics.getStorageCopyTimerCount shouldBe 0L
    metrics.getStorageDeleteErrorsTotal shouldBe 0L
    metrics.getStorageGetTimerCount shouldBe 0L
    metrics.getStorageListErrorsTotal shouldBe 0L
  }

  // -------------------------------------------------------------------------
  // Unchecked exception paths — timedWithErr / countErr must still record
  // -------------------------------------------------------------------------

  test("uploadFile thrown exception: timer and error counter still recorded, exception rethrown") {
    val boom      = new RuntimeException("transport failure")
    val stub      = new StubStorageInterface(throwOnUpload = Some(boom))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)

    val caught = intercept[RuntimeException](decorator.uploadFile(makeUploadFile(), "b", "k"))
    caught shouldBe boom
    metrics.getStorageUploadTimerCount shouldBe 1L
    metrics.getStorageUploadErrorsTotal shouldBe 1L
  }

  test("mvFile thrown exception: copy timer and error counter still recorded, exception rethrown") {
    val boom      = new RuntimeException("network reset")
    val stub      = new StubStorageInterface(throwOnMv = Some(boom))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)

    val caught = intercept[RuntimeException](decorator.mvFile("b", "src", "b", "dst", None))
    caught shouldBe boom
    metrics.getStorageCopyTimerCount shouldBe 1L
    metrics.getStorageCopyErrorsTotal shouldBe 1L
  }

  test("getBlobAsStringAndEtag thrown exception: get timer and error counter still recorded, exception rethrown") {
    val boom      = new RuntimeException("read timeout")
    val stub      = new StubStorageInterface(throwOnGetBlob = Some(boom))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)

    val caught = intercept[RuntimeException](decorator.getBlobAsStringAndEtag("b", "p"))
    caught shouldBe boom
    metrics.getStorageGetTimerCount shouldBe 1L
    metrics.getStorageGetErrorsTotal shouldBe 1L
  }

  test("deleteFile thrown exception: delete error counter still recorded, exception rethrown") {
    val boom      = new RuntimeException("connection refused")
    val stub      = new StubStorageInterface(throwOnDelete = Some(boom))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)

    val caught = intercept[RuntimeException](decorator.deleteFile("b", "f", "etag"))
    caught shouldBe boom
    metrics.getStorageDeleteErrorsTotal shouldBe 1L
  }

  test("deleteFiles thrown exception: delete error counter still recorded, exception rethrown") {
    val boom      = new RuntimeException("bulk delete failure")
    val stub      = new StubStorageInterface(throwOnDeleteFiles = Some(boom))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)

    val caught = intercept[RuntimeException](decorator.deleteFiles("b", Seq("f1", "f2")))
    caught shouldBe boom
    metrics.getStorageDeleteErrorsTotal shouldBe 1L
  }

  test("listKeysRecursive thrown exception: list error counter still recorded, exception rethrown") {
    val boom      = new RuntimeException("list timeout")
    val stub      = new StubStorageInterface(throwOnListKeys = Some(boom))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)

    val caught = intercept[RuntimeException](decorator.listKeysRecursive("b", None))
    caught shouldBe boom
    metrics.getStorageListErrorsTotal shouldBe 1L
  }

  test("listFileMetaRecursive thrown exception: list error counter still recorded, exception rethrown") {
    val boom      = new RuntimeException("meta list timeout")
    val stub      = new StubStorageInterface(throwOnListMeta = Some(boom))
    val decorator = new StorageInterfaceWithMetrics(stub, metrics)

    val caught = intercept[RuntimeException](decorator.listFileMetaRecursive("b", None))
    caught shouldBe boom
    metrics.getStorageListErrorsTotal shouldBe 1L
  }
}
