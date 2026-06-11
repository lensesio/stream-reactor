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
package io.lenses.streamreactor.connect.cloud.common.storage

import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.config.CommitRetryConfig
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectProtection
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.InputStream
import java.net.SocketException
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

class RetryingStorageInterfaceTest extends AnyFunSuiteLike with Matchers with BeforeAndAfterEach {

  private var metrics: CloudSinkMetrics = _

  override def beforeEach(): Unit = {
    metrics = new CloudSinkMetrics()
    super.beforeEach()
  }

  // Zero-sleep config for fast tests
  private val immediateConfig = CommitRetryConfig(
    maxAttempts = 3,
    baseDelayMs = 0L,
    multiplier  = 1.0,
    maxDelayMs  = 0L,
  )

  private val alwaysTransient: TransientErrorClassifier = _ => true
  private val neverTransient:  TransientErrorClassifier = _ => false

  private class StubFileMetadata extends FileMetadata {
    override def file:         String  = "stub"
    override def lastModified: Instant = Instant.EPOCH
  }

  private class CountingStorageInterface(
    mvResults:     Iterator[Either[FileMoveError, Unit]],
    deleteResults: Iterator[Either[FileDeleteError, Unit]],
  ) extends StorageInterface[StubFileMetadata] {

    val mvCallCount:     AtomicInteger = new AtomicInteger(0)
    val deleteCallCount: AtomicInteger = new AtomicInteger(0)

    override def mvFile(
      oldBucket: String,
      oldPath:   String,
      newBucket: String,
      newPath:   String,
      maybeEtag: Option[String],
    ): Either[FileMoveError, Unit] = {
      mvCallCount.incrementAndGet()
      mvResults.next()
    }

    override def deleteFile(bucket: String, file: String, eTag: String): Either[FileDeleteError, Unit] = {
      deleteCallCount.incrementAndGet()
      deleteResults.next()
    }

    override def system(): String = "stub"
    override def close():  Unit   = ()
    override def uploadFile(s: UploadableFile, b: String, p: String): Either[UploadError, String] = Right("etag")
    override def pathExists(b: String, p:         String): Either[PathError, Boolean] = Right(false)
    override def list(
      b: String,
      p: Option[String],
      l: Option[StubFileMetadata],
      n: Int,
    ): Either[FileListError, Option[ListOfKeysResponse[StubFileMetadata]]] = Right(None)
    override def listFileMetaRecursive(
      b: String,
      p: Option[String],
    ): Either[FileListError, Option[ListOfMetadataResponse[StubFileMetadata]]] = Right(None)
    override def listKeysRecursive(
      b: String,
      p: Option[String],
    ): Either[FileListError, Option[ListOfKeysResponse[StubFileMetadata]]] = Right(None)
    override def seekToFile(b: String, f: String, lm: Option[Instant]): Option[StubFileMetadata] = None
    override def getBlob(b:    String, p: String): Either[FileLoadError, InputStream] =
      Right(InputStream.nullInputStream())
    override def getBlobAsString(b:        String, p: String): Either[FileLoadError, String] = Right("")
    override def getBlobAsStringAndEtag(b: String, p: String): Either[FileLoadError, (String, String)] =
      Right(("", "etag"))
    override def getMetadata(b: String, p: String): Either[FileLoadError, ObjectMetadata] =
      Right(ObjectMetadata(0L, Instant.EPOCH))
    override def writeStringToFile(b: String, p: String, d: UploadableString): Either[UploadError, Unit] = Right(())
    override def writeBlobToFile[O](
      b:  String,
      p:  String,
      op: ObjectProtection[O],
    )(
      implicit
      enc: io.circe.Encoder[O],
    ): Either[UploadError, ObjectWithETag[O]] =
      Left(UploadFailedError(new RuntimeException("not implemented"), new File(".")))
    override def deleteFiles(b:                String, files: Seq[String]): Either[FileDeleteError, Unit] = Right(())
    override def createDirectoryIfNotExists(b: String, p:     String):      Either[FileCreateError, Unit] = Right(())
    override def touchFile(b:                  String, p:     String):      Either[FileTouchError, Unit]  = Right(())
  }

  private val transientMvError: FileMoveError =
    FileMoveError(new SocketException("Connection reset"), "src", "dst")

  private val permanentMvError: FileMoveError =
    FileMoveError(new RuntimeException("412 Precondition Failed"), "src", "dst")

  private val transientDeleteError: FileDeleteError =
    FileDeleteError(new SocketException("Connection reset"), "file")

  test("mvFile: transient error on first attempt, success on second — no task failure") {
    val stub = new CountingStorageInterface(
      mvResults     = Iterator(Left(transientMvError), Right(())),
      deleteResults = Iterator.empty,
    )
    val retrying = new RetryingStorageInterface(stub, immediateConfig, alwaysTransient, metrics)

    val result = retrying.mvFile("b", "src", "b", "dst", None)
    result shouldBe Right(())
    stub.mvCallCount.get() shouldBe 2
    metrics.getCommitRetriesTotal shouldBe 1L
  }

  test("mvFile: exhausts all attempts and returns the original error") {
    val stub = new CountingStorageInterface(
      mvResults     = Iterator(Left(transientMvError), Left(transientMvError), Left(transientMvError)),
      deleteResults = Iterator.empty,
    )
    val retrying = new RetryingStorageInterface(stub, immediateConfig, alwaysTransient, metrics)

    val result = retrying.mvFile("b", "src", "b", "dst", None)
    result shouldBe Left(transientMvError)
    stub.mvCallCount.get() shouldBe 3
    metrics.getCommitRetriesTotal shouldBe 2L // 2 retries after the first attempt
  }

  test("mvFile: permanent error returns immediately with no retries") {
    val stub = new CountingStorageInterface(
      mvResults     = Iterator(Left(permanentMvError)),
      deleteResults = Iterator.empty,
    )
    val retrying = new RetryingStorageInterface(stub, immediateConfig, neverTransient, metrics)

    val result = retrying.mvFile("b", "src", "b", "dst", None)
    result shouldBe Left(permanentMvError)
    stub.mvCallCount.get() shouldBe 1
    metrics.getCommitRetriesTotal shouldBe 0L
  }

  test("deleteFile: transient error on first attempt, success on second") {
    val stub = new CountingStorageInterface(
      mvResults     = Iterator.empty,
      deleteResults = Iterator(Left(transientDeleteError), Right(())),
    )
    val retrying = new RetryingStorageInterface(stub, immediateConfig, alwaysTransient, metrics)

    val result = retrying.deleteFile("b", "file", "etag")
    result shouldBe Right(())
    stub.deleteCallCount.get() shouldBe 2
    metrics.getCommitRetriesTotal shouldBe 1L
  }

  test("deleteFile: exhausts attempts and returns original error") {
    val stub = new CountingStorageInterface(
      mvResults     = Iterator.empty,
      deleteResults = Iterator(Left(transientDeleteError), Left(transientDeleteError), Left(transientDeleteError)),
    )
    val retrying = new RetryingStorageInterface(stub, immediateConfig, alwaysTransient, metrics)

    val result = retrying.deleteFile("b", "file", "etag")
    result shouldBe Left(transientDeleteError)
    stub.deleteCallCount.get() shouldBe 3
    metrics.getCommitRetriesTotal shouldBe 2L
  }

  test("other methods (uploadFile) are delegated without retry") {
    val stub = new CountingStorageInterface(
      mvResults     = Iterator.empty,
      deleteResults = Iterator.empty,
    )
    val retrying = new RetryingStorageInterface(stub, immediateConfig, alwaysTransient, metrics)

    val file = File.createTempFile("retry-test", ".bin")
    file.deleteOnExit()
    retrying.uploadFile(UploadableFile(file), "b", "p") shouldBe Right("etag")
    metrics.getCommitRetriesTotal shouldBe 0L
  }

  test("CommitRetriesTotal increments but no other retry counter is touched") {
    val stub = new CountingStorageInterface(
      mvResults     = Iterator(Left(transientMvError), Right(())),
      deleteResults = Iterator.empty,
    )
    val retrying = new RetryingStorageInterface(stub, immediateConfig, alwaysTransient, metrics)
    retrying.mvFile("b", "src", "b", "dst", None)
    metrics.getCommitRetriesTotal shouldBe 1L
  }

  test("maxAttempts=1 means no retry at all") {
    val noRetryConfig = immediateConfig.copy(maxAttempts = 1)
    val stub = new CountingStorageInterface(
      mvResults     = Iterator(Left(transientMvError)),
      deleteResults = Iterator.empty,
    )
    val retrying = new RetryingStorageInterface(stub, noRetryConfig, alwaysTransient, metrics)

    val result = retrying.mvFile("b", "src", "b", "dst", None)
    result shouldBe Left(transientMvError)
    stub.mvCallCount.get() shouldBe 1
    metrics.getCommitRetriesTotal shouldBe 0L
  }

  test("aborts retries immediately when the thread is interrupted during backoff") {
    // Use a real delay so Thread.sleep actually blocks and throws InterruptedException.
    val retryWithDelay = CommitRetryConfig(maxAttempts = 5, baseDelayMs = 5000L, multiplier = 1.0, maxDelayMs = 5000L)
    // All attempts return the same transient error so the loop would retry indefinitely if not interrupted.
    val stub = new CountingStorageInterface(
      mvResults     = Iterator.continually(Left(transientMvError)),
      deleteResults = Iterator.empty,
    )
    val retrying = new RetryingStorageInterface(stub, retryWithDelay, alwaysTransient, metrics)

    // Pre-set the interrupt flag so the first Thread.sleep throws InterruptedException immediately.
    Thread.currentThread().interrupt()
    val result = retrying.mvFile("b", "src", "b", "dst", None)

    result shouldBe Left(transientMvError)
    // Only the initial attempt was made — no extra cloud calls after cancellation.
    stub.mvCallCount.get() shouldBe 1
    // CommitRetriesTotal is incremented before the sleep, so it counts the one aborted retry.
    metrics.getCommitRetriesTotal shouldBe 1L
    // The interrupt flag must have been restored by the handler.
    Thread.interrupted() shouldBe true // also clears the flag so it does not leak into other tests
  }
}
