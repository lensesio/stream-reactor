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
package io.lenses.streamreactor.connect.benchmarks.gcs

import cats.implicits.catsSyntaxEitherId
import io.circe.Encoder
import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectProtection
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.storage._
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata

import java.io.InputStream
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

/**
 * A [[StorageInterface]] that never touches the network: every operation succeeds instantly
 * (optionally after a configurable simulated latency) and nothing is persisted -- there is no
 * backing store at all, not even an in-memory map, because a benchmark run never needs to read
 * back what it wrote (unlike [[io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface]],
 * which is built for correctness tests that assert on stored content).
 *
 * `uploadFile` deliberately skips reading the staged file's bytes (no `Files.readAllBytes`) so
 * this interface measures pure orchestration/CPU cost, not local disk I/O -- see the "pure CPU
 * ceiling" sweep in the benchmark plan.
 *
 * Wrapped by production's own `RetryingStorageInterface` + `StorageInterfaceWithMetrics`
 * decorators (see `CloudSinkTask.createWriterMan`), exactly as a real backend would be, so the
 * only thing being swapped out is the actual GCS RPC.
 */
class NoOpGCPStorage(simulatedLatencyMillis: Long = 0L) extends StorageInterface[GCPStorageFileMetadata] {

  private val uploadCount = new AtomicLong(0L)

  // Every storage call that charges simulated latency is a distinct network round-trip a real
  // backend would pay for. `uploadCount` only tracks `uploadFile` (i.e. the flush count), which
  // undercounts the exactly-once commit chain's ~7 latency-charged calls per flush (upload +
  // temp-file move + temp-file delete + index/lock bookkeeping writes). `chargedOps` counts them
  // all, so the honest records-per-round-trip amortisation is visible.
  private val chargedOps = new AtomicLong(0L)

  /** Flush count: number of `uploadFile` calls (one per uploaded data file). */
  def totalUploads: Long = uploadCount.get()

  /**
   * Number of latency-charged storage calls actually made across all override methods that invoke
   * [[simulateLatency]] (uploadFile, writeBlobToFile, writeStringToFile, deleteFile, mvFile).
   */
  def totalChargedOps: Long = chargedOps.get()

  private def simulateLatency(): Unit = {
    chargedOps.incrementAndGet()
    if (simulatedLatencyMillis > 0) Thread.sleep(simulatedLatencyMillis)
  }

  private def freshETag(): String = UUID.randomUUID().toString

  override def system(): String = "noop-gcs-benchmark"

  override def close(): Unit = ()

  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] = {
    simulateLatency()
    uploadCount.incrementAndGet()
    freshETag().asRight
  }

  override def pathExists(bucket: String, path: String): Either[PathError, Boolean] = false.asRight

  override def list(
    bucket:     String,
    prefix:     Option[String],
    lastFile:   Option[GCPStorageFileMetadata],
    numResults: Int,
  ): Either[FileListError, Option[ListOfKeysResponse[GCPStorageFileMetadata]]] = None.asRight

  override def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[GCPStorageFileMetadata]]] = None.asRight

  override def listKeysRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfKeysResponse[GCPStorageFileMetadata]]] = None.asRight

  override def seekToFile(
    bucket:       String,
    fileName:     String,
    lastModified: Option[Instant],
  ): Option[GCPStorageFileMetadata] = None

  override def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream] =
    FileNotFoundError(new java.io.FileNotFoundException(path), path).asLeft

  override def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String] =
    FileNotFoundError(new java.io.FileNotFoundException(path), path).asLeft

  override def getBlobAsStringAndEtag(bucket: String, path: String): Either[FileLoadError, (String, String)] =
    FileNotFoundError(new java.io.FileNotFoundException(path), path).asLeft

  override def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata] =
    FileNotFoundError(new java.io.FileNotFoundException(path), path).asLeft

  override def writeBlobToFile[O](
    bucket:           String,
    path:             String,
    objectProtection: ObjectProtection[O],
  )(
    implicit
    encoder: Encoder[O],
  ): Either[UploadError, ObjectWithETag[O]] = {
    val _ = encoder // no serialisation performed; nothing is persisted
    simulateLatency()
    ObjectWithETag(objectProtection.wrappedObject, freshETag()).asRight
  }

  override def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit] = {
    simulateLatency()
    ().asRight
  }

  override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] = ().asRight

  override def deleteFile(bucket: String, file: String, eTag: String): Either[FileDeleteError, Unit] = {
    simulateLatency()
    ().asRight
  }

  override def deleteFilesIfMatch(bucket: String, files: Seq[ConditionalDelete]): Seq[DeleteOutcome] = {
    simulateLatency()
    files.map(_ => DeleteOutcome.Deleted)
  }

  override def mvFile(
    oldBucket: String,
    oldPath:   String,
    newBucket: String,
    newPath:   String,
    maybeEtag: Option[String],
  ): Either[FileMoveError, Unit] = {
    simulateLatency()
    ().asRight
  }

  override def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit] = ().asRight

  override def touchFile(bucket: String, path: String): Either[FileTouchError, Unit] = ().asRight
}
