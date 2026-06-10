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

import com.typesafe.scalalogging.LazyLogging
import io.circe.Encoder
import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.config.CommitRetryConfig
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectProtection
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag

import java.io.InputStream
import java.time.Instant

/**
 * Decorator around [[StorageInterface]] that adds bounded exponential-backoff
 * retry for transient network failures on the commit-chain Copy (`mvFile`) and
 * Delete (`deleteFile`) operations.
 *
 * Only these two methods are retried because:
 *   - Both are idempotent: the final destination key is deterministic (Copy) and
 *     deleting an already-absent blob is harmless (Delete).
 *   - Retrying `uploadFile` is handled at a higher level via `recommitPending`.
 *   - Lock writes (`writeBlobToFile` with `ObjectWithETag`) MUST NOT be retried
 *     here — a `412` precondition failure is the zombie-fencing signal and must
 *     remain fatal.
 *
 * On exhaustion the original `Left` is returned unchanged so that
 * `PendingOperationsProcessors` can apply its normal Fatal/NonFatal escalation.
 *
 * Increments [[CloudSinkMetrics.incrementCommitRetriesTotal]] once per retry
 * attempt (not counting the initial attempt) for dashboard observability,
 * keeping these counts distinct from upload-retry counts
 * ([[CloudSinkMetrics.incrementPendingOperationRetriesTotal]]).
 *
 * @param delegate   The underlying storage implementation.
 * @param retryConfig Retry settings (max attempts, backoff, multiplier, cap).
 * @param classifier Classifies a `Throwable` as transient (safe to retry) or permanent.
 * @param metrics    Metrics sink; `incrementCommitRetriesTotal` is called per retry.
 */
class RetryingStorageInterface[SM <: FileMetadata](
  delegate:     StorageInterface[SM],
  retryConfig:  CommitRetryConfig,
  classifier:   TransientErrorClassifier,
  metrics:      CloudSinkMetrics,
) extends StorageInterface[SM]
    with LazyLogging {

  /**
   * Executes `op` with bounded exponential-backoff retry, returning the original
   * `Left` on permanent error or when attempts are exhausted.
   *
   * Sleep happens between attempts (not before the first). The delay sequence is:
   *   baseDelay, baseDelay * multiplier, ..., up to maxDelay.
   */
  private def withRetry[E <: UploadError, A](opName: String)(op: => Either[E, A]): Either[E, A] = {
    val maxAttempts = retryConfig.maxAttempts.max(1)

    @annotation.tailrec
    def loop(attempt: Int, delayMs: Long): Either[E, A] = {
      val result = op
      result match {
        case Right(_) => result
        case Left(_) if attempt >= maxAttempts =>
          result
        case Left(err) =>
          val maybeTransient = err.toExceptionOption.exists(classifier.isTransient)
          if (!maybeTransient) {
            result
          } else {
            logger.warn(
              s"[$opName] Transient error on attempt $attempt/$maxAttempts, retrying in ${delayMs}ms: ${err.message()}",
              err.toExceptionOption.orNull,
            )
            metrics.incrementCommitRetriesTotal()
            val interrupted =
              try { Thread.sleep(delayMs); false }
              catch { case _: InterruptedException => Thread.currentThread().interrupt(); true }
            if (interrupted) {
              logger.warn(
                s"[$opName] Interrupted during commit-retry backoff; aborting retries to allow prompt " +
                  s"shutdown/rebalance and avoid further cloud calls. Returning last error.",
              )
              result
            } else {
              val nextDelay = (delayMs * retryConfig.multiplier).toLong.min(retryConfig.maxDelayMs)
              loop(attempt + 1, nextDelay)
            }
          }
      }
    }

    loop(1, retryConfig.baseDelayMs)
  }

  override def mvFile(
    oldBucket: String,
    oldPath:   String,
    newBucket: String,
    newPath:   String,
    maybeEtag: Option[String],
  ): Either[FileMoveError, Unit] =
    withRetry(s"mvFile($oldBucket/$oldPath -> $newBucket/$newPath)")(
      delegate.mvFile(oldBucket, oldPath, newBucket, newPath, maybeEtag),
    )

  override def deleteFile(bucket: String, file: String, eTag: String): Either[FileDeleteError, Unit] =
    withRetry(s"deleteFile($bucket/$file)")(
      delegate.deleteFile(bucket, file, eTag),
    )

  // All remaining methods delegate unchanged.

  override def system(): String = delegate.system()

  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] =
    delegate.uploadFile(source, bucket, path)

  override def close(): Unit = delegate.close()

  override def pathExists(bucket: String, path: String): Either[PathError, Boolean] =
    delegate.pathExists(bucket, path)

  override def list(
    bucket:     String,
    prefix:     Option[String],
    lastFile:   Option[SM],
    numResults: Int,
  ): Either[FileListError, Option[ListOfKeysResponse[SM]]] =
    delegate.list(bucket, prefix, lastFile, numResults)

  override def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[SM]]] =
    delegate.listFileMetaRecursive(bucket, prefix)

  override def listKeysRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfKeysResponse[SM]]] =
    delegate.listKeysRecursive(bucket, prefix)

  override def seekToFile(
    bucket:       String,
    fileName:     String,
    lastModified: Option[Instant],
  ): Option[SM] = delegate.seekToFile(bucket, fileName, lastModified)

  override def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream] =
    delegate.getBlob(bucket, path)

  override def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String] =
    delegate.getBlobAsString(bucket, path)

  override def getBlobAsStringAndEtag(bucket: String, path: String): Either[FileLoadError, (String, String)] =
    delegate.getBlobAsStringAndEtag(bucket, path)

  override def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata] =
    delegate.getMetadata(bucket, path)

  override def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit] =
    delegate.writeStringToFile(bucket, path, data)

  override def writeBlobToFile[O](
    bucket:           String,
    path:             String,
    objectProtection: ObjectProtection[O],
  )(
    implicit
    encoder: Encoder[O],
  ): Either[UploadError, ObjectWithETag[O]] =
    delegate.writeBlobToFile(bucket, path, objectProtection)

  override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] =
    delegate.deleteFiles(bucket, files)

  override def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit] =
    delegate.createDirectoryIfNotExists(bucket, path)

  override def touchFile(bucket: String, path: String): Either[FileTouchError, Unit] =
    delegate.touchFile(bucket, path)
}
