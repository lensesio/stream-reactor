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
import cats.implicits.catsSyntaxOptionId
import cats.implicits.toBifunctorOps
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError

import java.io.File

/**
 * A class responsible for processing pending file operations for a specific topic partition
 * in a cloud storage-backed Kafka Connect SinkTask. This class handles operations such as
 * copying, deleting, and uploading files, and ensures that errors are handled appropriately.
 *
 * Pending operations are stored in the index files associated with each topic partition.
 * These index files act as a checkpoint mechanism, ensuring that any task picking up the
 * work can resume and complete the pending operations before processing new offsets. As
 * each operation is processed, the index file is updated with the new state, including
 * the updated list of pending operations and the latest committed offset. This ensures
 * that the system remains consistent and fault-tolerant, even in the event of task failures
 * or restarts.
 *
 * eTags are used to ensure consistency and prevent race conditions when multiple sources
 * attempt to write to the same file in cloud storage. Each operation checks the eTag of the
 * file before performing the operation, ensuring that the file has not been modified by
 * another source since the eTag was last retrieved.
 *
 * @param storageInterface The storage interface used to interact with the cloud storage.
 */
class PendingOperationsProcessors(
  storageInterface: StorageInterface[_],
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends LazyLogging {

  // Processor for handling copy operations.
  private val copyProcessor: OperationProcessor[CopyOperation] = new CopyOperationProcessor(storageInterface)
  // Processor for handling delete operations.
  private val deleteProcessor: OperationProcessor[DeleteOperation] = new DeleteOperationProcessor(storageInterface)
  // Processor for handling upload operations.
  private val uploadProcessor: OperationProcessor[UploadOperation] = new UploadOperationProcessor(storageInterface)

  /**
   * Retrieves the appropriate processor for a given file operation.
   *
   * @param in The file operation to process.
   * @tparam T The type of the file operation.
   * @return The corresponding `OperationProcessor` for the given file operation.
   */
  private def getProcessor[T <: FileOperation](in: T): OperationProcessor[T] =
    in match {
      case CopyOperation(_, _, _, _) => copyProcessor.asInstanceOf[OperationProcessor[T]]
      case DeleteOperation(_, _, _)  => deleteProcessor.asInstanceOf[OperationProcessor[T]]
      case UploadOperation(_, _, _)  => uploadProcessor.asInstanceOf[OperationProcessor[T]]
    }

  /**
   * Processes a list of pending file operations for a specific topic partition.
   *
   * Each operation checks the eTag of the file to ensure that it has not been modified
   * by another source. If the eTag does not match, the operation is aborted to prevent
   * overwriting changes made by another source. This mechanism ensures consistency and
   * protects against concurrent writes.
   *
   * Each operation is processed in sequence, and the index file is updated after each
   * operation to reflect the new state. This ensures that any task picking up the work
   * can resume from the last known state. If an operation fails, the system ensures that
   * the remaining operations are not processed, and the index file is left in a consistent
   * state. This mechanism ensures that pending operations are completed before new offsets
   * are processed, maintaining data integrity and consistency.
   *
   * @param topicPartition   The topic partition for which the operations are being processed.
   * @param committedOffset  The last committed offset for the topic partition.
   * @param pendingState     The current pending state containing the list of operations.
   * @param fnIndexUpdate    A function to update the index with the new state.
   * @param escalateOnCancel When `true`, a `NonExistingFileError` on an `UploadOperation` is escalated to
   *                         `FatalCloudSinkError` so the task fails fast (used by the live commit path).
   *                         When `false` (the default), the same error gracefully clears the pending state
   *                         and returns the old committed offset (used by the dead-worker recovery paths
   *                         in `IndexManagerV2.open`, `loadGranularLock`, `resolveAndCacheGranularLock`).
   * @param partitionKey     Optional PARTITIONBY partition key for diagnostic logging.
   * @param stagingFile      Optional reference to the live staging file for diagnostic logging.
   * @return Either a `SinkError` if an error occurred, or an `Option[Offset]` representing the updated offset.
   */
  def processPendingOperations(
    topicPartition:   TopicPartition,
    committedOffset:  Option[Offset],
    pendingState:     PendingState,
    fnIndexUpdate:    (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]],
    escalateOnCancel: Boolean        = false,
    partitionKey:     Option[String] = None,
    stagingFile:      Option[File]   = None,
  ): Either[SinkError, Option[Offset]] = {

    /**
     * Processes a non-empty list of file operations recursively.
     *
     * @param operations The list of file operations to process.
     * @return Either a `SinkError` if an error occurred, or an `Option[Offset]` representing the updated offset.
     */
    def processOperations(operations: NonEmptyList[FileOperation]): Either[SinkError, Option[Offset]] = {
      logger.trace(s"Starting to process ${operations.size} pending operations for topic partition $topicPartition.")

      val head      = operations.head
      val processor = getProcessor(head)

      NonEmptyList.fromList(operations.tail) match {
        case Some(furtherOps) =>
          // IMPORTANT: case order below is load-bearing.
          // 1. Live-commit cancellation MUST come BEFORE the dead-worker-recovery case,
          //    because both cases match on `(UploadOperation, Left(NonExistingFileError(_)))`
          //    and only the guard `if escalateOnCancel` distinguishes them. Reordering would
          //    silently route live commits into the recovery branch and break fail-fast.
          // 2. Both NonExistingFileError cases MUST come BEFORE the generic transient-upload
          //    case, otherwise NonExistingFileError would be classified as a transient error
          //    and trigger recommitPending (which would also fail, but with no operator signal).
          // 3. The generic `(_, Left(uploadErr))` mid-chain Fatal case MUST come LAST so it
          //    does NOT shadow the Upload-specific cases above.
          (head, processor.process(head)) match {
            // Live-commit cancellation: tampering with task-private state. Fail fast.
            case (upload: UploadOperation, Left(NonExistingFileError(missing))) if escalateOnCancel =>
              escalateLiveCancel(topicPartition, upload, missing, pendingState, fnIndexUpdate)

            // Dead-worker recovery: legitimate, gracefully clear and continue.
            case (_: UploadOperation, Left(NonExistingFileError(_))) =>
              logger.warn(
                s"[${connectorTaskId.show}] Dead-worker recovery: local file from previous task missing for $topicPartition; clearing PendingState.",
              )
              fnIndexUpdate(topicPartition, committedOffset, Option.empty)

            // Transient Upload failures keep NonFatal so recommitPending can retry from the still-on-disk local file.
            // Escalating to Fatal here would trigger CloudSinkTask.rollback -> WriterManager.cleanUp ->
            // Writer.close, which deletes the local temp file before the upload is retried, permanently
            // losing all buffered records that had not yet been uploaded.
            case (_: UploadOperation, Left(uploadErr)) =>
              logger.warn(
                s"[${connectorTaskId.show}] Transient upload failure for $topicPartition; deferring to recommitPending: ${uploadErr.message()}",
                uploadErr.toExceptionOption.orNull,
              )
              NonFatalCloudSinkError(uploadErr.message(), uploadErr.toExceptionOption).asLeft

            // Mid-chain Copy/Delete errors escalate to Fatal. The data is already in cloud storage
            // at .temp-upload/<uuid> and the granular/master lock has a PendingState referencing
            // that uuid, so the existing crash-recovery path can resume the chain after cleanup.
            case (_, Left(uploadErr)) =>
              logger.error(
                s"[${connectorTaskId.show}] Fatal error encountered while processing $head: ${uploadErr.message()}",
                uploadErr.toExceptionOption.orNull,
              )
              new FatalCloudSinkError(
                s"Unable to resume processOperations: ${uploadErr.message()}",
                uploadErr.toExceptionOption,
                topicPartition,
              ).asLeft

            case (_, Right(newEtag)) =>
              logger.trace(s"Successfully processed $head. Updating further operations with new eTag.")
              val updatedOps = updateEtag(furtherOps, newEtag)
              fnIndexUpdate(topicPartition, committedOffset, pendingState.copy(pendingOperations = updatedOps).some)
                .flatMap(_ => processOperations(updatedOps))
          }

        case None =>
          // The head IS the last (or only) op in the chain. Currently reachable for:
          //   - UploadOperation: TODAY only in NoIndexManager mode (indexingEnabled=false), where
          //     Writer.commit builds the single-op chain [Upload] (Writer.scala:164-167). NOT
          //     reachable from IndexManagerV2-backed writers TODAY because the only persisted-
          //     pendingState write happens AFTER an op succeeds with the REMAINING ops, so a
          //     single-op [Upload] pendingState cannot be deserialized from a master/granular
          //     lock at present. This is enforced by chain-construction code, NOT by a type
          //     constraint -- a future change that lets [Upload] alone be persisted would make
          //     this branch reachable in indexed mode too, so the live-vs-recovery split below
          //     MUST stay symmetric with the multi-op (Some(furtherOps)) branch.
          //   - DeleteOperation: NORMAL last-op for IndexManagerV2-backed [Upload, Copy, Delete]
          //     chains, reached via recursion after Upload and Copy have succeeded.
          //   - CopyOperation: never reachable here today.
          // Future maintainers must NOT add a multi-op-style `UploadOperation` branch here that
          // would conflict with the `Some(furtherOps)` handling, and must NOT escalate non-Upload
          // last-op errors to Fatal (Delete failures stay NonFatal -- preserves current behaviour).
          (head, processor.process(head)) match {
            // Live-commit cancellation on a single-op chain (NoIndexManager path)
            case (upload: UploadOperation, Left(NonExistingFileError(missing))) if escalateOnCancel =>
              escalateLiveCancel(topicPartition, upload, missing, pendingState, fnIndexUpdate)

            // Dead-worker recovery on a single-op chain
            case (_: UploadOperation, Left(NonExistingFileError(_))) =>
              logger.warn(
                s"[${connectorTaskId.show}] Dead-worker recovery: local file from previous task missing for $topicPartition; clearing PendingState.",
              )
              fnIndexUpdate(topicPartition, committedOffset, Option.empty)

            // Non-Upload last-op errors (Delete) stay NonFatal -- preserves current behaviour.
            case (_, Left(uploadErr)) =>
              logger.error(
                s"[${connectorTaskId.show}] Error encountered while processing $head: ${uploadErr.message()}",
                uploadErr.toExceptionOption.orNull,
              )
              NonFatalCloudSinkError(uploadErr.message(), uploadErr.toExceptionOption).asLeft

            case (_, Right(_)) =>
              logger.trace(s"Successfully processed the last operation $head for $topicPartition.")
              fnIndexUpdate(topicPartition, pendingState.pendingOffset.some, Option.empty)
          }
      }
    }

    /**
     * Live-commit cancellation handler. Logs a rich operator-facing ERROR line with the full
     * context, performs a best-effort `fnIndexUpdate` to clear the stale `PendingState`
     * (failure does NOT mask the Fatal -- the next restart's `IndexManagerV2.open` will
     * gracefully clear it via the same code path), and returns `Left(FatalCloudSinkError)`.
     */
    def escalateLiveCancel(
      tp:           TopicPartition,
      upload:       UploadOperation,
      missing:      File,
      pending:      PendingState,
      fnIndexUpdt:  (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]],
    ): Either[SinkError, Option[Offset]] = {
      val message = buildOperatorMessage(tp, missing, pending)
      logger.error(message)
      val _ = fnIndexUpdt(tp, committedOffset, Option.empty)
        .leftMap(err =>
          logger.warn(
            s"[${connectorTaskId.show}] Best-effort PendingState clear failed pre-Fatal for $tp: ${err.message()}",
            err.exception().orNull,
          ),
        )
      // Use IllegalStateException to carry the missing path; UploadError is not a Throwable.
      val cause = new IllegalStateException(missing.getPath)
      new FatalCloudSinkError(message, cause.some, tp).asLeft
    }

    /** Builds the operator-facing message used both for the ERROR log and the FatalCloudSinkError. */
    def buildOperatorMessage(
      tp:      TopicPartition,
      missing: File,
      pending: PendingState,
    ): String = {
      val pkStr           = partitionKey.getOrElse("<none>")
      val stagingPath     = stagingFile.map(_.getAbsolutePath).getOrElse(missing.getPath)
      val parentExistsStr = stagingFile.flatMap(f => Option(f.getParentFile)).map(_.exists().toString).getOrElse("<unknown>")
      val opsSummary      = pending.pendingOperations.toList.map(opSummary).mkString(" -> ")
      s"[${connectorTaskId.show}] Local staging file disappeared mid-commit for ${tp.topic.value}-${tp.partition} " +
        s"(partitionKey=$pkStr, staging=$stagingPath, parentExists=$parentExistsStr, " +
        s"pendingOffset=${pending.pendingOffset.value}, ops=[$opsSummary]). " +
        s"Refusing to silently lose buffered records. Failing the task so Connect can restart it; on restart, " +
        s"IndexManagerV2.open will seek the consumer back from the master lock and Kafka will re-deliver " +
        s"the records. Likely cause: tmpwatch/tmpreaper, container RuntimeDirectory clean-up, or manual " +
        s"deletion of the task's staging directory. Configure the staging directory on durable, task-private " +
        s"storage that is not subject to background clean-up."
    }

    def opSummary(op: FileOperation): String = op match {
      case UploadOperation(bucket, _, dest) => s"Upload(bucket=$bucket, dest=$dest)"
      case CopyOperation(bucket, src, dest, _) => s"Copy(bucket=$bucket, src=$src, dest=$dest)"
      case DeleteOperation(bucket, src, _) => s"Delete(bucket=$bucket, src=$src)"
    }

    logger.trace(
      s"Processing pending operations for topic partition $topicPartition with committed offset $committedOffset.",
    )
    processOperations(pendingState.pendingOperations)
  }

  /**
   * Updates the eTag for a list of file operations.
   *
   * eTags are used to ensure that the file being operated on has not been modified
   * by another source since the last operation. This prevents race conditions and
   * ensures that operations are performed on the correct version of the file.
   * The updated eTag is also stored in the index file to ensure that subsequent
   * operations can verify the file's integrity.
   *
   * @param tail      The list of file operations to update.
   * @param maybeEtag The new eTag to apply to the operations.
   * @return The updated list of file operations.
   */
  private def updateEtag(tail: NonEmptyList[FileOperation], maybeEtag: Option[String]): NonEmptyList[FileOperation] =
    maybeEtag match {
      case None => tail
      case Some(eTag: String) =>
        tail.map {
          case c: CopyOperation   => c.copy(eTag = eTag)
          case d: DeleteOperation => d.copy(eTag = eTag)
          case x => x
        }
    }

}

/**
 * A trait representing a processor for a specific type of file operation.
 *
 * @tparam T The type of the file operation.
 */
trait OperationProcessor[T <: FileOperation] {

  /**
   * Processes a file operation.
   *
   * @param op The file operation to process.
   * @return Either an `UploadError` if an error occurred, or an `Option[String]` containing the resulting eTag.
   */
  def process(op: T): Either[UploadError, Option[String]]

  // Indicates whether the processor updates eTags of further operations.
  val updatesEtags: Boolean = false
}

/**
 * Processor for handling upload operations in the cloud storage system.
 *
 * This processor is responsible for uploading files from a local source to a specified
 * destination in the cloud storage. It ensures that the operation is executed correctly
 * and returns the resulting eTag of the uploaded file if successful.
 *
 * This processor can only be run where the file exists locally.  If it is run without
 * the existence of the file then it will discard the progress since the last committed
 * offset and ensure that the offsets are reprocessed.
 *
 * @param storageInterface The storage interface used to interact with the cloud storage.
 */
class UploadOperationProcessor(storageInterface: StorageInterface[_]) extends OperationProcessor[UploadOperation] {

  /**
   * Processes an upload operation by uploading a file to the cloud storage.
   *
   * @param op The upload operation containing the source file, bucket, and destination path.
   * @return Either an `UploadError` if an error occurred, or an `Option[String]` containing the resulting eTag.
   */
  override def process(op: UploadOperation): Either[UploadError, Option[String]] =
    storageInterface.uploadFile(UploadableFile(op.source), op.bucket, op.destination).map(_.some)

  /**
   * Indicates that this processor updates eTags of further operations.
   */
  override val updatesEtags: Boolean = true
}

/**
 * Processor for handling delete operations in the cloud storage system.
 *
 * This processor is responsible for deleting files from the cloud storage.
 * It ensures that the operation is executed correctly and does not return an eTag.
 *
 * @param storageInterface The storage interface used to interact with the cloud storage.
 */
class DeleteOperationProcessor(storageInterface: StorageInterface[_]) extends OperationProcessor[DeleteOperation] {

  /**
   * Processes a delete operation by removing a file from the cloud storage.
   *
   * @param op The delete operation containing the bucket, source file, and eTag.
   * @return Either an `UploadError` if an error occurred, or an empty `Option[String]`.
   */
  override def process(op: DeleteOperation): Either[UploadError, Option[String]] =
    storageInterface.deleteFile(op.bucket, op.source, op.eTag).map(_ => Option.empty)
}

/**
 * Processor for handling copy operations in the cloud storage system.
 *
 * This processor is responsible for moving files within the cloud storage.
 * It ensures that the operation is executed correctly and does not return an eTag.
 *
 * @param storageInterface The storage interface used to interact with the cloud storage.
 */
class CopyOperationProcessor(storageInterface: StorageInterface[_]) extends OperationProcessor[CopyOperation] {

  /**
   * Processes a copy operation by moving a file within the cloud storage.
   *
   * @param op The copy operation containing the bucket, source file, destination path, and eTag.
   * @return Either an `UploadError` if an error occurred, or an empty `Option[String]`.
   */
  override def process(op: CopyOperation): Either[UploadError, Option[String]] =
    storageInterface.mvFile(op.bucket, op.source, op.bucket, op.destination, op.eTag.some).map(_ => Option.empty)

}
