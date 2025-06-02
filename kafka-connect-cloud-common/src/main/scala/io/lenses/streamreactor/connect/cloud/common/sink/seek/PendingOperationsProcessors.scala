/*
 * Copyright 2017-2025 Lenses.io Ltd
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
import cats.implicits.none
import cats.implicits.toBifunctorOps
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.model.Offset
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
   * @param topicPartition  The topic partition for which the operations are being processed.
   * @param committedOffset The last committed offset for the topic partition.
   * @param pendingState    The current pending state containing the list of operations.
   * @param fnIndexUpdate   A function to update the index with the new state.
   * @return Either a `SinkError` if an error occurred, or an `Option[Offset]` representing the updated offset.
   */
  def processPendingOperations(
    topicPartition:  TopicPartition,
    committedOffset: Option[Offset],
    pendingState:    PendingState,
    fnIndexUpdate:   (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]],
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
          processor.process(head) match {
            case Left(NonFatalCloudSinkError(_, _, true)) =>
              logger.warn(s"Non-fatal error encountered. Cancelling pending operations for $topicPartition.")
              fnIndexUpdate(topicPartition, committedOffset, Option.empty)

            case Left(error: SinkError) =>
              logger.error(s"Fatal error encountered while processing $head: ${error.message()}",
                           error.exception().orNull,
              )
              new FatalCloudSinkError(
                s"Unable to resume processOperations: ${error.message()}",
                error.exception(),
                topicPartition,
              ).asLeft

            case Right(newEtag) =>
              logger.trace(s"Successfully processed $head. Updating further operations with new eTag.")
              val updatedOps = updateEtag(furtherOps, newEtag)
              fnIndexUpdate(topicPartition, committedOffset, pendingState.copy(pendingOperations = updatedOps).some)
                .flatMap(_ => processOperations(updatedOps))
          }

        case None =>
          processor.process(head) match {
            case Left(NonFatalCloudSinkError(_, _, true)) =>
              logger.warn(
                s"Non-fatal error encountered. Pending operations will be cancelled but processing will continue. Returning committed offset for $topicPartition.",
              )
              committedOffset.asRight

            case Left(error) =>
              logger.error(s"Error encountered while processing $head: ${error.message()}", error.exception().orNull)
              error.asLeft

            case Right(_) =>
              logger.trace(s"Successfully processed the last operation $head for $topicPartition.")
              fnIndexUpdate(topicPartition, pendingState.pendingOffset.some, Option.empty)
          }
      }
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
   * @return Either a `SinkError` if an error occurred, or an `Option[String]` containing the resulting eTag.
   */
  def process(op: T): Either[SinkError, Option[String]]

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
   * @return Either a `SinkError` if an error occurred, or an `Option[String]` containing the resulting eTag.
   */
  override def process(op: UploadOperation): Either[SinkError, Option[String]] =
    for {
      uploadEtag <- storageInterface.uploadFile(UploadableFile(op.source), op.bucket, op.destination)
        .leftMap {
          case UploadFailedError(exception, _) =>
            NonFatalCloudSinkError(exception.getMessage, exception.some)
          case NonExistingFileError(filename) =>
            NonFatalCloudSinkError(s"non existing file $filename", none, cancelPending = true)
        }

    } yield uploadEtag.some

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
   * @return Either a `SinkError` if an error occurred, or an empty `Option[String]`.
   */
  override def process(op: DeleteOperation): Either[SinkError, Option[String]] =
    for {
      _ <- storageInterface.deleteFile(op.bucket, op.source, op.eTag)
        .leftMap {
          case FileDeleteError(exception, _) => NonFatalCloudSinkError(exception.getMessage, exception.some)
        }
    } yield Option.empty
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
   * @return Either a `SinkError` if an error occurred, or an empty `Option[String]`.
   */
  override def process(op: CopyOperation): Either[SinkError, Option[String]] =
    for {
      _ <- storageInterface.mvFile(op.bucket, op.source, op.bucket, op.destination, op.eTag.some)
        .leftMap {
          case FileMoveError(exception, _, _) => NonFatalCloudSinkError(exception.getMessage, exception.some)
        }
    } yield Option.empty

}
