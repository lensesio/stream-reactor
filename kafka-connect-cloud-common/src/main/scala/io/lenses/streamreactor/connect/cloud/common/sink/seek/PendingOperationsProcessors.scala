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

class PendingOperationsProcessors(
  storageInterface: StorageInterface[_],
) extends LazyLogging {

  private val copyProcessor:   OperationProcessor[CopyOperation]   = new CopyOperationProcessor(storageInterface)
  private val deleteProcessor: OperationProcessor[DeleteOperation] = new DeleteOperationProcessor(storageInterface)
  private val uploadProcessor: OperationProcessor[UploadOperation] = new UploadOperationProcessor(storageInterface)

  private def getProcessor[T <: FileOperation](in: T): OperationProcessor[T] =
    in match {
      case CopyOperation(_, _, _, _) => copyProcessor.asInstanceOf[OperationProcessor[T]]
      case DeleteOperation(_, _, _)  => deleteProcessor.asInstanceOf[OperationProcessor[T]]
      case UploadOperation(_, _, _)  => uploadProcessor.asInstanceOf[OperationProcessor[T]]
    }

  def processPendingOperations(
    topicPartition:  TopicPartition,
    committedOffset: Option[Offset],
    pendingState:    PendingState,
    fnIndexUpdate:   (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]],
  ): Either[SinkError, Option[Offset]] = {

    def processOperations(operations: NonEmptyList[FileOperation]): Either[SinkError, Option[Offset]] = {
      logger.info(s"processOperations for ${operations.size}")
      val head = operations.head
      val ps   = getProcessor(head)

      NonEmptyList.fromList(operations.tail) match {
        case Some(furtherOps: NonEmptyList[FileOperation]) =>
          logger.info("processOperations further ops")
          for {
            idx <- ps.process(head) match {
              case Left(NonFatalCloudSinkError(_, _, true)) =>
                fnIndexUpdate(
                  topicPartition,
                  committedOffset,
                  Option.empty,
                )
              case Left(e: SinkError) =>
                new FatalCloudSinkError("Unable to resume processOperations: " + e.message(),
                                        e.exception(),
                                        topicPartition,
                ).asLeft
              case Right(newEtag) =>
                val _          = logger.info("processOperations post process")
                val updatedOps = updateEtag(furtherOps, newEtag)

                val _ = logger.info("processOperations post update etag")
                for {

                  _ <- fnIndexUpdate(
                    topicPartition,
                    committedOffset,
                    pendingState.copy(pendingOperations = updatedOps).some,
                  )
                  _ = logger.info("processOperations post update")

                  idx <- processOperations(updatedOps)
                  _    = logger.info("processOperations post recursion")

                } yield idx
            }

          } yield idx
        case None =>
          logger.info("processOperations no further ops")
          for {
            updated <- ps.process(head) match {
              case Left(NonFatalCloudSinkError(_, _, true)) =>
                committedOffset.asRight
              case Left(err) =>
                err.asLeft[Option[Offset]]
              case Right(_) =>
                fnIndexUpdate(
                  topicPartition,
                  pendingState.pendingOffset.some,
                  Option.empty,
                )
            }

          } yield updated
      }
    }

    for {
      idx <- processOperations(pendingState.pendingOperations)
    } yield idx

  }

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

trait OperationProcessor[T <: FileOperation] {
  def process(op: T): Either[SinkError, Option[String]]

  val updatesEtags: Boolean = false
}

// TODO:  this one can only be run where the file exists locally!!
class UploadOperationProcessor(storageInterface: StorageInterface[_]) extends OperationProcessor[UploadOperation] {

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

  override val updatesEtags: Boolean = true
}

class DeleteOperationProcessor(storageInterface: StorageInterface[_]) extends OperationProcessor[DeleteOperation] {

  override def process(op: DeleteOperation): Either[SinkError, Option[String]] =
    for {
      _ <- storageInterface.deleteFile(op.bucket, op.source, op.eTag)
        .leftMap {
          case FileDeleteError(exception, _) => NonFatalCloudSinkError(exception.getMessage, exception.some)
        }
    } yield Option.empty
}

class CopyOperationProcessor(storageInterface: StorageInterface[_]) extends OperationProcessor[CopyOperation] {

  override def process(op: CopyOperation): Either[SinkError, Option[String]] =
    for {
      _ <- storageInterface.mvFile(op.bucket, op.source, op.bucket, op.destination, op.eTag.some)
        .leftMap {
          case FileMoveError(exception, _, _) => NonFatalCloudSinkError(exception.getMessage, exception.some)
        }
    } yield Option.empty

}
