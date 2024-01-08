/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CloudCommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.storage._
import org.apache.kafka.connect.data.Schema

import java.io.File
import scala.math.Ordered.orderingToOrdered
import scala.util.Try

class Writer[SM <: FileMetadata](
  topicPartition:    TopicPartition,
  commitPolicy:      CommitPolicy,
  indexManager:      IndexManager[SM],
  stagingFilenameFn: () => Either[SinkError, File],
  finalFilenameFn:   Offset => Either[SinkError, CloudLocation],
  formatWriterFn:    File => Either[SinkError, FormatWriter],
  lastSeekedOffset:  Option[Offset],
)(
  implicit
  connectorTaskId:  ConnectorTaskId,
  storageInterface: StorageInterface[SM],
) extends LazyLogging {

  private var writeState: WriteState = NoWriter(CommitState(topicPartition, lastSeekedOffset))

  def write(messageDetail: MessageDetail): Either[SinkError, Unit] = {

    def innerMessageWrite(writingState: Writing): Either[NonFatalCloudSinkError, Unit] =
      writingState.formatWriter.write(messageDetail) match {
        case Left(err: Throwable) =>
          logger.error(err.getMessage)
          NonFatalCloudSinkError(err.getMessage, err).asLeft
        case Right(_) =>
          writeState = writingState.updateOffset(messageDetail.offset, messageDetail.value.schema())
          ().asRight
      }

    writeState match {
      case writingWS @ Writing(_, _, _, _) =>
        innerMessageWrite(writingWS)

      case noWriter @ NoWriter(_) =>
        val writingStateEither = for {
          file         <- stagingFilenameFn()
          formatWriter <- formatWriterFn(file)
          writingState <- noWriter.toWriting(formatWriter, file, messageDetail.offset).asRight
        } yield writingState
        writingStateEither.flatMap { writingState =>
          writeState = writingState
          innerMessageWrite(writingState)
        }

      case Uploading(_, _, _) =>
        // before we write we need to retry the upload
        NonFatalCloudSinkError("Attempting Write in Uploading State").asLeft
    }
  }

  def commit: Either[SinkError, Unit] = {

    writeState match {
      case writingState @ Writing(_, formatWriter, _, _) =>
        formatWriter.complete() match {
          case Left(ex) => return ex.asLeft
          case Right(_) =>
        }
        writeState = writingState.toUploading()
      case Uploading(_, _, _) =>
      // your turn will come, nothing to do here because we're already in the correct state
      case NoWriter(_) =>
        // nothing to commit, get out of here
        return ().asRight
    }

    writeState match {
      case uploadState @ Uploading(commitState, file, uncommittedOffset) =>
        for {
          finalFileName <- finalFilenameFn(uncommittedOffset)
          path          <- finalFileName.path.toRight(NonFatalCloudSinkError("No path exists within cloud location"))
          indexFileName <- indexManager.write(
            finalFileName.bucket,
            path,
            topicPartition.withOffset(uncommittedOffset),
          )
          _ <- storageInterface.uploadFile(UploadableFile(file), finalFileName.bucket, path)
            .recover {
              case _: NonExistingFileError => ()
              case _: ZeroByteFileError    => ()
            }
            .leftMap {
              case UploadFailedError(exception, _) => NonFatalCloudSinkError(exception.getMessage, exception)
            }
          _ <- indexManager.clean(finalFileName.bucket, indexFileName, topicPartition)
          stateReset <- Try {
            logger.debug(s"[{}] Writer.resetState: Resetting state $writeState", connectorTaskId.show)
            writeState = uploadState.toNoWriter()
            file.delete()
            logger.debug(s"[{}] Writer.resetState: New state $writeState", connectorTaskId.show)
          }.toEither.leftMap(e => FatalCloudSinkError(e.getMessage, commitState.topicPartition))
        } yield stateReset
      case other =>
        FatalCloudSinkError(s"Other $other error detected, abort", topicPartition).asLeft

    }
  }

  def close(): Unit =
    writeState = writeState match {
      case state @ NoWriter(_) => state
      case Writing(commitState, formatWriter, file, _) =>
        Try(formatWriter.close())
        Try(file.delete())
        NoWriter(commitState.reset())
      case Uploading(commitState, file, _) =>
        Try(file.delete())
        NoWriter(commitState.reset())
    }

  def getCommittedOffset: Option[Offset] = writeState.getCommitState.committedOffset

  def shouldFlush: Boolean =
    writeState match {
      case Writing(commitState, _, file, uncommittedOffset) => commitPolicy.shouldFlush(
          CloudCommitContext(
            topicPartition.withOffset(uncommittedOffset),
            commitState.recordCount,
            commitState.lastKnownFileSize,
            commitState.createdTimestamp,
            commitState.lastFlushedTime,
            file.getName,
          ),
        )
      case NoWriter(_)        => false
      case Uploading(_, _, _) => false
    }

  /**
    * If the offsets provided by Kafka Connect have already been processed, then they must be skipped to avoid duplicate records and protect the integrity of the data files.
    *
    * @param currentOffset the current offset
    * @return true if the given offset should be skipped, false otherwise
    */
  def shouldSkip(currentOffset: Offset): Boolean = {

    def largestOffset(maybeCommittedOffset: Option[Offset], uncommittedOffset: Offset): Offset = {
      logger.trace(s"[{}] maybeCommittedOffset: {}, uncommittedOffset: {}",
                   connectorTaskId.show,
                   maybeCommittedOffset,
                   uncommittedOffset,
      )
      (maybeCommittedOffset.toList :+ uncommittedOffset).max
    }

    def shouldSkipInternal(currentOffset: Offset, latestOffset: Option[Offset]): Boolean = {

      def logSkipOutcome(currentOffset: Offset, latestOffset: Option[Offset], skipRecord: Boolean): Unit = {
        val skipping = if (skipRecord) "SKIPPING" else "PROCESSING"
        logger.debug(
          s"[${connectorTaskId.show}] lastSeeked=${lastSeekedOffset} current=${currentOffset.value} latest=$latestOffset - $skipping",
        )
      }

      val shouldSkip = if (latestOffset.isEmpty) {
        false
      } else if (latestOffset.exists(_ >= currentOffset)) {
        true
      } else {
        false
      }
      logSkipOutcome(currentOffset, latestOffset, skipRecord = shouldSkip)
      shouldSkip
    }

    writeState match {
      case NoWriter(commitState) =>
        shouldSkipInternal(currentOffset, commitState.committedOffset)
      case Uploading(commitState, _, uncommittedOffset) =>
        shouldSkipInternal(currentOffset, Option(largestOffset(commitState.committedOffset, uncommittedOffset)))
      case Writing(commitState, _, _, uncommittedOffset) =>
        shouldSkipInternal(currentOffset, Option(largestOffset(commitState.committedOffset, uncommittedOffset)))
    }
  }

  def hasPendingUpload(): Boolean =
    writeState match {
      case Uploading(_, _, _) => true
      case _                  => false
    }

  def shouldRollover(schema: Schema): Boolean =
    rolloverOnSchemaChange &&
      schemaHasChanged(schema)

  private def schemaHasChanged(schema: Schema): Boolean =
    writeState.getCommitState.lastKnownSchema.exists(_ != schema)

  private def rolloverOnSchemaChange: Boolean =
    writeState match {
      case Writing(_, formatWriter, _, _) => formatWriter.rolloverFileOnSchemaChange()
      case _                              => false
    }

}
