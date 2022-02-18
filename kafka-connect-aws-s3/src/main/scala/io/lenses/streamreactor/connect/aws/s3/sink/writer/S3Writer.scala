
/*
 * Copyright 2021 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.sink.writer

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.sink._
import io.lenses.streamreactor.connect.aws.s3.sink.seek.IndexManager
import io.lenses.streamreactor.connect.aws.s3.storage.{NonExistingFileError, StorageInterface, UploadFailedError, ZeroByteFileError}
import org.apache.kafka.connect.data.Schema

import java.io.File
import scala.math.Ordered.orderingToOrdered
import scala.util.Try

class S3Writer(
                sinkName: String,
                topicPartition: TopicPartition,
                commitPolicy: CommitPolicy,
                indexManager: IndexManager,
                stagingFilenameFn: () => Either[SinkError, File],
                finalFilenameFn: Offset => Either[SinkError, RemoteS3PathLocation],
                formatWriterFn: File => Either[SinkError, S3FormatWriter],
                lastSeekedOffset: Option[Offset],
              )
              (
                implicit storageInterface: StorageInterface
              ) extends LazyLogging {

  private var writeState: WriteState = NoWriter(CommitState(topicPartition, lastSeekedOffset))


  def write(messageDetail: MessageDetail, o: Offset): Either[SinkError, Unit] = {

    def innerMessageWrite(writingState: Writing): Either[NonFatalS3SinkError, Unit] = {
      writingState.s3FormatWriter.write(messageDetail.keySinkData, messageDetail.valueSinkData, topicPartition.topic) match {
        case Left(err: Throwable) =>
          logger.error(err.getMessage)
          NonFatalS3SinkError(err.getMessage, err).asLeft
        case Right(_) =>
          writeState = writingState.updateOffset(o, messageDetail.valueSinkData.schema())
          ().asRight
      }
    }

    writeState match {
      case writingWS@Writing(_, _, _, _) =>
        innerMessageWrite(writingWS)

      case noWriter@NoWriter(_) =>
        val writingStateEither = for {
          file <- stagingFilenameFn()
          formatWriter <- formatWriterFn(file)
          writingState <- noWriter.toWriting(formatWriter, file, o).asRight
        } yield writingState
        writingStateEither.flatMap(writingState => {
          writeState = writingState
          innerMessageWrite(writingState)
        })

      case Uploading(_, _, _) =>
        // before we write we need to retry the upload
        NonFatalS3SinkError("Attempting Write in Uploading State").asLeft
    }
  }

  def commit: Either[SinkError, Unit] = {

    writeState match {
      case writingState@Writing(_, s3FormatWriter, _, _) =>
        s3FormatWriter.complete() match {
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
      case uploadState@Uploading(commitState, file, uncommittedOffset) =>
        for {
          finalFileName <- finalFilenameFn(uncommittedOffset)
          indexFileName <- indexManager.write(topicPartition.withOffset(uncommittedOffset), finalFileName)
          _ <- storageInterface.uploadFile(file, finalFileName).recover {
            case _: NonExistingFileError => ()
            case _: ZeroByteFileError => ()
          }.leftMap {
            case UploadFailedError(exception, _) => NonFatalS3SinkError(exception.getMessage, exception)
          }
          clean <- indexManager.clean(indexFileName, topicPartition)
          stateReset <- Try {
            logger.debug(s"[{}] S3Writer.resetState: Resetting state $writeState", sinkName)
            writeState = uploadState.toNoWriter()
            file.delete()
            logger.debug(s"[{}] S3Writer.resetState: New state $writeState", sinkName)
          }.toEither.leftMap(e => FatalS3SinkError(e.getMessage, commitState.topicPartition))
        } yield stateReset
      case other =>
        FatalS3SinkError(s"Other $other error detected, abort", topicPartition).asLeft

    }
  }

  def close(): Unit = {
    writeState = writeState match {
      case state@NoWriter(_) => state
      case Writing(commitState, s3FormatWriter, file, _) =>
        Try(s3FormatWriter.close())
        Try(file.delete())
        NoWriter(commitState.reset())
      case Uploading(commitState, file, _) =>
        Try(file.delete())
        NoWriter(commitState.reset())
    }
  }

  def getCommittedOffset: Option[Offset] = writeState.getCommitState.committedOffset

  def shouldFlush: Boolean = {
    writeState match {
      case Writing(commitState, _, _, uncommittedOffset) => commitPolicy.shouldFlush(
        CommitContext(
          topicPartition.withOffset(uncommittedOffset),
          commitState.recordCount,
          commitState.lastKnownFileSize,
          commitState.createdTimestamp,
          commitState.lastFlushedTime,
        )
      )
      case NoWriter(_) => false
      case Uploading(_, _, _) => false
    }

  }

  /**
   * If the offsets provided by Kafka Connect have already been processed, then they must be skipped to avoid duplicate records and protect the integrity of the data files.
   *
   * @param currentOffset the current offset
   * @return true if the given offset should be skipped, false otherwise
   */
  def shouldSkip(currentOffset: Offset): Boolean = {

    def largestOffset(maybeCommittedOffset: Option[Offset], uncommittedOffset: Offset): Offset = {
      logger.trace(s"[{}] maybeCommittedOffset: {}, uncommittedOffset: {}", sinkName, maybeCommittedOffset, uncommittedOffset)
      (maybeCommittedOffset.toList :+ uncommittedOffset).max
    }

    def shouldSkipInternal(currentOffset: Offset, latestOffset: Option[Offset]): Boolean = {

      def logSkipOutcome(currentOffset: Offset, latestOffset: Option[Offset], skipRecord: Boolean): Unit = {
        val skipping = if (skipRecord) "SKIPPING" else "PROCESSING"
        logger.debug(s"[$sinkName] lastSeeked=${lastSeekedOffset} current=${currentOffset.value} latest=$latestOffset - $skipping")
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

  def hasPendingUpload(): Boolean = {
    writeState match {
      case Uploading(_, _, _) => true
      case _ => false
    }
  }

  def shouldRollover(schema: Schema): Boolean = {
    rolloverOnSchemaChange &&
      schemaHasChanged(schema)
  }

  private def schemaHasChanged(schema: Schema): Boolean = {
    writeState.getCommitState.lastKnownSchema.exists(_ != schema)
  }

  private def rolloverOnSchemaChange: Boolean = {
    writeState match {
      case Writing(commitState, s3FormatWriter, file, uncommittedOffset) => s3FormatWriter.rolloverFileOnSchemaChange()
      case _ => false
    }
  }

}