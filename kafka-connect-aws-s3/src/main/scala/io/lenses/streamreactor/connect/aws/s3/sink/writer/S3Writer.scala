
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
import io.lenses.streamreactor.connect.aws.s3.model.location.{LocalPathLocation, RemoteS3PathLocation}
import io.lenses.streamreactor.connect.aws.s3.sink._
import io.lenses.streamreactor.connect.aws.s3.storage.{FatalNonExistingFileError, FatalZeroByteFileError, StorageInterface, UploadFailedError}
import org.apache.kafka.connect.data.Schema

import scala.util.Try

class S3Writer(
                sinkName: String,
                topicPartition: TopicPartition,
                commitPolicy: CommitPolicy,
                stagingFilenameFn: () => Either[SinkError, LocalPathLocation],
                finalFilenameFn: Offset => Either[SinkError, RemoteS3PathLocation],
                formatWriterFn: LocalPathLocation => Either[SinkError, S3FormatWriter],
              )
              (
                implicit storageInterface: StorageInterface
              ) extends LazyLogging {

  private var writeState: WriteState = NoWriter(CommitState(topicPartition))


  def write(messageDetail: MessageDetail, o: Offset): Either[SinkError, Unit] = {

    writeState match {
      case noWriter @ NoWriter(_) => {
        for {
          localPathLocation <- stagingFilenameFn()
          formatWriter <- formatWriterFn(localPathLocation)
          writer <- noWriter.toWriting(formatWriter, localPathLocation, o).asRight
        } yield writer
      } match {
        case Left(ex) => return ex.asLeft
        case Right(writingWS) => writeState = writingWS
      }
      case _ =>
    }

    writeState match {
      case writingWS @ Writing(_, s3FormatWriter, _, _) =>
        s3FormatWriter.write(messageDetail.keySinkData, messageDetail.valueSinkData, topicPartition.topic) match {
          case Left(err: Throwable) =>
            logger.error(err.getMessage)
            NonFatalS3SinkError(err.getMessage, err).asLeft
          case Right(_) =>
            writeState = writingWS.updateOffset(o, messageDetail.valueSinkData.schema())
            ().asRight
        }

      case NoWriter(commitState) =>
        NonFatalS3SinkError("No Writer Available").asLeft
      case Uploading(commitState, file, uncommittedOffset) =>
        // before we write we need to retry the upload
        NonFatalS3SinkError("Attempting Write in Uploading State").asLeft
    }
  }

  def commit: Either[SinkError, Unit] = {

    writeState match {
      case ws@Writing(commitState, s3FormatWriter, file, uncommittedOffset) =>
        s3FormatWriter.complete() match {
          case Left(ex) => return ex.asLeft
          case Right(_) =>
        }
        writeState = ws.toUploading()
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
          _ <- storageInterface.uploadFile(file, finalFileName) match {
            case Left(e : FatalNonExistingFileError) => FatalS3SinkError(e.message, commitState.topicPartition).asLeft
            case Left(e : FatalZeroByteFileError) => FatalS3SinkError(e.message, commitState.topicPartition).asLeft
            case Left(UploadFailedError(exception, _)) => NonFatalS3SinkError(exception.getMessage, exception).asLeft
            case Right(_) => ().asRight
          }
          stateReset <- Try {
            logger.debug(s"[{}] S3Writer.resetState: Resetting state $writeState", sinkName)
            writeState = uploadState.toNoWriter()
            logger.debug(s"[{}] S3Writer.resetState: New state $writeState", sinkName)
          }.toEither.leftMap(e => FatalS3SinkError(e.getMessage, commitState.topicPartition))
        } yield stateReset
      case other =>
        FatalS3SinkError(s"Other ${other} error detected, abort", topicPartition).asLeft

    }
  }

  def close(): Unit = {
    writeState = writeState match {
      case state @ NoWriter(_) => state
      case Writing(commitState, s3FormatWriter, file, uncommittedOffset) => {
        Try(s3FormatWriter.complete())
        Try(file.delete())
        NoWriter(commitState.reset())
      }
      case Uploading(commitState, file, uncommittedOffset) => {
        Try(file.delete())
        NoWriter(commitState.reset())
      }
    }
  }

  def getCommittedOffset: Option[Offset] = writeState.getCommitState.committedOffset

  def shouldFlush: Boolean = {
    writeState match {
      case NoWriter(commitState) => false
      case Writing(commitState, s3FormatWriter, file, uncommittedOffset) => commitPolicy.shouldFlush(
        CommitContext(
          topicPartition.withOffset(uncommittedOffset),
          commitState.recordCount,
          commitState.lastKnownFileSize,
          commitState.createdTimestamp,
          commitState.lastFlushedTime,
        )
      )
      case Uploading(commitState, file, uncommittedOffset) => false
    }

  }

  /**
    * If the offsets provided by Kafka Connect have already been processed, then they must be skipped to avoid duplicate records and protect the integrity of the data files.
    *
    * @param currentOffset the current offset
    * @return true if the given offset should be skipped, false otherwise
    */
  def shouldSkip(currentOffset: Offset): Boolean = {

    def logSkipOutcome(currentOffset: Offset, latestOffset: Offset, skipRecord: Boolean): Unit = {
      val skipping = if (skipRecord) "SKIPPING" else "PROCESSING"
      val latest: String = latestOffset.value.toString
      logger.info(s"[$sinkName] (current:latest) ${currentOffset.value.toString}:$latest ∴ $skipping")
    }

    writeState match {
      case NoWriter(commitState) => false
      case Uploading(commitState, file, uncommittedOffset) => false
      case Writing(commitState, s3FormatWriter, file, uncommittedOffset) => false
        val neverProcessWhen = uncommittedOffset.value >= currentOffset.value
        if (neverProcessWhen) {
          logSkipOutcome(currentOffset, uncommittedOffset, skipRecord = true)
          return true
        }
        logSkipOutcome(currentOffset, uncommittedOffset, skipRecord = false)
        false
    }



  }

  def hasPendingUpload() : Boolean = {
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