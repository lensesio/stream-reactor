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
package io.lenses.streamreactor.connect.cloud.common.sink.writer

import cats.data.NonEmptyList
import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.confluent.connect.avro.AvroData
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CloudCommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CopyOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.DeleteOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.FileOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.UploadOperation
import io.lenses.streamreactor.connect.cloud.common.storage._
import org.apache.kafka.connect.data.Schema

import java.io.File
import java.util.UUID
import scala.math.Ordered.orderingToOrdered
import scala.util.Try

class Writer[SM <: FileMetadata](
  topicPartition:              TopicPartition,
  commitPolicy:                CommitPolicy,
  indexManager:                IndexManager,
  stagingFilenameFn:           () => Either[SinkError, File],
  objectKeyBuilder:            ObjectKeyBuilder,
  formatWriterFn:              File => Either[SinkError, FormatWriter],
  schemaChangeDetector:        SchemaChangeDetector,
  pendingOperationsProcessors: PendingOperationsProcessors,
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends LazyLogging {

  private val lastSeekedOffset: Option[Offset] = indexManager.getSeekedOffsetForTopicPartition(topicPartition)

  var writeState: WriteState = NoWriter(CommitState(topicPartition, lastSeekedOffset))

  def write(messageDetail: MessageDetail): Either[SinkError, Unit] = {

    def innerMessageWrite(writingState: Writing): Either[NonFatalCloudSinkError, Unit] =
      writingState.formatWriter.write(messageDetail) match {
        case Left(err: Throwable) =>
          val avroDataConverter = new AvroData(2)
          val schema            = messageDetail.value.schema().map(avroDataConverter.fromConnectSchema)
          val keySchema         = messageDetail.key.schema().map(avroDataConverter.fromConnectSchema)
          logger.error(
            s"An error occurred while writing using ${writingState.formatWriter.getClass.getSimpleName}. " +
              s"Details: Topic-Partition: ${messageDetail.topic.value}-${messageDetail.partition}, " +
              s"Offset: ${messageDetail.offset.value}, " +
              s"Key: ${messageDetail.key}, " +
              s"Key schema: ${keySchema.getOrElse("<null>")}, " +
              s"Value: ${messageDetail.value}, " +
              s"Value schema: ${schema.getOrElse("<null>")}, " +
              s"Headers: ${messageDetail.headers}.",
            err,
          )
          NonFatalCloudSinkError(err.getMessage, err.some).asLeft
        case Right(_) =>
          writeState =
            writingState.update(messageDetail.offset, messageDetail.epochTimestamp, messageDetail.value.schema())
          ().asRight
      }

    writeState match {
      case writingWS: Writing =>
        innerMessageWrite(writingWS)

      case noWriter @ NoWriter(_) =>
        val writingStateEither = for {
          file         <- stagingFilenameFn()
          formatWriter <- formatWriterFn(file)
          writingState <-
            noWriter.toWriting(formatWriter, file, messageDetail.offset, messageDetail.epochTimestamp).asRight
        } yield writingState
        writingStateEither.flatMap { writingState =>
          writeState = writingState
          innerMessageWrite(writingState)
        }

      case _: Uploading =>
        // before we write we need to retry the upload
        NonFatalCloudSinkError("Attempting Write in Uploading State").asLeft
    }
  }

  def commit: Either[SinkError, Unit] = {

    writeState match {
      case writingState: Writing =>
        writingState.formatWriter.complete() match {
          case Left(ex) => return ex.asLeft
          case Right(_) =>
        }
        writeState = writingState.toUploading
      case _: Uploading =>
      // your turn will come, nothing to do here because we're already in the correct state
      case NoWriter(_) =>
        // nothing to commit, get out of here
        return ().asRight
    }

    writeState match {
      case uploadState @ Uploading(commitState,
                                   file,
                                   uncommittedOffset,
                                   earliestRecordTimestamp,
                                   latestRecordTimestamp,
          ) =>
        for {
          key         <- objectKeyBuilder.build(uncommittedOffset, earliestRecordTimestamp, latestRecordTimestamp)
          path        <- key.path.toRight(NonFatalCloudSinkError("No path exists within cloud location"))
          pendingOperations = if (indexManager.indexingEnabled) {
            val tempFileUuid = UUID.randomUUID().toString
            val tempFileName = path.prependedAll(
              s".temp-upload/${topicPartition.topic}/${topicPartition.partition}/$tempFileUuid",
            )

            NonEmptyList.of[FileOperation](
              UploadOperation(key.bucket, file, tempFileName),
              CopyOperation(key.bucket, tempFileName, path, "placeholder"),
              DeleteOperation(key.bucket, tempFileName, "placeholder"),
            )
          } else
            NonEmptyList.of[FileOperation](
              UploadOperation(key.bucket, file, path)
            )

          _ <- pendingOperationsProcessors.processPendingOperations(
            topicPartition,
            getCommittedOffset,
            PendingState(uncommittedOffset, pendingOperations),
            indexManager.update,
          )
          stateReset <- Try {
            logger.debug(s"[{}] Writer.resetState: Resetting state $writeState", connectorTaskId.show)
            writeState = uploadState.toNoWriter
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
      case Writing(commitState, formatWriter, file, _, _, _) =>
        Try(formatWriter.close())
        Try(file.delete())
        NoWriter(commitState.reset())
      case Uploading(commitState, file, _, _, _) =>
        Try(file.delete())
        NoWriter(commitState.reset())
    }

  def getCommittedOffset: Option[Offset] = writeState.getCommitState.committedOffset

  def shouldFlush: Boolean =
    writeState match {
      case Writing(commitState, _, file, uncommittedOffset, _, _) => commitPolicy.shouldFlush(
          CloudCommitContext(
            topicPartition.withOffset(uncommittedOffset),
            commitState.recordCount,
            commitState.lastKnownFileSize,
            commitState.createdTimestamp,
            commitState.lastFlushedTime,
            file.getName,
          ),
        )
      case NoWriter(_) => false
      case _: Uploading => false
    }

  /**
   * If the offsets provided by Kafka Connect have already been processed, then they must be skipped to avoid duplicate records and protect the integrity of the data files.
   *
   * @param currentOffset the current offset
   * @return true if the given offset should be skipped, false otherwise
   */
  def shouldSkip(currentOffset: Offset): Boolean =
    if (!indexManager.indexingEnabled) false
    else {

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
            s"[${connectorTaskId.show}] lastSeeked=${lastSeekedOffset.getOrElse("None")} current=${currentOffset.value} latest=${latestOffset.getOrElse("None")} - $skipping",
          )
        }

        val shouldSkip =
          if (latestOffset.isEmpty) {
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
        case Uploading(commitState, _, uncommittedOffset, _, _) =>
          shouldSkipInternal(currentOffset, Option(largestOffset(commitState.committedOffset, uncommittedOffset)))
        case Writing(commitState, _, _, uncommittedOffset, _, _) =>
          shouldSkipInternal(currentOffset, Option(largestOffset(commitState.committedOffset, uncommittedOffset)))
      }
    }

  def hasPendingUpload: Boolean =
    writeState match {
      case _: Uploading => true
      case _ => false
    }

  def shouldRollover(schema: Schema): Boolean =
    rolloverOnSchemaChange &&
      schemaHasChanged(schema)

  protected[writer] def schemaHasChanged(schema: Schema): Boolean =
    writeState match {
      case w: Writing =>
        w.getCommitState.lastKnownSchema.exists { lastSchema =>
          lastSchema != schema && schemaChangeDetector.detectSchemaChange(lastSchema, schema)
        }
      case _ => false
    }

  protected[writer] def rolloverOnSchemaChange: Boolean =
    writeState match {
      case w: Writing => w.formatWriter.rolloverFileOnSchemaChange()
      case _ => false
    }

}
