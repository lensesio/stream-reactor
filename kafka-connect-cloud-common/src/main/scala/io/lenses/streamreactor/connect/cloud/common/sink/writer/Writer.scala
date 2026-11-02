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
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.ToAvroDataConverter
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CopyOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.DeleteOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.FileOperation
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.sink.seek.UploadOperation
import io.lenses.streamreactor.connect.cloud.common.storage._
import org.apache.kafka.connect.data.Schema

import java.io.File
import java.util.UUID
import scala.math.Ordered.orderingToOrdered
import scala.util.Failure
import scala.util.Success
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
  partitionKey:                Option[String]   = None,
  lastSeekedOffset:            Option[Offset]   = None,
  metrics:                     CloudSinkMetrics = new CloudSinkMetrics(),
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends LazyLogging {

  private var writeState: WriteState = NoWriter(CommitState(topicPartition, lastSeekedOffset))

  private[writer] def forceWriteState(state: WriteState): Unit = writeState = state
  private[writer] def currentWriteState: WriteState = writeState

  /** The sanitized PARTITIONBY key this writer is keyed on, if any. */
  private[writer] def partitionKeyOpt: Option[String] = partitionKey

  /**
   * Returns true if any cause in the throwable chain is an IOException.
   *
   * An IOException on the local write path (e.g. ENOSPC / disk full) leaves the underlying
   * file descriptor and format-writer container (Avro DataFileWriter, Parquet writer) in an
   * undefined state. Retrying a write on the same writer produces a corrupt file. We therefore
   * escalate IOException-caused failures to FatalCloudSinkError so the existing cleanUp path
   * deletes the staging file and the task restarts from the durable floor.
   *
   * Non-IOException throwables (e.g. schema/conversion errors) do NOT corrupt the stream
   * (they throw before any bytes reach the underlying OutputStream), so those keep the
   * existing NonFatalCloudSinkError behavior for NOOP/RETRY poison-record handling.
   */
  private def causedByIOException(t: Throwable): Boolean = {
    def loop(current: Throwable, visited: Set[Throwable]): Boolean =
      if (current == null || visited.contains(current)) false
      else if (current.isInstanceOf[java.io.IOException]) true
      else loop(current.getCause, visited + current)
    loop(t, Set.empty)
  }

  def write(messageDetail: MessageDetail): Either[SinkError, Unit] = {

    def innerMessageWrite(writingState: Writing): Either[SinkError, Unit] =
      writingState.formatWriter.write(messageDetail) match {
        case Left(err: Throwable) =>
          // Use Try to safely convert schema - prevents secondary errors from masking the original
          val schema    = messageDetail.value.schema().flatMap(s => Try(ToAvroDataConverter.convertSchema(s)).toOption)
          val keySchema = messageDetail.key.schema().flatMap(s => Try(ToAvroDataConverter.convertSchema(s)).toOption)
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
          if (causedByIOException(err))
            FatalCloudSinkError(err.getMessage, err.some, topicPartition).asLeft
          else
            NonFatalCloudSinkError(err.getMessage, err.some).asLeft
        case Right(_) =>
          writeState =
            writingState.update(messageDetail.offset, messageDetail.epochTimestamp, messageDetail.value.schema())
          metrics.incrementRecordsWrittenTotal()
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
          metrics.incrementFilesOpenedTotal()
          innerMessageWrite(writingState)
        }

      case _: Uploading =>
        // before we write we need to retry the upload. Unswallowable: under
        // errors.policy=NOOP/THROW this record would otherwise be silently dropped while the
        // writer is mid-commit, and the master offset later advances past the dropped offset
        // once the commit succeeds -- a genuine, if narrow, integrity hazard rather than a safe
        // no-op. RETRY still re-delivers, which is the desired outcome.
        NonFatalCloudSinkError.unswallowable("Attempting Write in Uploading State", None).asLeft

      case _: Staged =>
        // Batch mode: the writer's bytes are staged and waiting for the master-lock CAS. Accepting
        // a record now would put it in a file that has already been sized and keyed. Unswallowable,
        // not just NonFatal: under errors.policy=NOOP/THROW this record -- and the rest of
        // the same put() batch, since every writer on the TP is Staged together -- would otherwise
        // be silently dropped, and the batch commit later advances the floor past them. RETRY
        // re-delivers, and the next recommitPending drives the batch.
        NonFatalCloudSinkError.unswallowable("Attempting Write in Staged State", None).asLeft
    }
  }

  /**
   * Partition-batch commit, phase 1: make this writer's bytes durable at a connector-scoped temp
   * path and freeze the `(bucket, tempPath, tempETag, finalPath)` tuple the batch's
   * `CopyOperation` is built from. Nothing durable is recorded — the commit point is the master
   * lock CAS in `WriterCommitManager.commitBatch`.
   *
   * Idempotent: a writer already `Staged` is returned unchanged with no storage call, so a batch
   * retry after a partial staging failure only re-uploads the writers that actually failed.
   *
   * @param batchUuid identifies this batch attempt; part of the temp path so two attempts can
   *                  never collide on the same object.
   * @return `Right(None)` for an idle writer, `Right(Some(staged))` otherwise.
   */
  def stage(batchUuid: String): Either[SinkError, Option[Staged]] = {
    writeState match {
      case NoWriter(_) => return Option.empty[Staged].asRight
      case s:            Staged => return s.some.asRight
      case writingState: Writing =>
        writingState.formatWriter.complete() match {
          case Left(ex) =>
            // Same IOException escalation as `commit`: a half-written container cannot be retried.
            val escalated: SinkError = ex match {
              case _: FatalCloudSinkError => ex
              case _ if causedByIOException(ex.exception().orNull) =>
                FatalCloudSinkError(ex.message(), ex.exception(), topicPartition)
              case _ => ex
            }
            return escalated.asLeft
          case Right(_) =>
        }
        writeState = writingState.toUploading
      case _: Uploading =>
    }

    writeState match {
      case uploading: Uploading =>
        metrics.incrementInFlightUploads()
        val result =
          try {
            for {
              key <- objectKeyBuilder.build(
                uploading.firstBufferedOffset,
                uploading.uncommittedOffset,
                uploading.earliestRecordTimestamp,
                uploading.latestRecordTimestamp,
                uploading.recordCount,
              )
              finalPath <- key.path.toRight(NonFatalCloudSinkError("No path exists within cloud location"): SinkError)
              // Connector-scoped so the batch sweep can delete orphans belonging to this
              // connector without ever touching granular-mode temps or another connector's temps.
              tempPath = finalPath.prependedAll(
                s".temp-upload/${connectorTaskId.name}/${topicPartition.topic}/${topicPartition.partition}/$batchUuid/",
              )
              tempETag <- pendingOperationsProcessors.uploadOnly(
                topicPartition,
                UploadOperation(key.bucket, uploading.file, tempPath),
                partitionKey,
                Some(uploading.file),
              )
            } yield {
              // On a Left the state deliberately stays `Uploading` with the staging file intact.
              val staged = uploading.toStaged(key.bucket, tempPath, tempETag, finalPath)
              writeState = staged
              staged.some
            }
          } finally metrics.decrementInFlightUploads()
        if (result.isLeft) metrics.incrementFilesFailedTotal()
        result
      case other =>
        FatalCloudSinkError(s"stage reached an unexpected state $other", topicPartition).asLeft
    }
  }

  /**
   * Partition-batch commit, phase 2: the batch's copies have all completed, so this writer's bytes
   * are at their final path. Only valid on `Staged`.
   */
  def finalizeCommit(newOffset: Offset): Unit =
    writeState match {
      case staged: Staged =>
        val fileSize = staged.file.length()
        writeState = staged.toNoWriter(newOffset)
        Try(staged.file.delete()) match {
          case Success(_) =>
          case Failure(e) =>
            logger.warn(
              s"[${connectorTaskId.show}] Failed to delete temp file ${staged.file.getAbsolutePath} after a " +
                s"successful batch commit; continuing (the cloud commit already succeeded)",
              e,
            )
        }
        metrics.incrementFilesCommittedTotal()
        metrics.addRecordsCommittedTotal(staged.recordCount)
        metrics.addBytesWrittenTotal(fileSize)
        metrics.setLastCommitEpochMillis(System.currentTimeMillis())
      case other =>
        // Defensive: commitBatch only calls this for writers it staged in the same pass.
        logger.warn(
          s"[${connectorTaskId.show}] finalizeCommit($newOffset) called on a non-Staged writer for " +
            s"$topicPartition (state=$other); ignoring.",
        )
    }

  def commit: Either[SinkError, Unit] = {

    writeState match {
      case writingState: Writing =>
        writingState.formatWriter.complete() match {
          case Left(ex) =>
            val escalated: SinkError = ex match {
              case _: FatalCloudSinkError => ex
              case _ if causedByIOException(ex.exception().orNull) =>
                FatalCloudSinkError(ex.message(), ex.exception(), topicPartition)
              case _ => ex
            }
            return escalated.asLeft
          case Right(_) =>
        }
        writeState = writingState.toUploading
      case _: Uploading =>
      // your turn will come, nothing to do here because we're already in the correct state
      case NoWriter(_) =>
        // nothing to commit, get out of here
        return ().asRight
      case _: Staged =>
        // Guards against a routing bug that sends a batch-mode writer down the granular path.
        return FatalCloudSinkError("commit called on Staged writer", topicPartition).asLeft
    }

    val commitStartNanos = System.nanoTime()
    metrics.incrementInFlightUploads()
    try {
      writeState match {
        case uploadState @ Uploading(commitState,
                                     file,
                                     firstBufferedOffset,
                                     uncommittedOffset,
                                     earliestRecordTimestamp,
                                     latestRecordTimestamp,
                                     recordCount,
            ) =>
          val fileSize = file.length()
          val fnIndexUpdate: (
            TopicPartition,
            Option[Offset],
            Option[PendingState],
          ) => Either[SinkError, Option[Offset]] =
            partitionKey match {
              case Some(pk) => (tp, co, ps) => indexManager.updateForPartitionKey(tp, pk, co, ps)
              case None     => (tp, co, ps) => indexManager.update(tp, co, ps)
            }
          val result = for {
            key <- objectKeyBuilder.build(firstBufferedOffset,
                                          uncommittedOffset,
                                          earliestRecordTimestamp,
                                          latestRecordTimestamp,
                                          recordCount,
            )
            path <- key.path.toRight(NonFatalCloudSinkError("No path exists within cloud location"))
            pendingOperations =
              if (indexManager.indexingEnabled) {
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
                  UploadOperation(key.bucket, file, path),
                )

            newOffset <- pendingOperationsProcessors.processPendingOperations(
              topicPartition,
              getCommittedOffset,
              PendingState(uncommittedOffset, pendingOperations),
              fnIndexUpdate,
              escalateOnCancel = true,
              partitionKey     = partitionKey,
              stagingFile      = Some(file),
            )
            _ = {
              logger.debug(s"[{}] Writer.resetState: Resetting state $writeState", connectorTaskId.show)
              writeState = uploadState.toNoWriter(newOffset)
              // The cloud commit has already succeeded at this point. A failure to
              // delete the local temp file is a hygiene issue (disk leak) -- not a
              // correctness issue -- so we must not propagate it as a FatalCloudSinkError.
              // Doing so would fail the task AFTER data was durably written, which risks
              // re-running the upload on restart and violating at-most-once for the
              // same record offsets.
              Try(file.delete()) match {
                case Success(_) =>
                case Failure(e) =>
                  logger.warn(
                    s"[${connectorTaskId.show}] Failed to delete temp file ${file.getAbsolutePath} after successful commit; " +
                      s"continuing (the cloud commit already succeeded)",
                    e,
                  )
              }
              logger.debug(s"[{}] Writer.resetState: New state $writeState", connectorTaskId.show)
            }
          } yield ()
          val elapsed = (System.nanoTime() - commitStartNanos) / 1_000_000L
          result match {
            case Right(_) =>
              metrics.recordCommitTimer(elapsed)
              metrics.incrementFilesCommittedTotal()
              metrics.addBytesWrittenTotal(fileSize)
              metrics.addRecordsCommittedTotal(recordCount)
              metrics.setLastCommitEpochMillis(System.currentTimeMillis())
            case Left(_) =>
              metrics.incrementFilesFailedTotal()
          }
          result
        case other =>
          FatalCloudSinkError(s"Other $other error detected, abort", topicPartition).asLeft
      }
    } finally {
      metrics.decrementInFlightUploads()
    }
  }

  def close(): Unit =
    writeState = writeState match {
      case state @ NoWriter(_) => state
      case Writing(commitState, formatWriter, file, _, _, _, _) =>
        Try(formatWriter.close())
        Try(file.delete())
        NoWriter(commitState.reset())
      case Uploading(commitState, file, _, _, _, _, _) =>
        Try(file.delete())
        NoWriter(commitState.reset())
      case staged: Staged =>
        // The temp object is deliberately left behind for the batch orphan sweep; deleting it here
        // could race a batch whose PendingState still references it.
        Try(staged.file.delete())
        NoWriter(staged.commitState.reset())
    }

  def isIdle: Boolean =
    writeState match {
      case NoWriter(_) => true
      case _           => false
    }

  def getCommittedOffset: Option[Offset] = writeState.getCommitState.committedOffset

  def getFirstBufferedOffset: Option[Offset] =
    writeState match {
      case _: NoWriter  => None
      case w: Writing   => Some(w.firstBufferedOffset)
      case u: Uploading => Some(u.firstBufferedOffset)
      case s: Staged    => Some(s.firstBufferedOffset)
    }

  def shouldFlush: Boolean = writeState match {
    case Writing(commitState, _, file, _, uncommittedOffset, _, _) => commitPolicy.shouldFlush(
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
    case _: Staged    => false
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
        case Uploading(commitState, _, _, uncommittedOffset, _, _, _) =>
          shouldSkipInternal(currentOffset, Option(largestOffset(commitState.committedOffset, uncommittedOffset)))
        case Writing(commitState, _, _, _, uncommittedOffset, _, _) =>
          shouldSkipInternal(currentOffset, Option(largestOffset(commitState.committedOffset, uncommittedOffset)))
        case s: Staged =>
          // Unreachable in practice: `shouldSkip` is only ever called from the granular
          // `WriterManager.write` branch, and `Staged` is only ever produced by `stage()` on the
          // batch-mode path (which uses the topic-partition floor in `batchDedupFloor` instead of
          // `shouldSkip`). Kept for match exhaustiveness; mirrors the `Uploading` arm above should
          // a future refactor ever make it reachable.
          shouldSkipInternal(currentOffset, Option(largestOffset(s.commitState.committedOffset, s.uncommittedOffset)))
      }
    }

  def hasPendingUpload: Boolean =
    writeState match {
      case _: Uploading => true
      case _: Staged    => true
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
