
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

package io.lenses.streamreactor.connect.aws.s3.sink

import cats.implicits.catsSyntaxEitherId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatWriter
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.processing.BlockingQueueProcessor
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.apache.kafka.connect.data.Schema

import scala.Option.empty
import scala.math.Ordered.orderingToOrdered

class S3Writer(sinkName: String,
               bucketAndPrefix: RemoteS3RootLocation,
               commitPolicy: CommitPolicy,
               formatWriterFn: (TopicPartitionOffset, Map[PartitionField, String], Offset => () => Unit) => Either[ProcessorException, S3FormatWriter],
               fileNamingStrategy: S3FileNamingStrategy,
               partitionValues: Map[PartitionField, String],
               processor: BlockingQueueProcessor
              )(implicit storageInterface: StorageInterface) extends LazyLogging {

  private var internalState: Option[S3WriterState] = None

  def createFormatWriterIfEmpty(
                                 currentState: Option[S3WriterState],
                                 topicPartitionOffset: TopicPartitionOffset,
                                 partitionValues: Map[PartitionField, String]
                               ): Either[ProcessorException, Unit] = {


    val newFormatWriterRequired = currentState.fold(true)(_.formatWriter.isEmpty)
    if (newFormatWriterRequired) {
      for {
        formatWriter <- formatWriterFn(topicPartitionOffset, partitionValues, updateCommittedOffsetFn)
      } yield {
        internalState = Some(
          currentState match {
            case Some(is) => is.withFormatWriter(formatWriter)
            case None => S3WriterState(topicPartitionOffset, formatWriter)
          }
        )
      }
    } else {
      ().asRight
    }
  }

  def enqueueWrite(messageDetail: MessageDetail, tpo: TopicPartitionOffset): Either[ProcessorException, Unit] = {

    createFormatWriterIfEmpty(internalState, tpo, partitionValues) match {
      case Left(exception) => return exception.asLeft
      case Right(_) =>
    }

    internalState.fold {
      logger.error(s"[{}] S3Writer.write: Internal state: None", sinkName)
      return ProcessorException("No internal state").asLeft
    } {
      is =>
        logger.debug(s"[{}] S3Writer.write: Internal state: {}", sinkName, is.show())
        is.getFormatWriter match {
          case Left(error) => return error.asLeft
          case Right(formatWriter: S3FormatWriter) =>
            formatWriter.write(messageDetail.keySinkData, messageDetail.valueSinkData, tpo.topic).asRight
        }
    }

    internalState = internalState.map(
      _.offsetChange(tpo.offset, messageDetail.valueSinkData.schema())
    )

    ().asRight
  }

  def shouldRollover(schema: Schema): Boolean = {
    internalState.nonEmpty &&
      rolloverOnSchemaChange &&
      schemaHasChanged(schema)
  }

  private def schemaHasChanged(schema: Schema): Boolean = {
    internalState.exists(_.lastKnownSchema.exists(_ != schema))
  }

  private def rolloverOnSchemaChange: Boolean = {
    internalState.exists(_.formatWriter.exists(_.rolloverFileOnSchemaChange()))
  }

  /**
    * Provided as a callback to the ProcessorOperation subclasses
    *
    * @param offset current offset
    * @return
    */
  def updateCommittedOffsetFn(offset: Offset): () => Unit = {
    () => {
      logger.trace("Updating committed offset from {} to {}", getCommittedOffset, offset)
      internalState = internalState.map(_.withCommittedOffset(offset))
      ()
    }
  }

  def enqueueCommit: Either[CommitException, Unit] = {

    internalState.fold {
      return CommitException("Invalid internal state").asLeft
    } { is =>

      val topicPartitionOffset = is.topicPartitionOffset

      // it is ok for there not to be a formatWriter attached here.
      for {
        formatWriter <- is.formatWriter
      } for {
        finalFileName <- fileNamingStrategy.finalFilename(
          bucketAndPrefix,
          topicPartitionOffset,
          partitionValues
        )
      } yield {
        formatWriter.close(finalFileName, is.offset,
          updateCommittedOffsetFn(is.offset))
      }

      resetState()

      ().asRight
    }

  }

  private def resetState(): Unit = {
    logger.debug(s"[{}] S3Writer.resetState: Resetting state $internalState", sinkName)

    internalState = internalState.map {
      is =>
        val updatedState = is.reset()
        logger.debug(s"[{}] S3Writer.resetState: New internal state: $updatedState", sinkName)
        updatedState
    }
  }

  def close(): Unit = {
    internalState.foreach(_.formatWriter.foreach(_.close()))
    storageInterface.close()
  }

  def getCommittedOffset: Option[Offset] = internalState.fold(empty[Offset])(_.committedOffset)

  def shouldFlush: Boolean = {
    internalState.exists {
      is =>
        commitPolicy.shouldFlush(
          CommitContext(
            is.topicPartitionOffset,
            is.recordCount,
            is.lastKnownFileSize,
            is.createdTimestamp,
            is.lastFlushedQueuedTimestamp,
          )
        )
    }
  }

  /**
    * If the offsets provided by Kafka Connect have already been processed, then they must be skipped to avoid duplicate records and protect the integrity of the data files.
    *
    * @param currentOffset the current offset
    * @return true if the given offset should be skipped, false otherwise
    */
  def shouldSkip(currentOffset: Offset): Boolean = {

    def logSkipOutcome(currentOffset: Offset, latestOffset: Option[Offset], skipRecord: Boolean): Unit = {
      val skipping = if (skipRecord) "SKIPPING" else "PROCESSING"
      val latest: String = latestOffset.fold("(No State)")(_.value.toString)
      logger.info(s"[$sinkName] (current:latest) ${currentOffset.value.toString}:$latest ∴ $skipping")
    }

    val latestOffset = offset
    val alwaysProcessWhen = latestOffset.isEmpty
    if (alwaysProcessWhen) {
      logSkipOutcome(currentOffset, latestOffset, skipRecord = false)
      return false
    }
    val neverProcessWhen = latestOffset.nonEmpty && latestOffset.exists(_ >= currentOffset)
    if (neverProcessWhen) {
      logSkipOutcome(currentOffset, latestOffset, skipRecord = true)
      return true
    }
    logSkipOutcome(currentOffset, latestOffset, skipRecord = false)
    false
  }

  private def offset = internalState.map(_.offset)

  /**
    * Delegates to the processor's process method
    *
    * @return
    */
  def process(): Either[ProcessorException, Unit] = processor.process()

}