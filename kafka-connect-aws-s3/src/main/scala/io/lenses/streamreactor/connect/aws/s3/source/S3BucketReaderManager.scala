/*
 * Copyright 2020 Lenses.io
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

package io.lenses.streamreactor.connect.aws.s3.source

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.{Format, FormatSelection}
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocationWithLine
import io.lenses.streamreactor.connect.aws.s3.source.config.SourceBucketOptions
import io.lenses.streamreactor.connect.aws.s3.storage.{ListFilesStorageInterface, StorageInterface}
import org.apache.kafka.common.errors.OffsetOutOfRangeException

sealed trait ReaderState extends AutoCloseable {

  def hasReader: Boolean

  override def close(): Unit = {}
}

case class EmptyReaderState() extends ReaderState {
  override def hasReader: Boolean = false
}

case class InitialisedReaderState(
                                   currentReader: S3FormatStreamReader[_ <: SourceData],
                                 ) extends ReaderState {

  override def close(): Unit = currentReader.close()

  override def hasReader: Boolean = true
}

case class CompleteReaderState() extends ReaderState {
  override def hasReader: Boolean = false
}

case class NoMoreFilesReaderState() extends ReaderState {
  override def hasReader: Boolean = false
}

case class ExceptionReaderState(exception: Throwable) extends ReaderState {
  override def hasReader: Boolean = false
}

/**
  * Given a sourceBucketOptions, manages readers for all of the files
  */
class S3BucketReaderManager(
                             recordsLimit: Int,
                             format: FormatSelection,
                             startingOffset: Option[RemoteS3PathLocationWithLine],
                             fileQueueProcessor: SourceFileQueue,
                             resultReader: ResultReader
                           )(
                             implicit storageInterface: StorageInterface,
                             listFilesStorageInterface: ListFilesStorageInterface,
                           ) extends LazyLogging with AutoCloseable {

  private var state: ReaderState = EmptyReaderState()
  startingOffset.foreach(fileQueueProcessor.init)

  def poll(): Vector[PollResults] = {
    logger.debug(s"Fresh poll call received")

    var allResults: Vector[PollResults] = Vector()

    var allLimit: Int = recordsLimit

    var moreFiles = true
    do {

      // set up a reader
      state match {
        case ExceptionReaderState(err) => throw err
        case EmptyReaderState() | CompleteReaderState() | NoMoreFilesReaderState() =>
          state = refreshStateForNextReader()
        case InitialisedReaderState(_) =>
      }

      state match {
        case InitialisedReaderState(currentReader) =>
          resultReader.retrieveResults(currentReader, allLimit) match {
            case None => state = toReaderCompleteState(currentReader)
            case Some(pollResults) => {
              allLimit -= pollResults.resultList.size
              allResults = allResults :+ pollResults
              if (pollResults.resultList.size < allLimit) {
                state = toReaderCompleteState(currentReader)
              }
            }
          }
        case _ =>
      }

      moreFiles = state match {
        case NoMoreFilesReaderState() => false
        case _ => true
      }

    } while (allLimit > 0 && moreFiles)

    logger.debug(s"State terminated")
    allResults
  }

  private def toReaderCompleteState(currentReader: S3FormatStreamReader[_ <: SourceData]): ReaderState = {
    fileQueueProcessor.markFileComplete(currentReader.getBucketAndPath)
    currentReader.close()
    CompleteReaderState()
  }

  private def refreshStateForNextReader(): ReaderState = {

    fileQueueProcessor.next() match {
      case Left(exception: Throwable) =>
        ExceptionReaderState(exception)
      case Right(Some(nextFile)) =>
        logger.debug(s"refreshStateForNextReader - Next file (${nextFile}) found")
        setUpReader(nextFile) match {
          case Some(value) => InitialisedReaderState(value)
          case None => ExceptionReaderState(new IllegalStateException(s"Cannot load requested next file $nextFile"))
        }
      case Right(None) =>
        logger.debug("refreshStateForNextReader - No next file found")
        NoMoreFilesReaderState()
    }
  }

  private def setUpReader(bucketAndPath: RemoteS3PathLocationWithLine): Option[S3FormatStreamReader[_ <: SourceData]] = {
    val file = bucketAndPath.file
    val inputStreamFn = () => storageInterface.getBlob(file)
    val fileSizeFn = () => storageInterface.getBlobSize(file)
    logger.info(s"Reading next file: ${bucketAndPath.file} from line ${bucketAndPath.line}")

    val reader = S3FormatStreamReader(inputStreamFn, fileSizeFn, format, file)

    if (!bucketAndPath.isFromStart) {
      skipLinesToStartLine(reader, bucketAndPath.line)
    }

    Some(reader)
  }

  private def skipLinesToStartLine(reader: S3FormatStreamReader[_ <: SourceData], lineToStartOn: Int) = {

    for (_ <- 0 to lineToStartOn) {
      if (reader.hasNext) {
        reader.next()
      } else {
        throw new OffsetOutOfRangeException("Unknown file offset")
      }
    }
  }


  override def close(): Unit = state.close()

}
