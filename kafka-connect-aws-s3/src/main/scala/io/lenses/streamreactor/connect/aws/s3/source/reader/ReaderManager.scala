/*
 * Copyright 2017-2023 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.aws.s3.source.reader

import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.PollResults
import io.lenses.streamreactor.connect.aws.s3.source.config.SourceBucketOptions
import io.lenses.streamreactor.connect.aws.s3.source.files.S3SourceFileQueue
import io.lenses.streamreactor.connect.aws.s3.source.files.SourceFileQueue
import io.lenses.streamreactor.connect.aws.s3.storage.FileListError
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.ThrowableEither.toJavaThrowableConverter

/**
  * Given a sourceBucketOptions, manages readers for all of the files
  */
class ReaderManager(
  recordsLimit:   Int,
  startingOffset: Option[S3Location],
  fileSource:     SourceFileQueue,
  readerFn:       S3Location => Either[Throwable, ResultReader],
)(
  implicit
  connectorTaskId: ConnectorTaskId,
) extends LazyLogging
    with AutoCloseable {

  sealed trait ReaderState extends AutoCloseable with LazyLogging {

    def hasReader: Boolean

    override def close(): Unit = {}

    def toExceptionState(err: Throwable): ExceptionReaderState =
      ExceptionReaderState(err)

  }

  sealed trait MoreFilesAvailableState extends ReaderState {

    def readNextFile: ReaderState =
      fileSource.next() match {
        case Left(exception: FileListError) =>
          toExceptionState(exception.exception)
        case Right(Some(nextFile)) =>
          logger.debug(s"[${connectorTaskId.show}] readNextFile - Next file ($nextFile) found")
          readerFn(nextFile) match {
            case Right(reader)   => toInitialisedState(reader)
            case Left(exception) => toExceptionState(exception)
          }
        case Right(None) =>
          logger.debug(s"[${connectorTaskId.show}] readNextFile - No next file found")
          toNoFurtherFilesState()
      }

    def toInitialisedState(reader: ResultReader): ReaderState =
      InitialisedReaderState(reader)

    def toNoFurtherFilesState(): ReaderState = NoMoreFilesReaderState()

  }

  case class EmptyReaderState() extends MoreFilesAvailableState {
    logger.trace(s"[${connectorTaskId.show}] state: EMPTY")
    override def hasReader: Boolean = false
  }

  case class InitialisedReaderState(currentReader: ResultReader) extends ReaderState {
    def retrieveResults(limit: Int): Option[PollResults] = currentReader.retrieveResults(limit)

    logger.trace(s"[${connectorTaskId.show}] state: INITIALISED")
    override def close(): Unit = currentReader.close()

    override def hasReader: Boolean = true

    def toCompleteState(): ReaderState = {
      fileSource.markFileComplete(currentReader.getLocation).toThrowable
      currentReader.close()
      CompleteReaderState()
    }
  }

  case class CompleteReaderState() extends MoreFilesAvailableState {
    logger.trace(s"[${connectorTaskId.show}] state: COMPLETE")
    override def hasReader: Boolean = false
  }

  case class NoMoreFilesReaderState() extends MoreFilesAvailableState {
    logger.trace(s"[${connectorTaskId.show}] state: NO MORE FILES")
    override def hasReader: Boolean = false
  }

  case class ExceptionReaderState(exception: Throwable) extends ReaderState {
    logger.trace(s"[${connectorTaskId.show}] state: EXCEPTION")
    override def hasReader: Boolean = false
  }

  private var state: ReaderState = EmptyReaderState()
  startingOffset.foreach(fileSource.init)

  def poll(): Vector[PollResults] = {
    logger.debug(s"[${connectorTaskId.show}] start poll()")

    var allResults: Vector[PollResults] = Vector()

    var allLimit: Int = recordsLimit

    var moreFiles = true
    do {

      state match {
        case ExceptionReaderState(err) => throw err
        case fileState: MoreFilesAvailableState => state =
            fileState.readNextFile
        case InitialisedReaderState(_) =>
      }

      state match {
        case initState @ InitialisedReaderState(_) =>
          initState.retrieveResults(allLimit) match {
            case None => state = initState.toCompleteState()
            case Some(pollResults) =>
              allLimit -= pollResults.resultList.size
              allResults = allResults :+ pollResults
              if (pollResults.resultList.size < allLimit) {
                state = initState.toCompleteState()
              }
          }
        case _ =>
      }

      moreFiles = state match {
        case NoMoreFilesReaderState() => false
        case _                        => true
      }

    } while (allLimit > 0 && moreFiles)

    logger.debug(s"[${connectorTaskId.show}] exit poll()")
    allResults
  }

  override def close(): Unit = state.close()

}
object ReaderManager {

  def apply(
    root:  S3Location,
    bOpts: SourceBucketOptions,
  )(
    implicit
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface,
    contextOffsetFn:  S3Location => Option[S3Location],
  ) =
    new ReaderManager(
      bOpts.recordsLimit,
      contextOffsetFn(root),
      new S3SourceFileQueue(
        bOpts.createBatchListerFn(storageInterface),
        storageInterface.getBlobModified,
      ),
      new ReaderCreator(bOpts.format, bOpts.targetTopic, bOpts.getPartitionExtractorFn).create,
    )
}
