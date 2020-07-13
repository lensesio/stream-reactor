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
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.model._
import io.lenses.streamreactor.connect.aws.s3.source.config.SourceBucketOptions
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.apache.kafka.common.errors.OffsetOutOfRangeException

case class State(
                  initFile: Option[S3StoredFile],
                  initLine: Option[Int],
                  file: Option[S3StoredFile],
                  currentReader: Option[_ <: S3FormatStreamReader[_ <: SourceData]],
                  terminated: Boolean = false
                )

/**
  * Given a sourceBucketOptions, manages readers for all of the files
  */
class S3BucketReaderManager(
                             sourceBucketOptions: SourceBucketOptions,
                             offsetReaderResultFn: (String, String) => Option[OffsetReaderResult]
                           )
                           (
                             implicit storageInterface: StorageInterface,
                             sourceLister: S3SourceLister
                           ) extends LazyLogging {

  private val prefix = sourceBucketOptions.fileNamingStrategy.prefix(sourceBucketOptions.sourceBucketAndPrefix)
  private val resultReader = new ResultReader(prefix, sourceBucketOptions.targetTopic)

  private var state: State = setupInitialState()

  def close(): Unit = state.currentReader.foreach(_.close)

  def poll(): Vector[PollResults] = {
    logger.debug(s"Fresh poll call received")

    state = state.copy(terminated = false)
    val limit = sourceBucketOptions.limit

    var allResults: Vector[PollResults] = Vector()

    var n = 0
    var allLimit: Int = limit
    do {
      chooseReader() match {
        case Some(currentReader) =>
          val newResults = resultReader.retrieveResults(currentReader, allLimit)
          state = if (newResults.isEmpty || (newResults.nonEmpty && newResults.get.resultList.size < allLimit)) {
            state.copy(currentReader = None)
          } else {
            state.copy(terminated = true)
          }
          allLimit -= newResults.fold(0)(_.resultList.size)
          allResults = allResults ++ newResults
        case None => state = state.copy(initLine = None, initFile = None, currentReader = None, terminated = true)
      }
      n += 1
      logger.debug(s"Moving onto next iteration of loop, $n, state not terminated yet")
    } while (!state.terminated)

    logger.debug(s"State terminated")
    allResults
  }

  private def chooseReader(): Option[S3FormatStreamReader[_ <: SourceData]] = {
    state.currentReader.fold(readNextFile()) { currentReader => Some(currentReader) }
  }

  private def readNextFile(): Option[S3FormatStreamReader[_ <: SourceData]] = {

    logger.debug("Reading next file")
    sourceLister
      .next(
        sourceBucketOptions.fileNamingStrategy,
        sourceBucketOptions.sourceBucketAndPrefix,
        state.file,
        state.initFile
      ).fold({
      logger.debug("No next file found")
      Option.empty[S3FormatStreamReader[_ <: SourceData]]
    }
    ) {
      value =>
        val reader = setUpReader(value)
        state = state.copy(file = Some(value), initLine = None, initFile = None, currentReader = reader)
        reader
    }
  }

  private def setUpReader(s3StoredFile: S3StoredFile): Option[S3FormatStreamReader[_ <: SourceData]] = {
    val bucketAndPath = BucketAndPath(sourceBucketOptions.sourceBucketAndPrefix.bucket, s3StoredFile.path)
    val inputStreamFn = () => storageInterface.getBlob(bucketAndPath)
    val fileSizeFn = () => storageInterface.getBlobSize(bucketAndPath)
    logger.info(s"Reading next file: $s3StoredFile")

    val reader = S3FormatStreamReader(inputStreamFn, fileSizeFn, sourceBucketOptions, bucketAndPath)

    skipLinesToStartLine(reader)

    Some(reader)
  }

  private def skipLinesToStartLine(reader: S3FormatStreamReader[_ <: SourceData]) = {
    state.initLine.map(
      line =>
        for (_ <- 1 to line) {
          if (reader.hasNext) {
            reader.next()
          } else {
            throw new OffsetOutOfRangeException("Unknown file offset")
          }
        }
    )
  }


  private def setupInitialState(): State = {
    val startingPoint: Option[OffsetReaderResult] = offsetReaderResultFn(
      sourceBucketOptions.sourceBucketAndPrefix.bucket,
      sourceBucketOptions.fileNamingStrategy.prefix(sourceBucketOptions.sourceBucketAndPrefix)
    )

    val s3StoredFile: Option[S3StoredFile] = startingPoint.fold(Option.empty[S3StoredFile])(offsetReaderResult =>
      S3StoredFile(offsetReaderResult.path)(sourceBucketOptions.fileNamingStrategy)
    )

    val offsetLine: Option[Int] = startingPoint.fold(Option.empty[Int])(offsetReaderResult => Some(offsetReaderResult.line.toInt))

    State(
      initFile = s3StoredFile,
      initLine = offsetLine,
      file = None,
      currentReader = None
    )
  }

}
