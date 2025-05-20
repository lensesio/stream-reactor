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
package io.lenses.streamreactor.connect.cloud.common.source.reader

import cats.effect.IO
import cats.effect.Ref
import cats.implicits.toBifunctorOps
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.source.CommitWatermark
import io.lenses.streamreactor.connect.cloud.common.source.config.DirectoryCache
import io.lenses.streamreactor.connect.cloud.common.source.config.PostProcessAction
import io.lenses.streamreactor.connect.cloud.common.source.files.SourceFileQueue
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.apache.kafka.connect.source.SourceRecord

import scala.util.Try

/**
  * Given a sourceBucketOptions, manages readers for all of the files
  */
class ReaderManager(
  val root:               CloudLocation,
  val path:               CloudLocation,
  recordsLimit:           Int,
  fileSource:             SourceFileQueue,
  readerBuilderF:         CloudLocation => Either[Throwable, ResultReader],
  connectorTaskId:        ConnectorTaskId,
  readerRef:              Ref[IO, Option[ResultReader]],
  storageInterface:       StorageInterface[_],
  maybePostProcessAction: Option[PostProcessAction],
) extends LazyLogging {

  val directoryCache = new DirectoryCache(storageInterface)

  def poll(): IO[Vector[SourceRecord]] = {
    def fromNexFile(pollResults: Vector[SourceRecord], allLimit: Int): IO[Vector[SourceRecord]] =
      for {
        maybePrev <- readerRef.getAndSet(None)
        _ <-
          closeAndLog(maybePrev)

        nextFile <- IO.fromEither(fileSource.next().leftMap(_.exception))
        results <- nextFile.fold(IO(pollResults)) {
          value =>
            for {
              _ <- IO.delay(
                logger.debug(s"[${connectorTaskId.show}] Start reading from ${value.toString}"),
              )
              reader <- IO.fromEither(readerBuilderF(value))
              _      <- readerRef.set(Some(reader))
              r      <- acc(pollResults, allLimit)
            } yield r
        }
      } yield results

    //re-implement poll() to use the IO effect and avoid state
    //  SourceFileQueue  provides the list of files to read from. For each file received a ResultReader is built.
    //  Since it reads recordsLimit records at a time, the last built reader is kept until it returns no more records.
    //  Once it returns no more records, the next file is read and a new reader is built.
    //  This is repeated until the recordsLimit is reached or there are no more files to read from.
    //  The results are accumulated in a Vector[PollResults] and returned.
    def acc(pollResults: Vector[SourceRecord], allLimit: Int): IO[Vector[SourceRecord]] =
      if (allLimit <= 0) IO.pure(pollResults)
      else {
        // if the reader is present, then read from it.
        // if there are no more records, then read the next file and build a new reader
        for {
          reader <- readerRef.get
          data <- reader match {
            case Some(value) =>
              val before: Long = transposeRecordIdxForLog(value.currentRecordIndex)
              value.retrieveResults(allLimit) match {
                case Some(results) =>
                  val accumulated = acc(pollResults ++ results, allLimit - results.size)
                  val after: Long = transposeRecordIdxForLog(value.currentRecordIndex)
                  logger.info("[{}] Read {} record(-s) from file {}",
                              connectorTaskId.show,
                              after - before,
                              value.source.toString,
                  )
                  accumulated
                case None =>
                  logger.info("[{}] Read 0 records from file {}", connectorTaskId.show, value.source.toString)
                  fromNexFile(pollResults, allLimit)
              }

            case None => fromNexFile(pollResults, allLimit)
          }
        } yield data
      }
    acc(Vector.empty, recordsLimit)
  }

  /**
    * The index of -1 means no record. It's an unfortunate state introduced by the readers keeping track of the current
    * records which is kept for backwards compatibility. If -1 then there are 0 records, and it adds 1 to the index to
    * get the number of records read.
    * @param currentRecordIndex the unadjusted index
    * @return the adjusted index (for logging)
    */
  private def transposeRecordIdxForLog(currentRecordIndex: Long): Long =
    if (currentRecordIndex == -1) 0 else currentRecordIndex + 1

  private def closeAndLog(maybePrev: Option[ResultReader]): IO[Unit] = IO.delay {
    maybePrev.foreach { prev =>
      val transposedIdx = transposeRecordIdxForLog(prev.currentRecordIndex)
      logger.info(s"[${connectorTaskId.show}] Read complete - {} records from file {}",
                  transposedIdx,
                  prev.source.toString,
      )
      Try(prev.close())
    }
  }

  def close(): IO[Unit] =
    for {
      currentState <- readerRef.get
      _            <- closeAndLog(currentState)
    } yield ()

  def postProcess(commitWatermark: CommitWatermark): IO[Unit] =
    maybePostProcessAction match {
      case Some(action) =>
        logger.info("PostProcess for {}", commitWatermark)
        action.run(storageInterface, directoryCache, commitWatermark.cloudLocation)
      case None =>
        logger.info("No PostProcess for {}", commitWatermark)
        IO.unit
    }

}
