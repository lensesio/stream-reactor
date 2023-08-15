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

import cats.effect.IO
import cats.effect.Ref
import cats.implicits.toBifunctorOps
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.PollResults
import io.lenses.streamreactor.connect.aws.s3.source.files.SourceFileQueue

import scala.util.Try

/**
  * Given a sourceBucketOptions, manages readers for all of the files
  */
case class ReaderManager(
  recordsLimit:    Int,
  fileSource:      SourceFileQueue,
  readerFn:        S3Location => Either[Throwable, ResultReader],
  connectorTaskId: ConnectorTaskId,
  readerRef:       Ref[IO, Option[ResultReader]],
) extends LazyLogging {

  def poll(): IO[Vector[PollResults]] = {
    def fromNexFile(pollResults: Vector[PollResults], allLimit: Int): IO[Vector[PollResults]] =
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
              reader <- IO.fromEither(readerFn(value))
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
    def acc(pollResults: Vector[PollResults], allLimit: Int): IO[Vector[PollResults]] =
      if (allLimit <= 0) IO.pure(pollResults)
      else {
        // if the reader is present, then read from it.
        // if there are no more records, then read the next file and build a new reader
        for {
          reader <- readerRef.get
          data <- reader match {
            case Some(value) =>
              val before = value.getLineNumber
              value.retrieveResults(allLimit) match {
                case Some(results) =>
                  val accumulated = acc(pollResults :+ results, allLimit - results.resultList.size)
                  val after       = value.getLineNumber
                  logger.debug("[{}] Read {} record(-s) from file {}",
                               connectorTaskId.show,
                               after - before,
                               value.getLocation.toString,
                  )
                  accumulated
                case None =>
                  logger.debug("[{}] Read 0 records from file {}", connectorTaskId.show, value.getLocation.toString)
                  fromNexFile(pollResults, allLimit)
              }

            case None => fromNexFile(pollResults, allLimit)
          }
        } yield data
      }
    acc(Vector.empty, recordsLimit)
  }

  private def closeAndLog(maybePrev: Option[ResultReader]): IO[Unit] = IO.delay {
    maybePrev.foreach { prev =>
      logger.info(s"[${connectorTaskId.show}] Read {} records from file {}",
                  prev.getLineNumber,
                  prev.getLocation.toString,
      )
      Try(prev.close())
    }
  }

  def close(): IO[Unit] =
    for {
      currentState <- readerRef.get
      _            <- closeAndLog(currentState)
    } yield ()

}
