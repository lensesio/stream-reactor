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
        _         <- IO.delay(maybePrev.foreach(r => Try(r.close())))
        nextFile  <- IO.fromEither(fileSource.next().leftMap(_.exception))
        results <- nextFile.fold(IO(pollResults)) {
          value =>
            for {
              _ <- IO.delay(
                logger.debug(s"[${connectorTaskId.show}] readNextFile - Next file ($nextFile) found"),
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
        readerRef.get.flatMap {
          case Some(reader) =>
            reader.retrieveResults(allLimit) match {
              case Some(results) =>
                acc(pollResults :+ results, allLimit - results.resultList.size)
              case None =>
                fromNexFile(pollResults, allLimit)
            }

          case None => fromNexFile(pollResults, allLimit)
        }
      }
    acc(Vector.empty, recordsLimit)
  }

  def close(): IO[Unit] =
    for {
      currentState <- readerRef.get
      _ <- currentState match {
        case Some(reader) =>
          IO.delay(Try(reader.close()))
        case None =>
          IO.unit
      }
    } yield ()

}
