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

import cats.implicits.toBifunctorOps
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.formats.reader.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.formats.reader.SourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.PollResults
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.IteratorOps

import scala.annotation.tailrec
import scala.util.Try

class ResultReader(
  reader:      S3FormatStreamReader[_ <: SourceData],
  targetTopic: String,
  partitionFn: String => Option[Int],
) extends LazyLogging
    with AutoCloseable {

  /**
    * Retrieves the results for a particular reader, or None if no further results are available
    */
  def retrieveResults(limit: Int): Option[PollResults] = {

    val results: Vector[_ <: SourceData] = retrieveResults(limit, reader, Vector.empty[SourceData])
    if (results.isEmpty) {
      logger.trace(s"No results found in reader ${reader.getBucketAndPath}")
      Option.empty[PollResults]
    } else {
      logger.trace(s"Results found in reader ${reader.getBucketAndPath}")
      Some(
        PollResults(
          results,
          reader.getBucketAndPath,
          targetTopic,
          partitionFn,
        ),
      )
    }

  }

  @tailrec
  private final def retrieveResults(
    limit:              Int,
    reader:             S3FormatStreamReader[_ <: SourceData],
    accumulatedResults: Vector[_ <: SourceData],
  ): Vector[_ <: SourceData] = {

    logger.trace(
      s"Calling retrieveResults with limit ($limit), reader (${reader.getBucketAndPath}/${reader.getLineNumber}), accumulatedResults size ${accumulatedResults.size}",
    )
    if (limit > 0 && reader.hasNext) {
      retrieveResults(limit - 1, reader, accumulatedResults :+ reader.next())
    } else {
      accumulatedResults
    }
  }

  override def close(): Unit = reader.close()

  def getLocation: S3Location = reader.getBucketAndPath

  def getLineNumber: Long = reader.getLineNumber
}

object ResultReader extends LazyLogging {

  def create(
    format:           FormatSelection,
    targetTopic:      String,
    partitionFn:      String => Option[Int],
    connectorTaskId:  ConnectorTaskId,
    storageInterface: StorageInterface,
  ): S3Location => Either[Throwable, ResultReader] = { pathWithLine =>
    for {
      path        <- pathWithLine.path.toRight(new IllegalStateException("No path found"))
      inputStream <- storageInterface.getBlob(pathWithLine.bucket, path).leftMap(_.toException)
      fileSize    <- storageInterface.getBlobSize(pathWithLine.bucket, path).leftMap(_.toException)
      _ <- Try(logger.info(
        s"[${connectorTaskId.show}] Reading next file: ${pathWithLine.show} from line ${pathWithLine.line}",
      )).toEither

      reader = S3FormatStreamReader(inputStream,
                                    fileSize,
                                    format,
                                    pathWithLine,
                                    () => storageInterface.getBlob(pathWithLine.bucket, path).leftMap(_.toException),
      )
      _ <- pathWithLine.line match {
        case Some(value) if value >= 0 => IteratorOps.skip(reader, value)
        case _                         => Right(())
      }
    } yield new ResultReader(reader, targetTopic, partitionFn)
  }
}
