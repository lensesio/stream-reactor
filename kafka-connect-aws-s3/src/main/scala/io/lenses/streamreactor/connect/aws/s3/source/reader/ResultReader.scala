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
import io.lenses.streamreactor.connect.aws.s3.config.StreamReaderInput
import io.lenses.streamreactor.connect.aws.s3.formats.reader.S3StreamReader
import io.lenses.streamreactor.connect.aws.s3.model.Topic
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.source.SourceWatermark
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.IteratorOps
import org.apache.kafka.connect.source.SourceRecord

import scala.annotation.tailrec
import scala.util.Try

class ResultReader(
                    reader: S3StreamReader,
) extends LazyLogging
    with AutoCloseable {

  /**
    * Retrieves the results for a particular reader, or None if no further results are available
    */
  def retrieveResults(limit: Int): Option[Vector[SourceRecord]] = {

    val results: Vector[SourceRecord] = accumulate(limit, reader, Vector.empty[SourceRecord])

    if (results.isEmpty) {
      None
    } else {
      Some(
        results,
      )
    }

  }

  @tailrec
  private final def accumulate(
                                limit:              Int,
                                reader:             S3StreamReader,
                                accumulatedResults: Vector[SourceRecord],
  ): Vector[SourceRecord] =
    if (limit > 0 && reader.hasNext) {
      accumulate(limit - 1, reader, accumulatedResults :+ reader.next())
    } else {
      accumulatedResults
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
    hasEnvelope:      Boolean,
  ): S3Location => Either[Throwable, ResultReader] = { pathWithLine =>
    for {
      path        <- pathWithLine.path.toRight(new IllegalStateException("No path found"))
      inputStream <- storageInterface.getBlob(pathWithLine.bucket, path).leftMap(_.toException)
      metadata    <- storageInterface.getMetadata(pathWithLine.bucket, path).leftMap(_.toException)
      _ <- Try(logger.info(
        s"[${connectorTaskId.show}] Reading next file: ${pathWithLine.show} from line ${pathWithLine.line}",
      )).toEither

      path <- pathWithLine.path.toRight(
        new IllegalStateException(s"Invalid state reached. Missing path for S3 location:${pathWithLine.show}}"),
      )

      partition = partitionFn(path).map(Int.box).orNull
      reader = format.toStreamReader(
        StreamReaderInput(
          inputStream,
          pathWithLine,
          metadata,
          hasEnvelope,
          () => storageInterface.getBlob(pathWithLine.bucket, path).leftMap(_.toException),
          partition,
          Topic(targetTopic),
          SourceWatermark.partition(pathWithLine),
        ),
      )
      _ <- pathWithLine.line match {
        case Some(value) if value >= 0 => IteratorOps.skip(reader, value)
        case _                         => Right(())
      }
    } yield new ResultReader(reader)
  }
}
