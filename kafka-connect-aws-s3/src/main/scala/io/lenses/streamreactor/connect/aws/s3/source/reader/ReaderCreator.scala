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
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.formats.reader.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.formats.reader.SourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.ThrowableEither._
import org.apache.kafka.common.errors.OffsetOutOfRangeException

import scala.util.Try

class ReaderCreator(
  format:      FormatSelection,
  targetTopic: String,
  partitionFn: String => Option[Int],
)(
  implicit
  connectorTaskId:  ConnectorTaskId,
  storageInterface: StorageInterface,
) extends LazyLogging {

  def create(pathWithLine: S3Location): Either[Throwable, ResultReader] =
    for {
      reader <- createInner(pathWithLine)
    } yield new ResultReader(reader, targetTopic, partitionFn)

  private def createInner(pathWithLine: S3Location): Either[Throwable, S3FormatStreamReader[_ <: SourceData]] =
    pathWithLine.pathOrError(
      fnAction = path => {
        Try {

          val inputStreamFn = () => storageInterface.getBlob(pathWithLine.bucket, path).toThrowable
          val fileSizeFn    = () => storageInterface.getBlobSize(pathWithLine.bucket, path).toThrowable
          logger.info(
            s"[${connectorTaskId.show}] Reading next file: ${pathWithLine.show} from line ${pathWithLine.line}",
          )

          val reader = S3FormatStreamReader(inputStreamFn, fileSizeFn, format, pathWithLine)

          (pathWithLine.line, pathWithLine.isFromStart) match {
            case (Some(lineFrom), false) => skipLinesToStartLine(reader, lineFrom)
            case _                       =>
          }

          reader
        }.toEither
      },
      fnErr = () => {
        new IllegalStateException("No path found")
      },
    )

  private def skipLinesToStartLine(reader: S3FormatStreamReader[_ <: SourceData], lineToStartOn: Int): Unit =
    for (_ <- 0 to lineToStartOn) {
      if (reader.hasNext) {
        reader.next()
      } else {
        throw new OffsetOutOfRangeException("Unknown file offset")
      }
    }

}
