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
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import io.lenses.streamreactor.connect.aws.s3.utils.IteratorOps

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
    createInner(pathWithLine).map(new ResultReader(_, targetTopic, partitionFn))

  private def createInner(pathWithLine: S3Location): Either[Throwable, S3FormatStreamReader[_ <: SourceData]] =
    pathWithLine.pathOrError(
      fnAction = path => {
        for {
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
        } yield reader
      },
      fnErr = () => {
        new IllegalStateException("No path found")
      },
    )

}
