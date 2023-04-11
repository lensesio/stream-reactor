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

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.config.FormatSelection
import io.lenses.streamreactor.connect.aws.s3.formats.S3FormatStreamReader
import io.lenses.streamreactor.connect.aws.s3.model.SourceData
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocationWithLine
import io.lenses.streamreactor.connect.aws.s3.sink.ThrowableEither._
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface
import org.apache.kafka.common.errors.OffsetOutOfRangeException

import scala.util.Try

class ReaderCreator(
  sourceName:  String,
  format:      FormatSelection,
  targetTopic: String,
)(
  implicit
  storageInterface: StorageInterface,
  partitionFn:      String => Option[Int],
) extends LazyLogging {

  def create(pathWithLine: RemoteS3PathLocationWithLine): Either[Throwable, ResultReader] =
    for {
      reader <- Try(createInner(pathWithLine)).toEither
    } yield new ResultReader(reader, targetTopic)

  private def createInner(pathWithLine: RemoteS3PathLocationWithLine): S3FormatStreamReader[_ <: SourceData] = {
    val file          = pathWithLine.file
    val inputStreamFn = () => storageInterface.getBlob(file).toThrowable(sourceName)
    val fileSizeFn    = () => storageInterface.getBlobSize(file).toThrowable(sourceName)
    logger.info(s"[$sourceName] Reading next file: ${pathWithLine.file} from line ${pathWithLine.line}")

    val reader = S3FormatStreamReader(inputStreamFn, fileSizeFn, format, file)

    if (!pathWithLine.isFromStart) {
      skipLinesToStartLine(reader, pathWithLine.line)
    }

    reader
  }

  private def skipLinesToStartLine(reader: S3FormatStreamReader[_ <: SourceData], lineToStartOn: Int) =
    for (_ <- 0 to lineToStartOn) {
      if (reader.hasNext) {
        reader.next()
      } else {
        throw new OffsetOutOfRangeException("Unknown file offset")
      }
    }

}
