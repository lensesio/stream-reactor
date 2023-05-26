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
package io.lenses.streamreactor.connect.aws.s3.source.files

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.S3Location
import io.lenses.streamreactor.connect.aws.s3.storage.FileListError
import io.lenses.streamreactor.connect.aws.s3.storage.FileLoadError
import io.lenses.streamreactor.connect.aws.s3.storage.FileMetadata
import io.lenses.streamreactor.connect.aws.s3.storage.ListResponse

import java.time.Instant
import scala.collection.mutable.ListBuffer

trait SourceFileQueue {

  def init(initFile: S3Location): Unit

  def next(): Either[FileListError, Option[S3Location]]

  def markFileComplete(fileWithLine: S3Location): Either[String, Unit]

}

/**
  * Blocking processor for queues of operations.  Used to ensure consistency.
  * Will block any further writes by the current file until the remote has caught up.
  */
class S3SourceFileQueue(
  batchListerFn:     Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]],
  getLastModifiedFn: (String, String) => Either[FileLoadError, Instant],
) extends SourceFileQueue
    with LazyLogging {

  private var lastSeenFile: Option[FileMetadata] = None

  private var files = ListBuffer.empty[S3Location]

  override def next(): Either[FileListError, Option[S3Location]] =
    files.headOption.fold(retrieveNextFile())(Some(_).asRight)

  private def retrieveNextFile(
  ): Either[FileListError, Option[S3Location]] = {
    val nextBatch: Either[FileListError, Option[ListResponse[String]]] = batchListerFn(lastSeenFile)
    nextBatch.flatMap {
      case Some(ListResponse(bucket, prefix, value, meta)) =>
        lastSeenFile = meta.some
        files ++= value.map(path =>
          S3Location(
            bucket,
            prefix,
            path.some,
          ).fromStart().withTimestamp(meta.lastModified),
        )
        files.headOption.asRight
      case _ => Option.empty.asRight
    }
  }

  override def markFileComplete(file: S3Location): Either[String, Unit] =
    files.headOption match {
      case Some(headLocation) if headLocation.path == file.path =>
        files = files.drop(1)
        ().asRight
      case Some(headLocation) if headLocation.path == headLocation.path =>
        s"File (${file.bucket}:${file.pathOrUnknown}) does not match that at head of the queue, which is (${headLocation.bucket}:${headLocation.pathOrUnknown})".asLeft
      case _ => "No files in queue to mark as complete".asLeft
    }

  override def init(initFile: S3Location): Unit = {
    files += initFile
    (initFile.path, initFile.timestamp) match {
      case (Some(filePath), None) =>
        // for migrations from previous version where ts not stored in offset
        getLastModifiedFn(initFile.bucket, filePath)
          .map(ts => lastSeenFile = FileMetadata(filePath, ts).some)
        ()
      case (Some(filePath), Some(timestamp)) =>
        lastSeenFile = FileMetadata(filePath, timestamp).some
      case _ =>
        ()
    }
  }
}
