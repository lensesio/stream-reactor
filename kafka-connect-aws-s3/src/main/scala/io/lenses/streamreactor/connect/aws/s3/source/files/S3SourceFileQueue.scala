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
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocationWithLine
import io.lenses.streamreactor.connect.aws.s3.storage.FileListError
import io.lenses.streamreactor.connect.aws.s3.storage.FileMetadata
import io.lenses.streamreactor.connect.aws.s3.storage.ListResponse

import scala.collection.mutable.ListBuffer

trait SourceFileQueue {

  def init(initFile: RemoteS3PathLocationWithLine): Unit

  def next(): Either[FileListError, Option[RemoteS3PathLocationWithLine]]

  def markFileComplete(fileWithLine: RemoteS3PathLocation): Either[String, Unit]

}

/**
  * Blocking processor for queues of operations.  Used to ensure consistency.
  * Will block any further writes by the current file until the remote has caught up.
  */
class S3SourceFileQueue(
  batchListerFn: Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]],
) extends SourceFileQueue
    with LazyLogging {

  private var lastSeenFile: Option[FileMetadata] = None

  private var files = ListBuffer.empty[RemoteS3PathLocationWithLine]

  override def next(): Either[FileListError, Option[RemoteS3PathLocationWithLine]] =
    files.headOption.fold(retrieveNextFile())(Some(_).asRight)

  private def retrieveNextFile(
  ): Either[FileListError, Option[RemoteS3PathLocationWithLine]] = {
    val nextBatch: Either[FileListError, Option[ListResponse[String]]] = batchListerFn(lastSeenFile)
    nextBatch.flatMap {
      case Some(ListResponse(container, value, meta)) =>
        lastSeenFile = meta.some
        files ++= value.map(container.withPath(_).fromStart(meta.lastModified))
        files.headOption.asRight
      case _ => Option.empty.asRight
    }
  }

  override def markFileComplete(file: RemoteS3PathLocation): Either[String, Unit] =
    files.headOption match {
      case Some(RemoteS3PathLocationWithLine(headFile, _, _)) if headFile.equals(file) =>
        files = files.drop(1)
        ().asRight
      case Some(RemoteS3PathLocationWithLine(headFile, _, _)) =>
        s"File (${file.bucket}:${file.path}) does not match that at head of the queue, which is (${headFile.bucket}:${headFile.path})".asLeft
      case None => "No files in queue to mark as complete".asLeft
    }

  override def init(initFile: RemoteS3PathLocationWithLine): Unit = {
    files += initFile
    lastSeenFile = FileMetadata(initFile.file.path, initFile.lastModified).some
  }
}
