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
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.storage.FileListError
import io.lenses.streamreactor.connect.aws.s3.storage.FileLoadError
import io.lenses.streamreactor.connect.aws.s3.storage.FileMetadata
import io.lenses.streamreactor.connect.aws.s3.storage.ListResponse
import io.lenses.streamreactor.connect.cloud.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.model.location.CloudLocationValidator

import java.time.Instant

trait SourceFileQueue {
  def next(): Either[FileListError, Option[CloudLocation]]
}

/**
  * Blocking processor for queues of operations.  Used to ensure consistency.
  * Will block any further writes by the current file until the remote has caught up.
  */
class S3SourceFileQueue private (
  private val taskId:        ConnectorTaskId,
  private val batchListerFn: Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]],
  private var files:         Seq[CloudLocation],
  private var lastSeenFile:  Option[FileMetadata],
)(
  implicit
  cloudLocationValidator: CloudLocationValidator,
) extends SourceFileQueue
    with LazyLogging {

  def this(
    taskId:        ConnectorTaskId,
    batchListerFn: Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]],
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ) = this(taskId, batchListerFn, Seq.empty, None)

  override def next(): Either[FileListError, Option[CloudLocation]] =
    files match {
      case ::(head, next) =>
        files = next
        logger.debug(
          s"[${taskId.show}] Next file to read ${head.show}. ${if (files.isEmpty) ""
          else s"The one after next is ${files.head.show}"}",
        )
        head.some.asRight
      case Nil =>
        logger.debug(s"[${taskId.show}] Retrieving next batch of files")
        val result = retrieveNextFile()
        logger.debug(s"[${taskId.show}]Retrieved ${files.size} files")
        result
    }

  private def retrieveNextFile(
  ): Either[FileListError, Option[CloudLocation]] = {
    val nextBatch: Either[FileListError, Option[ListResponse[String]]] = batchListerFn(lastSeenFile)
    nextBatch.flatMap {
      case Some(ListResponse(bucket, prefix, value, meta)) =>
        lastSeenFile = meta.some
        files = value.map(path =>
          CloudLocation(
            bucket,
            prefix,
            path.some,
          ).fromStart().withTimestamp(meta.lastModified),
        )

        files match {
          case ::(head, next) =>
            files = next
            head.some.asRight
          case Nil => None.asRight
        }
      case _ => Option.empty.asRight
    }
  }
}

object S3SourceFileQueue {
  def from(
    batchListerFn:     Option[FileMetadata] => Either[FileListError, Option[ListResponse[String]]],
    getLastModifiedFn: (String, String) => Either[FileLoadError, Instant],
    startingFile:      CloudLocation,
    taskId:            ConnectorTaskId,
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): S3SourceFileQueue = {
    val lastSeen: Option[FileMetadata] = (startingFile.path, startingFile.timestamp) match {
      case (Some(filePath), None) =>
        // for migrations from previous version where ts not stored in offset
        getLastModifiedFn(startingFile.bucket, filePath)
          .map(FileMetadata(filePath, _)).toOption

      case (Some(filePath), Some(timestamp)) =>
        FileMetadata(filePath, timestamp).some
      case _ =>
        None
    }
    new S3SourceFileQueue(taskId, batchListerFn, Seq(startingFile), lastSeen)
  }

}
