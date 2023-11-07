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
package io.lenses.streamreactor.connect.cloud.common.source.files

import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.toShow
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.ListResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface

trait SourceFileQueue {
  def next(): Either[FileListError, Option[CloudLocation]]
}

/**
  * Blocking processor for queues of operations.  Used to ensure consistency.
  * Will block any further writes by the current file until the remote has caught up.
  */
class CloudSourceFileQueue[SM <: FileMetadata] private (
  private val taskId:        ConnectorTaskId,
  private val batchListerFn: Option[SM] => Either[FileListError, Option[ListResponse[String, SM]]],
  private var files:         Seq[CloudLocation],
  private var lastSeenFile:  Option[SM],
)(
  implicit
  cloudLocationValidator: CloudLocationValidator,
) extends SourceFileQueue
    with LazyLogging {

  def this(
    taskId:        ConnectorTaskId,
    batchListerFn: Option[SM] => Either[FileListError, Option[ListOfKeysResponse[SM]]],
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
    val nextBatch: Either[FileListError, Option[ListResponse[String, SM]]] = batchListerFn(lastSeenFile)
    nextBatch.flatMap {
      case Some(ListOfKeysResponse(bucket, prefix, value, meta)) =>
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

object CloudSourceFileQueue {
  def from[SM <: FileMetadata](
    batchListerFn:    Option[SM] => Either[FileListError, Option[ListOfKeysResponse[SM]]],
    storageInterface: StorageInterface[SM],
    startingFile:     CloudLocation,
    taskId:           ConnectorTaskId,
  )(
    implicit
    cloudLocationValidator: CloudLocationValidator,
  ): CloudSourceFileQueue[SM] = {
    val lastSeen: Option[SM] = (startingFile.path, startingFile.timestamp) match {
      case (Some(filePath), maybeTimestamp) =>
        // for migrations from previous version where ts not stored in offset
        storageInterface.seekToFile(startingFile.bucket, filePath, maybeTimestamp)

      case _ =>
        Option.empty[SM]
    }
    new CloudSourceFileQueue(taskId, batchListerFn, Seq(startingFile), lastSeen)
  }

}
