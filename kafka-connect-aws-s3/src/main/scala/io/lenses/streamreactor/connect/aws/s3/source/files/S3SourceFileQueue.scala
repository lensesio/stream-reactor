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
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocationWithLine
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

import scala.collection.mutable.ListBuffer

trait SourceFileQueue {

  def init(initFile: RemoteS3PathLocationWithLine): Unit

  def next(): Either[Throwable, Option[RemoteS3PathLocationWithLine]]

  def markFileComplete(fileWithLine: RemoteS3PathLocation): Either[String, Unit]

}

/**
  * Blocking processor for queues of operations.  Used to ensure consistency.
  * Will block any further writes by the current file until the remote has caught up.
  *
  * @param storageInterface the storage interface of the remote service.
  */
class S3SourceFileQueue(
  root:       RemoteS3RootLocation,
  numResults: Int,
)(
  implicit
  storageInterface: StorageInterface,
) extends SourceFileQueue
    with LazyLogging {

  private var lastSeenFile: Option[RemoteS3PathLocation] = None

  private var files = ListBuffer.empty[RemoteS3PathLocationWithLine]

  override def next(): Either[Throwable, Option[RemoteS3PathLocationWithLine]] =
    files.headOption.fold(retrieveNextFile(lastSeenFile))(nextFile => Some(nextFile).asRight)

  private def retrieveNextFile(
    lastSeenFile: Option[RemoteS3PathLocation],
  ): Either[Throwable, Option[RemoteS3PathLocationWithLine]] =
    listBatch(root, lastSeenFile, numResults)
      .map {
        value =>
          files ++= value.map(_.fromStart())
          files.headOption
      }

  def listBatch(
    bucketAndPrefix: RemoteS3RootLocation,
    lastFile:        Option[RemoteS3PathLocation],
    numResults:      Int,
  ): Either[Throwable, List[RemoteS3PathLocation]] =
    storageInterface
      .list(bucketAndPrefix, lastFile, numResults)
      .map(_.map(bucketAndPrefix.withPath))

  override def markFileComplete(file: RemoteS3PathLocation): Either[String, Unit] =
    files.headOption match {
      case Some(RemoteS3PathLocationWithLine(headFile, _)) if headFile.equals(file) => files =
          files.drop(1)
        lastSeenFile = Some(file)
        ().asRight
      case Some(RemoteS3PathLocationWithLine(headFile, _)) =>
        s"File (${file.bucket}:${file.path}) does not match that at head of the queue, which is (${headFile.bucket}:${headFile.path})".asLeft
      case None => "No files in queue to mark as complete".asLeft
    }

  override def init(initFile: RemoteS3PathLocationWithLine): Unit = {
    val _ = { files += initFile }
  }
}
