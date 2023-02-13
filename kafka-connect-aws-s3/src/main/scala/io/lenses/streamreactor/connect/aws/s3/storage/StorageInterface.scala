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
package io.lenses.streamreactor.connect.aws.s3.storage

import cats.effect.IO
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3RootLocation

import java.io.File
import java.io.InputStream
import java.time.Clock
import java.time.Instant

trait StorageInterface {

  def uploadFile(source: File, target: RemoteS3PathLocation): Either[UploadError, Unit]

  def close(): Unit

  def pathExists(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, Boolean]

  def list(bucketAndPrefix: RemoteS3PathLocation): Either[FileListError, List[String]]

  def getBlob(bucketAndPath: RemoteS3PathLocation): Either[String, InputStream]

  def getBlobAsString(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, String]

  def getBlobSize(bucketAndPath: RemoteS3PathLocation): Either[String, Long]

  def getBlobModified(location: RemoteS3PathLocation): Either[String, Instant]

  def writeStringToFile(target: RemoteS3PathLocation, data: String): Either[UploadError, Unit]

  def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit]

  def list(
    bucketAndPrefix: RemoteS3RootLocation,
    lastFile:        Option[RemoteS3PathLocation],
    numResults:      Int,
  ): Either[Throwable, List[String]]

  def findDirectories(
    bucketAndPrefix:  RemoteS3RootLocation,
    completionConfig: DirectoryFindCompletionConfig,
    exclude:          Set[String],
    continueFrom:     Option[String],
  )(
    implicit
    clock: Clock,
  ): IO[DirectoryFindResults]

}
