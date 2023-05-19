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
import software.amazon.awssdk.services.s3.model.S3Object

import java.io.File
import java.io.InputStream
import java.time.Instant

case class FileMetadata(
  file:         String,
  lastModified: Instant,
)

case class ListResponse[T](
  containingBucketAndPath: RemoteS3PathLocation,
  files:                   Seq[T],
  latestFileMetadata:      FileMetadata,
)
trait StorageInterface {

  def uploadFile(source: File, target: RemoteS3PathLocation): Either[UploadError, Unit]

  def close(): Unit

  def pathExists(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, Boolean]

  def list(
    bucketAndPrefix: RemoteS3PathLocation,
    lastFile:        Option[FileMetadata],
    numResults:      Int,
  ): Either[FileListError, Option[ListResponse[String]]]

  def listRecursive[T](
    bucketAndPrefix: RemoteS3PathLocation,
    processFn:       (RemoteS3PathLocation, Seq[S3Object]) => Option[ListResponse[T]],
  ): Either[FileListError, Option[ListResponse[T]]]

  def getBlob(bucketAndPath: RemoteS3PathLocation): Either[String, InputStream]

  def getBlobAsString(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, String]

  def getBlobSize(bucketAndPath: RemoteS3PathLocation): Either[String, Long]

  def getBlobModified(location: RemoteS3PathLocation): Either[String, Instant]

  def writeStringToFile(target: RemoteS3PathLocation, data: String): Either[UploadError, Unit]

  def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit]

  def findDirectories(
    bucketAndPrefix:  RemoteS3RootLocation,
    completionConfig: DirectoryFindCompletionConfig,
    exclude:          Set[String],
    continueFrom:     Option[String],
  ): IO[DirectoryFindResults]

}
