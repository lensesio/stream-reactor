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

import software.amazon.awssdk.services.s3.model.S3Object

import java.io.File
import java.io.InputStream
import java.time.Instant

case class FileMetadata(
  file:         String,
  lastModified: Instant,
)

case class ListResponse[T](
  bucket:             String,
  prefix:             Option[String],
  files:              Seq[T],
  latestFileMetadata: FileMetadata,
)
trait StorageInterface {

  def uploadFile(source: File, bucket: String, path: String): Either[UploadError, Unit]

  def close(): Unit

  def pathExists(bucket: String, path: String): Either[FileLoadError, Boolean]

  def list(
    bucket:     String,
    prefix:     Option[String],
    lastFile:   Option[FileMetadata],
    numResults: Int,
  ): Either[FileListError, Option[ListResponse[String]]]

  def listRecursive[T](
    bucket:    String,
    prefix:    Option[String],
    processFn: (String, Option[String], Seq[S3Object]) => Option[ListResponse[T]],
  ): Either[FileListError, Option[ListResponse[T]]]

  def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream]

  def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String]

  def getBlobSize(bucket: String, path: String): Either[FileLoadError, Long]

  def getBlobModified(bucket: String, path: String): Either[FileLoadError, Instant]

  def writeStringToFile(bucket: String, path: String, data: String): Either[UploadError, Unit]

  def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit]
}
