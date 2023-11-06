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
package io.lenses.streamreactor.connect.cloud.common.storage

import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString

import java.io.InputStream
import java.time.Instant

trait StorageInterface[SM <: FileMetadata] extends ResultProcessors {

  def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, Unit]

  def close(): Unit

  def pathExists(bucket: String, path: String): Either[FileLoadError, Boolean]

  def list(
    bucket:     String,
    prefix:     Option[String],
    lastFile:   Option[SM],
    numResults: Int,
  ): Either[FileListError, Option[ListOfKeysResponse[SM]]]

  def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[SM]]]

  def listKeysRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfKeysResponse[SM]]]

  def seekToFile(
    bucket:       String,
    fileName:     String,
    lastModified: Option[Instant],
  ): Option[SM]

  def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream]

  def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String]

  def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata]

  def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit]

  def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit]
}
