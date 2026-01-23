/*
 * Copyright 2017-2026 Lenses.io Ltd
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

import cats.implicits.toBifunctorOps
import io.circe.Decoder
import io.circe.Encoder
import io.circe.parser.decode
import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectProtection
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag

import java.io.InputStream
import java.time.Instant

trait StorageInterface[SM <: FileMetadata] extends ResultProcessors {

  /**
   * Gets the system name for use in log messages.
   * @return
   */
  def system(): String

  def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String]

  def close(): Unit

  def pathExists(bucket: String, path: String): Either[PathError, Boolean]

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

  def getBlobAsObject[O](
    bucket: String,
    path:   String,
  )(
    implicit
    decoder: Decoder[O],
  ): Either[FileLoadError, ObjectWithETag[O]] =
    for {
      (s, eTag) <- getBlobAsStringAndEtag(bucket, path)
      decoded   <- decode[O](s).leftMap(e => GeneralFileLoadError(e, path))
    } yield {
      ObjectWithETag[O](decoded, eTag)
    }

  def writeBlobToFile[O](
    bucket:           String,
    path:             String,
    objectProtection: ObjectProtection[O],
  )(
    implicit
    encoder: Encoder[O],
  ): Either[UploadError, ObjectWithETag[O]]

  def getBlobAsStringAndEtag(bucket: String, path: String): Either[FileLoadError, (String, String)]

  def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata]

  def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit]

  def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit]

  def deleteFile(bucket: String, file: String, eTag: String): Either[FileDeleteError, Unit]

  def mvFile(
    oldBucket: String,
    oldPath:   String,
    newBucket: String,
    newPath:   String,
    maybeEtag: Option[String],
  ): Either[FileMoveError, Unit]

  /**
   * Creates a directory if it does not already exist.
   *
   * @param bucket The name of the bucket where the directory should be created.
   * @param path The path of the directory to create.
   * @return Either a FileCreateError if the directory creation failed, or Unit if the directory was created successfully or already exists.
   */
  def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit]

  /**
   * Updates the lastModified timestamp of a file by copying it to itself.
   * This is used to "touch" late-arrival files so they appear after the watermark.
   *
   * @param bucket The name of the bucket where the file is located.
   * @param path The path of the file to touch.
   * @return Either a FileTouchError if the operation failed, or Unit if successful.
   */
  def touchFile(bucket: String, path: String): Either[FileTouchError, Unit]
}
