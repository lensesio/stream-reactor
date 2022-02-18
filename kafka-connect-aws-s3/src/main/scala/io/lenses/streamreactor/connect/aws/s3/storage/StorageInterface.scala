
/*
 * Copyright 2020 Lenses.io
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

import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation

import java.io.{File, InputStream}
import java.time.{Instant, LocalDateTime}

trait StorageInterface {

  def uploadFile(source: File, target: RemoteS3PathLocation): Either[UploadError, Unit]

  def close(): Unit

  def pathExists(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, Boolean]

  def list(bucketAndPrefix: RemoteS3PathLocation): Either[FileListError, List[String]]

  def getBlob(bucketAndPath: RemoteS3PathLocation): InputStream

  def getBlobAsString(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, String]

  def getBlobSize(bucketAndPath: RemoteS3PathLocation): Long

  def getBlobModified(location: RemoteS3PathLocation): Instant

  def writeStringToFile(target: RemoteS3PathLocation, data: String): Either[UploadError, Unit]

  def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit]
}

