
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

import io.lenses.streamreactor.connect.aws.s3.model.location.{LocalPathLocation, RemoteS3PathLocation, RemoteS3RootLocation}
import org.jclouds.blobstore.domain.{MultipartPart, MultipartUpload}

import java.io.InputStream

case class MultiPartUploadState(
                                 upload: MultipartUpload,
                                 parts: Vector[MultipartPart],
                               )

trait StorageInterface {

  def initUpload(bucketAndPath: RemoteS3PathLocation): MultiPartUploadState

  def completeUpload(state: MultiPartUploadState): Unit

  def uploadPart(state: MultiPartUploadState, bytes: Array[Byte]): MultiPartUploadState

  def uploadFile(initialName: LocalPathLocation, finalDestination: RemoteS3PathLocation): Unit

  def rename(originalFilename: RemoteS3PathLocation, newFilename: RemoteS3PathLocation): Unit

  def close(): Unit

  def pathExists(bucketAndPath: RemoteS3PathLocation): Boolean

  def list(bucketAndPrefix: RemoteS3PathLocation): List[String]

  def getBlob(bucketAndPath: RemoteS3PathLocation): InputStream

  def getBlobSize(bucketAndPath: RemoteS3PathLocation): Long

  def pathExists(bucketAndPrefix: RemoteS3RootLocation): Either[Throwable, Boolean]

  def list(
            bucketAndPrefix: RemoteS3RootLocation,
            lastFile : Option[RemoteS3PathLocation],
            numResults : Int
          ): Either[Throwable, List[String]]

}

