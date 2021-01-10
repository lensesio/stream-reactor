
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

import java.io.InputStream

import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, BucketAndPrefix}
import org.jclouds.blobstore.domain.{MultipartPart, MultipartUpload}

case class MultiPartUploadState(
                                 upload: MultipartUpload,
                                 parts: Vector[MultipartPart]
                               )

trait Storage {
  def initUpload(bucketAndPath: BucketAndPath): MultiPartUploadState

  def completeUpload(state: MultiPartUploadState): Unit

  def uploadPart(state: MultiPartUploadState, bytes: Array[Byte], size: Long): MultiPartUploadState

  def rename(originalFilename: BucketAndPath, newFilename: BucketAndPath): Unit

  def remove(filename: BucketAndPath): Unit

  def close(): Unit

  def pathExists(bucketAndPrefix: BucketAndPrefix): Boolean

  def pathExists(bucketAndPath: BucketAndPath): Boolean

  def list(bucketAndPrefix: BucketAndPath): Vector[String]

  def list(bucketAndPrefix: BucketAndPrefix): Vector[String]

  def putBlob(bucketAndPath: BucketAndPath, payload: String): Unit

  def getBlob(bucketAndPath: BucketAndPath): InputStream

  def getBlobSize(bucketAndPath: BucketAndPath): Long
}
