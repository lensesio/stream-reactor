/*
 * Copyright 2021 Lenses.io
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
import org.jclouds.blobstore.BlobStoreContext
import software.amazon.awssdk.services.s3.S3Client

import java.io.InputStream

class S3StorageInterface(sinkName: String, s3Client: S3Client, blobStoreContext: BlobStoreContext) extends JCloudsStorageInterface(sinkName, blobStoreContext) {

  override def pathExists(bucketAndPrefix: RemoteS3RootLocation): Either[Throwable, Boolean] = s3SI.pathExists(bucketAndPrefix)

  override def list(bucketAndPrefix: RemoteS3RootLocation, lastFile: Option[RemoteS3PathLocation], numResults: Int): Either[Throwable, List[String]] = s3SI.list(bucketAndPrefix, lastFile, numResults)

  // when we upgrade to Scala 3 this can be changed to a Mixin trait with parameters, rendering this unnecessary
  val s3SI = new AwsS3StorageInterface(s3Client) {
    override def initUpload(bucketAndPath: RemoteS3PathLocation): MultiPartUploadState = ???

    override def completeUpload(state: MultiPartUploadState): Unit = ???

    override def uploadPart(state: MultiPartUploadState, bytes: Array[Byte]): MultiPartUploadState = ???

    override def uploadFile(initialName: LocalPathLocation, finalDestination: RemoteS3PathLocation): Unit = ???

    override def rename(originalFilename: RemoteS3PathLocation, newFilename: RemoteS3PathLocation): Unit = ???

    override def close(): Unit = ???

    override def pathExists(bucketAndPath: RemoteS3PathLocation): Boolean = ???

    override def list(bucketAndPrefix: RemoteS3PathLocation): List[String] = ???

    override def getBlob(bucketAndPath: RemoteS3PathLocation): InputStream = ???

    override def getBlobSize(bucketAndPath: RemoteS3PathLocation): Long = ???

  }
}
