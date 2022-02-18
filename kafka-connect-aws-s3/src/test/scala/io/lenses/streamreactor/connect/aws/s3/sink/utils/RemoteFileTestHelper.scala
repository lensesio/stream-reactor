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

package io.lenses.streamreactor.connect.aws.s3.sink.utils

import com.google.common.io.ByteStreams
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.storage.StorageInterface

import java.io.{File, InputStream}
import java.nio.file.Files
import java.time.{Instant, LocalDateTime}

class RemoteFileTestHelper(implicit storageInterface: StorageInterface) {

  def listBucketPath(bucketName: String, prefix: String): List[String] = {
    storageInterface.list(RemoteS3PathLocation(bucketName, prefix)) match {
      case Left(err) => throw err.exception
      case Right(fileList) => fileList
    }
  }

  def getFileSize(bucketName: String, fileName: String): Long = {
    storageInterface.getBlobSize(RemoteS3PathLocation(bucketName, fileName))
  }

  def getModificationDate(bucketName: String, fileName: String): Instant = {
    storageInterface.getBlobModified(RemoteS3PathLocation(bucketName, fileName))
  }

  def remoteFileAsBytes(bucketName: String, fileName: String): Array[Byte] = {
    streamToByteArray(remoteFileAsStream(bucketName, fileName))
  }

  def localFileAsBytes(localFile: File): Array[Byte] = {
    Files.readAllBytes(localFile.toPath)
  }

  def remoteFileAsStream(bucketName: String, fileName: String): InputStream = {
    storageInterface.getBlob(RemoteS3PathLocation(bucketName, fileName))
  }

  def remoteFileAsString(bucketName: String, fileName: String): String = {
    streamToString(remoteFileAsStream(bucketName, fileName))
  }

  def streamToString(inputStream: InputStream): String = {
    new String(streamToByteArray(inputStream)).replace("\n", "")
  }

  private def streamToByteArray(inputStream: InputStream): Array[Byte] = {
    ByteStreams.toByteArray(inputStream)
  }

}
