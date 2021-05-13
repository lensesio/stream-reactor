
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

import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.BucketAndPath
import io.lenses.streamreactor.connect.aws.s3.model.BucketAndPrefix
import org.jclouds.blobstore.BlobStoreContext
import org.jclouds.blobstore.domain.BlobMetadata
import org.jclouds.blobstore.domain.StorageType
import org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl
import org.jclouds.blobstore.options.CopyOptions
import org.jclouds.blobstore.options.ListContainerOptions
import org.jclouds.blobstore.options.PutOptions
import org.jclouds.io.payloads.BaseMutableContentMetadata
import org.jclouds.io.payloads.InputStreamPayload

import java.io.ByteArrayInputStream
import java.io.InputStream
import java.util.UUID
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class MultipartBlobStoreStorageInterface(sinkName: String, blobStoreContext: BlobStoreContext) extends StorageInterface with LazyLogging {

  private val blobStore = blobStoreContext.getBlobStore
  private val awsMaxKeys = 1000

  override def initUpload(bucketAndPath: BucketAndPath): MultiPartUploadState = {
    logger.debug(s"[{}] Initialising upload for bucketAndPath: {}", sinkName, bucketAndPath)
    val s3PutOptions = PutOptions.Builder.multipart()

    MultiPartUploadState(
      upload = blobStore.initiateMultipartUpload(
        bucketAndPath.bucket,
        buildBlobMetadata(bucketAndPath),
        s3PutOptions
      ),
      Vector()
    )
  }

  private def buildBlobMetadata(bucketAndPath: BucketAndPath): BlobMetadata = {
    val blobMetadata = new MutableBlobMetadataImpl()
    blobMetadata.setId(UUID.randomUUID().toString)
    blobMetadata.setName(bucketAndPath.path)
    blobMetadata
  }

  override def uploadPart(state: MultiPartUploadState, bytes: Array[Byte], size: Long): MultiPartUploadState = {
    logger.debug(s"[{}] Uploading part #{} for {}/{}",
      sinkName, state.parts.size, state.upload.containerName(), state.upload.blobName())

    val byteArrayInputStream = new ByteArrayInputStream(bytes.slice(0, size.toInt))

    val payload = new InputStreamPayload(byteArrayInputStream)
    val newPart = try {
      val contentMetadata = new BaseMutableContentMetadata()
      contentMetadata.setContentLength(size)
      payload.setContentMetadata(contentMetadata)
      blobStore.uploadMultipartPart(
        state.upload,
        state.parts.size + 1,
        payload
      )
    } finally {
      Try {
        payload.close()
      }
      Try {
        byteArrayInputStream.close()
      }
    }

    state.copy(parts = state.parts :+ newPart)
  }

  override def completeUpload(state: MultiPartUploadState): Unit = {

    logger.debug(s"[{}] Completing upload of {} with {} parts",
      sinkName, state.upload.blobName(), state.parts.size)

    blobStore.completeMultipartUpload(
      state.upload,
      state.parts.asJava
    )
  }

  override def rename(originalFilename: BucketAndPath, newFilename: BucketAndPath): Unit = {
    logger.info(s"[{}] Renaming upload from {} to {}", sinkName, originalFilename, newFilename)

    blobStore.copyBlob(originalFilename.bucket, originalFilename.path, newFilename.bucket, newFilename.path, CopyOptions.NONE)

    blobStore.removeBlob(originalFilename.bucket, originalFilename.path)
  }

  override def close(): Unit = blobStoreContext.close()

  override def pathExists(bucketAndPrefix: BucketAndPrefix): Boolean =
    blobStore.list(bucketAndPrefix.bucket, ListContainerOptions.Builder.prefix(bucketAndPrefix.prefix.getOrElse(""))).size() > 0

  override def pathExists(bucketAndPath: BucketAndPath): Boolean =
    blobStore.list(bucketAndPath.bucket, ListContainerOptions.Builder.prefix(bucketAndPath.path)).size() > 0

  override def list(bucketAndPath: BucketAndPath): List[String] = {

    val options = ListContainerOptions.Builder.recursive().prefix(bucketAndPath.path).maxResults(awsMaxKeys)

    var pageSetStrings: List[String] = List()
    var nextMarker: Option[String] = None
    do {
      if (nextMarker.nonEmpty) {
        options.afterMarker(nextMarker.get)
      }
      val pageSet = blobStore.list(bucketAndPath.bucket, options)
      nextMarker = Option(pageSet.getNextMarker)
      pageSetStrings ++= pageSet
        .asScala
        .filter(_.getType == StorageType.BLOB)
        .map(
          storageMetadata => storageMetadata.getName
        )
        .toList

    } while (nextMarker.nonEmpty)
    pageSetStrings
  }

  override def list(bucketAndPrefix: BucketAndPrefix): List[String] = {
    val options = bucketAndPrefix
      .prefix
      .fold(
        ListContainerOptions.Builder.recursive()
      )(
        ListContainerOptions.Builder.recursive().prefix
      )
      .maxResults(awsMaxKeys)

    var pageSetStrings: List[String] = List()
    var nextMarker: Option[String] = None
    do {
      if (nextMarker.nonEmpty) {
        options.afterMarker(nextMarker.get)
      }
      val pageSet = blobStore.list(bucketAndPrefix.bucket, options)
      nextMarker = Option(pageSet.getNextMarker)
      pageSetStrings ++= pageSet
        .asScala
        .filter(_.getType == StorageType.BLOB)
        .map(
          storageMetadata => storageMetadata.getName
        )
        .toList

    } while (nextMarker.nonEmpty)
    pageSetStrings

  }

  override def checkBucket(bucketAndPrefix: BucketAndPrefix): Option[Throwable] = {
    val options = bucketAndPrefix
      .prefix
      .fold(
        ListContainerOptions.Builder.recursive()
      )(
        ListContainerOptions.Builder.recursive().prefix
      )
      .maxResults(1)

    Try(blobStore.list(bucketAndPrefix.bucket, options)) match {
      case Failure(ex) => Some(ex)
      case Success(_) => None
    }



  }

  override def getBlob(bucketAndPath: BucketAndPath): InputStream = {
    blobStore.getBlob(bucketAndPath.bucket, bucketAndPath.path).getPayload.openStream()
  }

  override def getBlobSize(bucketAndPath: BucketAndPath): Long = {
    blobStore.getBlob(bucketAndPath.bucket, bucketAndPath.path).getMetadata.getSize
  }

}
