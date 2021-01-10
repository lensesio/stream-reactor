
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

import java.io.{ByteArrayInputStream, InputStream}
import java.util.UUID
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.{BucketAndPath, BucketAndPrefix}
import org.jclouds.blobstore.BlobStoreContext
import org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl
import org.jclouds.blobstore.domain.{BlobMetadata, StorageType}
import org.jclouds.blobstore.options.{CopyOptions, ListContainerOptions, PutOptions}
import org.jclouds.io.payloads.{BaseMutableContentMetadata, InputStreamPayload}

import scala.collection.JavaConverters._
import scala.collection.immutable.VectorBuilder
import scala.util.Try

class MultipartBlobStoreStorage(blobStoreContext: BlobStoreContext) extends Storage with LazyLogging {

  private val blobStore = blobStoreContext.getBlobStore
  private val awsMaxKeys = 1000

  override def initUpload(bucketAndPath: BucketAndPath): MultiPartUploadState = {

    logger.debug(s"StorageInterface - initialising upload for bucketAndPath: $bucketAndPath")
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

    logger.debug(s"StorageInterface - uploading part #${state.parts.size} for ${state.upload.containerName()}/ ${state.upload.blobName()}")

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

    logger.debug(s"StorageInterface - Completing upload of ${state.upload.blobName()} with ${state.parts.size} parts")

    blobStore.completeMultipartUpload(
      state.upload,
      state.parts.asJava
    )
  }

  override def rename(originalFilename: BucketAndPath, newFilename: BucketAndPath): Unit = {
    logger.info(s"StorageInterface - renaming upload from $originalFilename to $newFilename")

    blobStore.copyBlob(
      originalFilename.bucket, originalFilename.path, newFilename.bucket, newFilename.path, CopyOptions.NONE)

    remove(originalFilename)
  }

  override def remove(filename: BucketAndPath): Unit = {
    blobStore.removeBlob(filename.bucket, filename.path)
  }

  override def close(): Unit = blobStoreContext.close()

  override def pathExists(bucketAndPrefix: BucketAndPrefix): Boolean =
    blobStore.list(bucketAndPrefix.bucket, ListContainerOptions.Builder.prefix(bucketAndPrefix.prefix.getOrElse(""))).size() > 0

  override def pathExists(bucketAndPath: BucketAndPath): Boolean =
    blobStore.list(bucketAndPath.bucket, ListContainerOptions.Builder.prefix(bucketAndPath.path)).size() > 0

  override def list(bucketAndPath: BucketAndPath): Vector[String] = {
    val options = ListContainerOptions.Builder.recursive().prefix(bucketAndPath.path).maxResults(awsMaxKeys)
    internalList(bucketAndPath.bucket, options)
  }

  override def list(bucketAndPrefix: BucketAndPrefix): Vector[String] = {
    val options: ListContainerOptions = bucketAndPrefix
      .prefix
      .map(ListContainerOptions.Builder.recursive().prefix)
      .getOrElse(ListContainerOptions.Builder.recursive())
      .maxResults(awsMaxKeys)

    internalList(bucketAndPrefix.bucket, options)
  }

  private def internalList(bucket:String, options: ListContainerOptions): Vector[String] = {
    val builder = new VectorBuilder[String]
    var nextMarker: Option[String] = None
    do {
      if (nextMarker.nonEmpty) {
        options.afterMarker(nextMarker.get)
      }
      val pageSet = blobStore.list(bucket, options)
      nextMarker = Option(pageSet.getNextMarker)
      pageSet.asScala.foreach { metadata =>
        if (metadata.getType == StorageType.BLOB)
          builder.+=(metadata.getName)
      }
    } while (nextMarker.nonEmpty)
    builder.result()
  }

  override def putBlob(bucketAndPath: BucketAndPath, payload: String): Unit = {
    val s3PutOptions = PutOptions.Builder.multipart()
    val blob = blobStore.blobBuilder(bucketAndPath.path)
      .payload(payload)
      .build
    blobStore.putBlob(bucketAndPath.bucket, blob, s3PutOptions)
  }

  override def getBlob(bucketAndPath: BucketAndPath): InputStream = {
    blobStore.getBlob(bucketAndPath.bucket, bucketAndPath.path).getPayload.openStream()
  }

  override def getBlobSize(bucketAndPath: BucketAndPath): Long = {
    blobStore.getBlob(bucketAndPath.bucket, bucketAndPath.path).getMetadata.getSize
  }

}
