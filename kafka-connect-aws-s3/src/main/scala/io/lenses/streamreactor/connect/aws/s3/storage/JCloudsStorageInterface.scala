
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

import cats.implicits._
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import org.jclouds.blobstore.BlobStoreContext
import org.jclouds.blobstore.domain.StorageType
import org.jclouds.blobstore.options.ListContainerOptions

import java.io.{File, InputStream}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class JCloudsStorageInterface(sinkName: String, blobStoreContext: BlobStoreContext) extends StorageInterface with LazyLogging {

  private val blobStore = blobStoreContext.getBlobStore
  private val awsMaxKeys = 1000

  override def uploadFile(source: File, target: RemoteS3PathLocation): Either[UploadError, Unit] = {
    logger.debug(s"[{}] Uploading file from local {} to s3 {}", sinkName, source, target)

    if(!source.exists()){
      NonExistingFileError(source).asLeft
    } else if (source.length() == 0L){
      ZeroByteFileError(source).asLeft
    } else {
      Try {
        val blob = blobStore
          .blobBuilder(target.path)
          .payload(source)
          .contentLength(source.length())
          .build()

        blobStore.putBlob(target.bucket, blob)
        ()
      } match {
        case Failure(exception) =>
          logger.error(s"[{}] Failed upload from local {} to s3 {}", sinkName, source, target, exception)
          UploadFailedError(exception, source).asLeft
        case Success(_) =>
          logger.debug(s"[{}] Completed upload from local {} to s3 {}", sinkName, source, target)
          ().asRight
      }
    }


  }

  override def close(): Unit = blobStoreContext.close()

  override def pathExists(bucketAndPath: RemoteS3PathLocation): Boolean =
    blobStore.list(bucketAndPath.bucket, ListContainerOptions.Builder.prefix(bucketAndPath.path)).size() > 0

  override def list(bucketAndPath: RemoteS3PathLocation): List[String] = {

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

  override def getBlob(bucketAndPath: RemoteS3PathLocation): InputStream = {
    blobStore.getBlob(bucketAndPath.bucket, bucketAndPath.path).getPayload.openStream()
  }

  override def getBlobSize(bucketAndPath: RemoteS3PathLocation): Long = {
    blobStore.getBlob(bucketAndPath.bucket, bucketAndPath.path).getMetadata.getSize
  }

}
