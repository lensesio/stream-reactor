
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
import org.apache.commons.io.IOUtils
import org.jclouds.blobstore.BlobStoreContext
import org.jclouds.blobstore.domain.StorageType
import org.jclouds.blobstore.options.ListContainerOptions

import java.io.{File, InputStream}
import java.time.{Instant, LocalDateTime}
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
        blobStore.putBlob(
          target.bucket,
          blobStore.blobBuilder(target.path)
            .payload(source)
            .contentLength(source.length())
            .build()
        )
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

  override def writeStringToFile(target: RemoteS3PathLocation, data: String): Either[UploadError, Unit] = {
    logger.debug(s"[{}] Uploading file from data string ({}) to s3 {}", sinkName, data, target)

    if(data.isEmpty){
      EmptyContentsStringError(data).asLeft
    } else {
      Try {
        blobStore.putBlob(
          target.bucket,
          blobStore.blobBuilder(target.path)
            .payload(data)
            .contentLength(data.length())
            .build()
        )
      } match {
        case Failure(exception) =>
          logger.error(s"[{}] Failed upload from data string ({}) to s3 {}", sinkName, data, target, exception)
          FileCreateError(exception, data).asLeft
        case Success(_) =>
          logger.debug(s"[{}] Completed upload from data string ({}) to s3 {}", sinkName, data, target)
          ().asRight
      }
    }
  }

  override def close(): Unit = blobStoreContext.close()

  def pathExistsInternal(bucketAndPath: RemoteS3PathLocation): Boolean =
    blobStore.list(bucketAndPath.bucket, ListContainerOptions.Builder.prefix(bucketAndPath.path)).size() > 0

  override def pathExists(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, Boolean] = {
    Try {
      pathExistsInternal(bucketAndPath)
    }.toEither.leftMap(FileLoadError(_, bucketAndPath.path))
  }

  private def listInternal(bucketAndPath: RemoteS3PathLocation): List[String] = {

    val options = ListContainerOptions.Builder.recursive().prefix(bucketAndPath.path).maxResults(awsMaxKeys)

    var pageSetStrings: List[String] = List()
    var nextMarker: Option[String] = None
    do {
      nextMarker.foreach(options.afterMarker)
      val pageSet = blobStore.list(bucketAndPath.bucket, options)
      nextMarker = Option(pageSet.getNextMarker)
      pageSetStrings ++= pageSet
        .asScala
        .collect{
          case blobOnly if blobOnly.getType == StorageType.BLOB => blobOnly.getName
        }
    } while (nextMarker.nonEmpty)
    pageSetStrings
  }

  override def list(bucketAndPrefix: RemoteS3PathLocation): Either[FileListError, List[String]] = {
    Try (listInternal(bucketAndPrefix))
      .toEither
      .leftMap(e => FileListError(e, bucketAndPrefix.path))
  }

  override def getBlob(bucketAndPath: RemoteS3PathLocation): InputStream = {
    blobStore.getBlob(bucketAndPath.bucket, bucketAndPath.path).getPayload.openStream()
  }

  override def getBlobSize(bucketAndPath: RemoteS3PathLocation): Long = {
    blobStore.getBlob(bucketAndPath.bucket, bucketAndPath.path).getMetadata.getSize
  }

  override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] = {
    Try (blobStore.removeBlobs(bucket, files.asJava))
        .toEither
        .leftMap(e => FileDeleteError(e, s"issue while deleting $files"))
  }

  override def getBlobAsString(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, String] = {
    Try {
      IOUtils.toString(getBlob(bucketAndPath))
    }.toEither.leftMap(FileLoadError(_, bucketAndPath.path))
  }

  override def getBlobModified(location: RemoteS3PathLocation): Instant = {
    blobStore.blobMetadata(location.bucket, location.path).getLastModified.toInstant
  }
}
