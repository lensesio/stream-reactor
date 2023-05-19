/*
 * Copyright 2017-2023 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.aws.s3.config.ConnectorTaskId
import io.lenses.streamreactor.connect.aws.s3.model.location.RemoteS3PathLocation
import io.lenses.streamreactor.connect.aws.s3.storage.ResultProcessors.processObjectsAsString
import org.apache.commons.io.IOUtils
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.io.File
import java.io.InputStream
import java.nio.charset.Charset
import java.time.Instant
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class AwsS3StorageInterface(implicit connectorTaskId: ConnectorTaskId, s3Client: S3Client)
    extends AwsS3DirectoryLister
    with StorageInterface
    with LazyLogging {

  override def list(
    bucketAndPrefix: RemoteS3PathLocation,
    lastFile:        Option[FileMetadata],
    numResults:      Int,
  ): Either[FileListError, Option[ListResponse[String]]] =
    Try {

      val builder = ListObjectsV2Request
        .builder()
        .maxKeys(numResults)
        .bucket(bucketAndPrefix.bucket)

      bucketAndPrefix.prefix.foreach(builder.prefix)
      lastFile.foreach(lf => builder.startAfter(lf.file))

      val listObjectsV2Response = s3Client.listObjectsV2(builder.build())
      processObjectsAsString(
        bucketAndPrefix,
        listObjectsV2Response
          .contents()
          .asScala
          .toSeq,
      )

    }.toEither.leftMap {
      ex: Throwable => FileListError(ex, bucketAndPrefix.path)
    }

  override def listRecursive[T](
    bucketAndPrefix: RemoteS3PathLocation,
    processFn:       (RemoteS3PathLocation, Seq[S3Object]) => Option[ListResponse[T]],
  ): Either[FileListError, Option[ListResponse[T]]] = Try {

    logger.debug(s"[{}] List path {}", connectorTaskId.show, bucketAndPrefix)

    val options = ListObjectsV2Request
      .builder()
      .bucket(bucketAndPrefix.bucket)
      .prefix(bucketAndPrefix.path)
      .build()

    val pagReq = s3Client.listObjectsV2Paginator(options)

    processFn(bucketAndPrefix, pagReq.iterator().asScala.flatMap(_.contents().asScala.toSeq).toSeq)

  }.toEither.leftMap {
    ex: Throwable => FileListError(ex, bucketAndPrefix.path)
  }

  override def uploadFile(source: File, target: RemoteS3PathLocation): Either[UploadError, Unit] = {

    logger.debug(s"[{}] AWS Uploading file from local {} to s3 {}", connectorTaskId.show, source, target)

    if (!source.exists()) {
      NonExistingFileError(source).asLeft
    } else if (source.length() == 0L) {
      ZeroByteFileError(source).asLeft
    } else
      Try {
        s3Client.putObject(PutObjectRequest.builder()
                             .bucket(target.bucket)
                             .key(target.path)
                             .contentLength(source.length())
                             .build(),
                           source.toPath,
        )
      } match {
        case Failure(exception) =>
          logger.error(s"[{}] Failed upload from local {} to s3 {}", connectorTaskId.show, source, target, exception)
          UploadFailedError(exception, source).asLeft
        case Success(_) =>
          logger.debug(s"[{}] Completed upload from local {} to s3 {}", connectorTaskId.show, source, target)
          ().asRight
      }
  }

  override def pathExists(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, Boolean] = {

    logger.debug(s"[{}] Path exists? {}", connectorTaskId.show, bucketAndPath)

    Try {
      s3Client.listObjectsV2(
        ListObjectsV2Request.builder().bucket(bucketAndPath.bucket).prefix(bucketAndPath.path).build(),
      ).keyCount().toInt
    } match {
      case Failure(_: NoSuchKeyException) => false.asRight
      case Failure(exception) => FileLoadError(exception, bucketAndPath.path).asLeft
      case Success(keyCount: Int) => (keyCount > 0).asRight
    }
  }

  private def getBlobInner(bucketAndPath: RemoteS3PathLocation) = {
    val request = GetObjectRequest
      .builder()
      .bucket(bucketAndPath.bucket)
      .key(bucketAndPath.path)
      .build()
    s3Client
      .getObject(
        request,
      )
  }

  private def headBlobInner(bucketAndPath: RemoteS3PathLocation) =
    s3Client
      .headObject(
        HeadObjectRequest
          .builder()
          .bucket(bucketAndPath.bucket)
          .key(bucketAndPath.path)
          .build(),
      )

  override def getBlob(bucketAndPath: RemoteS3PathLocation): Either[String, InputStream] =
    Try(getBlobInner(bucketAndPath)) match {
      case Failure(exception) => exception.getMessage.asLeft
      case Success(value: ResponseInputStream[GetObjectResponse]) =>
        value.asRight
    }

  override def getBlobSize(bucketAndPath: RemoteS3PathLocation): Either[String, Long] =
    Try(headBlobInner(bucketAndPath).contentLength().toLong).toEither.leftMap(_.getMessage)

  override def close(): Unit = s3Client.close()

  override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] = Try {
    if (files.isEmpty) {
      return ().asRight
    }
    s3Client.deleteObjects(
      DeleteObjectsRequest
        .builder()
        .bucket(bucket)
        .delete(
          Delete
            .builder()
            .objects(
              files
                .map(f => ObjectIdentifier.builder().key(f).build()).toArray: _*,
            )
            .build(),
        )
        .build(),
    )
  } match {
    case Failure(ex) => FileDeleteError(ex, files.mkString(" - ")).asLeft
    case Success(_)  => ().asRight
  }

  override def getBlobAsString(bucketAndPath: RemoteS3PathLocation): Either[FileLoadError, String] = for {
    blob <- getBlob(bucketAndPath).leftMap(e => FileLoadError(new IllegalArgumentException(e), bucketAndPath.path))
    asString <- Try(IOUtils.toString(blob, Charset.forName("UTF-8"))).toEither.leftMap(FileLoadError(_,
                                                                                                     bucketAndPath.path,
    ))
  } yield asString

  override def getBlobModified(location: RemoteS3PathLocation): Either[String, Instant] =
    Try(headBlobInner(location).lastModified()).toEither.leftMap(_.getMessage)

  override def writeStringToFile(target: RemoteS3PathLocation, data: String): Either[UploadError, Unit] = {
    logger.debug(s"[{}] Uploading file from data string ({}) to s3 {}", connectorTaskId.show, data, target)

    if (data.isEmpty) {
      EmptyContentsStringError(data).asLeft
    } else {
      Try {
        s3Client.putObject(
          PutObjectRequest
            .builder()
            .bucket(target.bucket)
            .key(target.path)
            .contentLength(data.length.toLong)
            .build(),
          RequestBody.fromString(data, Charset.forName("UTF-8")),
        )
      } match {
        case Failure(exception) =>
          logger.error(s"[{}] Failed upload from data string ({}) to s3 {}",
                       connectorTaskId.show,
                       data,
                       target,
                       exception,
          )
          FileCreateError(exception, data).asLeft
        case Success(_) =>
          logger.debug(s"[{}] Completed upload from data string ({}) to s3 {}", connectorTaskId.show, data, target)
          ().asRight
      }
    }
  }

}
