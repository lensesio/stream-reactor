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
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.aws.s3.model.location.{RemoteS3PathLocation, RemoteS3RootLocation}
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.{GetObjectRequest, GetObjectResponse, ListObjectsV2Request, NoSuchKeyException, PutObjectRequest}
import cats.implicits._
import software.amazon.awssdk.core.ResponseInputStream

import java.io.{File, InputStream}
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.{Failure, Success, Try}

class AwsS3StorageInterface(connectorName: String, s3Client: S3Client) extends SourceStorageInterface with StorageInterface with LazyLogging {

  override def list(bucketAndPrefix: RemoteS3RootLocation, lastFile: Option[RemoteS3PathLocation], numResults: Int): Either[Throwable, List[String]] = {

    Try {

      val builder = ListObjectsV2Request
        .builder()
        .maxKeys(numResults)
        .bucket(bucketAndPrefix.bucket)

      bucketAndPrefix.prefix.foreach(builder.prefix)
      lastFile.foreach(lf => builder.startAfter(lf.path))

      val listObjectsV2Response = s3Client.listObjectsV2(builder.build())
      listObjectsV2Response.contents().asScala.map(_.key()).toList

    }.toEither
  }

  override def uploadFile(source: File, target: RemoteS3PathLocation): Either[UploadError, Unit] = {

    logger.debug(s"[{}] Uploading file from local {} to s3 {}", connectorName, source, target)

    if(!source.exists()){
      NonExistingFileError(source).asLeft
    } else if (source.length() == 0L){
      ZeroByteFileError(source).asLeft
    } else Try {
        s3Client.putObject(PutObjectRequest.builder()
          .bucket(target.bucket)
          .key(target.path)
          .contentLength(source.length())
          .build(), source.toPath)
      } match {
        case Failure(exception) =>
          logger.error(s"[{}] Failed upload from local {} to s3 {}", connectorName, source, target, exception)
          UploadFailedError(exception, source).asLeft
        case Success(_) =>
          logger.debug(s"[{}] Completed upload from local {} to s3 {}", connectorName, source, target)
          ().asRight
      }
  }

  override def pathExists(bucketAndPath: RemoteS3PathLocation): Either[String, Boolean] = {

    logger.debug(s"[{}] Path exists? {}", connectorName, bucketAndPath)

    Try {
      s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(bucketAndPath.bucket).prefix(bucketAndPath.path).build()).keyCount().toInt
    } match {
      case Failure(_: NoSuchKeyException) => false.asRight
      case Failure(exception) => exception.getMessage.asLeft
      case Success(keyCount : Int) => (keyCount > 0).asRight
    }
  }

  private def getBlobInner(bucketAndPath: RemoteS3PathLocation) = {
    val gor = GetObjectRequest
      .builder()
      .bucket(bucketAndPath.bucket)
      .key(bucketAndPath.path)
      .build()

    s3Client
      .getObject(
        gor
      )
  }

  override def getBlob(bucketAndPath: RemoteS3PathLocation): Either[String, InputStream] = Try(getBlobInner(bucketAndPath)) match {
    case Failure(exception) => exception.getMessage.asLeft
    case Success(value: ResponseInputStream[GetObjectResponse]) => value.asRight
  }

  override def getBlobSize(bucketAndPath: RemoteS3PathLocation): Either[String, Long] = {
    Try (getBlobInner(bucketAndPath).response().metadata().size().toLong).toEither.leftMap(_.getMessage)
  }

  override def list(bucketAndPrefix: RemoteS3PathLocation): Either[String,List[String]] = Try {

    logger.debug(s"[{}] List path {}", connectorName, bucketAndPrefix)

    val options = ListObjectsV2Request.builder().bucket(bucketAndPrefix.bucket).prefix(bucketAndPrefix.path)

    var pageSetStrings: List[String] = List()
    var nextMarker: Option[String] = None
    do {
      options.continuationToken(nextMarker.orNull)
      val pageSet = s3Client.listObjectsV2(options.build())
      nextMarker = Option(pageSet.continuationToken()).filter(_.trim.nonEmpty)
      pageSetStrings ++= pageSet
        .contents()
        .asScala
        .map(_.key)
        .toList

    } while (nextMarker.nonEmpty)

    pageSetStrings
  }.toEither.leftMap(_.getMessage)

  override def close(): Unit = s3Client.close()

}
