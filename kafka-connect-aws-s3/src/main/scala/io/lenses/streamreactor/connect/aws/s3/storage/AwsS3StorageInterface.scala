/*
 * Copyright 2017-2025 Lenses.io Ltd
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
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.storage.ExtensionFilter
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.ListResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import org.apache.commons.io.IOUtils
import software.amazon.awssdk.core.ResponseInputStream
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model._

import java.io.InputStream
import java.nio.charset.Charset
import java.time.Instant
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class AwsS3StorageInterface(
  connectorTaskId: ConnectorTaskId,
  s3Client:        S3Client,
  batchDelete:     Boolean,
  extensionFilter: Option[ExtensionFilter],
) extends StorageInterface[S3FileMetadata]
    with LazyLogging {

  override def list(
    bucket:     String,
    prefix:     Option[String],
    lastFile:   Option[S3FileMetadata],
    numResults: Int,
  ): Either[FileListError, Option[ListOfKeysResponse[S3FileMetadata]]] =
    Try {

      val builder = ListObjectsV2Request
        .builder()
        .maxKeys(numResults)
        .bucket(bucket)

      prefix.foreach(builder.prefix)
      lastFile.foreach(lf => builder.startAfter(lf.file))

      val listObjectsV2Response = s3Client.listObjectsV2(builder.build())
      val objects = listObjectsV2Response
        .contents()
        .asScala
        .filterNot(AwsS3StorageFilter.filterOut)
        .filter(_.size() > 0)
        .map(o => S3FileMetadata(o.key(), o.lastModified()))
        .filter(md => extensionFilter.forall(_.filter(md)))

      processAsKey(
        bucket,
        prefix,
        objects.toSeq,
      )

    }.toEither.leftMap {
      ex: Throwable => FileListError(ex, bucket, prefix)
    }

  override def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[S3FileMetadata]]] =
    listRecursive[ListOfMetadataResponse[S3FileMetadata], S3FileMetadata](bucket,
                                                                          prefix,
                                                                          processObjectsAsFileMeta[S3FileMetadata],
    )

  def listKeysRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfKeysResponse[S3FileMetadata]]] =
    listRecursive[ListOfKeysResponse[S3FileMetadata], String](bucket, prefix, processAsKey[S3FileMetadata])

  private def listRecursive[LR <: ListResponse[T, S3FileMetadata], T](
    bucket:    String,
    prefix:    Option[String],
    processFn: (String, Option[String], Seq[S3FileMetadata]) => Option[LR],
  ): Either[FileListError, Option[LR]] = {
    logger.debug(s"[{}] List path {}:{}", connectorTaskId.show, bucket, prefix)
    Try {
      val options = ListObjectsV2Request
        .builder()
        .bucket(bucket)
        .prefix(prefix.getOrElse(""))
        .build()

      val pagReq = s3Client.listObjectsV2Paginator(options)

      processFn(
        bucket,
        prefix,
        pagReq.iterator().asScala.flatMap(
          _.contents().asScala.filterNot(AwsS3StorageFilter.filterOut)
            .filter(_.size() > 0)
            .toSeq.map(o => S3FileMetadata(o.key(), o.lastModified())).filter(md =>
              extensionFilter.forall(_.filter(md)),
            ),
        ).toSeq,
      )
    }.toEither.leftMap {
      ex: Throwable => FileListError(ex, bucket, prefix)
    }
  }

  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, Unit] = {
    logger.debug(s"[{}] AWS Uploading file from local {} to s3 {}:{}", connectorTaskId.show, source, bucket, path)
    for {
      file <- source.validate.toEither
      _ <- Try {
        s3Client.putObject(PutObjectRequest.builder()
                             .bucket(bucket)
                             .key(path)
                             .contentLength(file.length())
                             .build(),
                           file.toPath,
        )
        logger.debug(s"[{}] Completed upload from local {} to s3 {}:{}", connectorTaskId.show, source, bucket, path)
      }.toEither.leftMap { ex: Throwable =>
        logger.error(s"[{}] Failed upload from local {} to s3 {}:{}", connectorTaskId.show, source, bucket, path, ex)
        UploadFailedError(ex, source.file)
      }
    } yield ()
  }

  override def pathExists(bucket: String, path: String): Either[FileLoadError, Boolean] = {

    logger.debug(s"[{}] Path exists? {}:{}", connectorTaskId.show, bucket, path)

    Try(
      s3Client.listObjectsV2(
        ListObjectsV2Request.builder().bucket(bucket).prefix(path).build(),
      ).keyCount().toInt,
    ).toEither match {
      case Left(_: NoSuchKeyException) => false.asRight
      case Left(other) => FileLoadError(other, path).asLeft
      case Right(keyCount: Int) => (keyCount > 0).asRight
    }
  }

  private def getBlobInner(bucket: String, path: String): ResponseInputStream[GetObjectResponse] = {
    val request = GetObjectRequest
      .builder()
      .bucket(bucket)
      .key(path)
      .build()
    s3Client
      .getObject(
        request,
      )
  }

  override def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream] =
    Try(getBlobInner(bucket, path)).toEither.leftMap(FileLoadError(_, path))

  override def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata] =
    Try {
      val response = s3Client
        .headObject(
          HeadObjectRequest
            .builder()
            .bucket(bucket)
            .key(path)
            .build(),
        )
      ObjectMetadata(response.contentLength(), response.lastModified())
    }.toEither.leftMap(ex => FileLoadError(ex, path))

  override def close(): Unit = s3Client.close()

  private def batchDeleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] = Try {
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

  private def loopDeleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] = Try {
    for (f <- files) {
      s3Client.deleteObject(
        DeleteObjectRequest
          .builder()
          .bucket(bucket)
          .key(f)
          .build(),
      )
    }
  } match {
    case Failure(ex) => FileDeleteError(ex, files.mkString(" - ")).asLeft
    case Success(_)  => ().asRight
  }

  override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] =
    if (files.isEmpty) {
      ().asRight
    } else if (!batchDelete) {
      loopDeleteFiles(bucket, files)
    } else {
      batchDeleteFiles(bucket, files)
    }

  override def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String] =
    getBlob(bucket, path).flatMap { blob =>
      Try(
        IOUtils.toString(blob, Charset.forName("UTF-8")),
      ).toEither.leftMap(ex => FileLoadError(ex, path))
    }

  override def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit] = {
    logger.debug(s"[{}] Uploading file from data string ({}) to s3 {}:{}", connectorTaskId.show, data, bucket, path)

    for {
      content <- data.validate.toEither
      _ <- Try {
        s3Client.putObject(
          PutObjectRequest
            .builder()
            .bucket(bucket)
            .key(path)
            .contentLength(content.length.toLong)
            .build(),
          RequestBody.fromString(content, Charset.forName("UTF-8")),
        )
        logger.debug(s"[{}] Completed upload from data string ({}) to s3 {}:{}",
                     connectorTaskId.show,
                     data,
                     bucket,
                     path,
        )
      }.toEither.leftMap {
        ex =>
          logger.error(s"[{}] Failed upload from data string ({}) to s3 {}:{}",
                       connectorTaskId.show,
                       data,
                       bucket,
                       path,
                       ex,
          )
          FileCreateError(ex, content)
      }
    } yield ()
  }

  override def seekToFile(bucket: String, fileName: String, lastModified: Option[Instant]): Option[S3FileMetadata] =
    lastModified
      .map(lmValue => S3FileMetadata(fileName, lmValue))
      .orElse(getMetadata(bucket, fileName).map(oMeta => S3FileMetadata(fileName, oMeta.lastModified)).toOption)

  /**
    * Gets the system name for use in log messages.
    *
    * @return
    */
  override def system(): String = "S3"

  override def mvFile(
    oldBucket: String,
    oldPath:   String,
    newBucket: String,
    newPath:   String,
  ): Either[FileMoveError, Unit] = {
    val headObjectRequest = HeadObjectRequest.builder().bucket(oldBucket).key(oldPath).build()
    Try(s3Client.headObject(headObjectRequest)) match {
      case Failure(ex: NoSuchKeyException) =>
        logger.warn("Object ({}/{}) doesn't exist to move", oldBucket, oldPath, ex)
        ().asRight
      case Failure(ex) =>
        logger.error("Object ({}/{}) could not be retrieved", ex)
        FileMoveError(ex, oldPath, newPath).asLeft
      case Success(_) =>
        Try {
          s3Client.copyObject(
            CopyObjectRequest.builder().sourceKey(oldPath).destinationKey(newPath).sourceBucket(
              oldBucket,
            ).destinationBucket(
              newBucket,
            ).build(),
          )
          s3Client.deleteObject(DeleteObjectRequest.builder().bucket(oldBucket).key(oldPath).build())
        }.toEither.leftMap(FileMoveError(_, oldPath, newPath)).void
    }
  }

  /**
    * Creates a directory in the specified S3 bucket if it does not already exist.
    *
    * @param bucket The name of the S3 bucket.
    * @param path The path of the directory to create.
    * @return Either a FileCreateError if the directory could not be created,
    *         or Unit if the directory was created successfully or already exists.
    */
  override def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit] = Try {
    def ensureEndsWithSlash(input: String): String =
      if (input.endsWith("/")) input else input + "/"

    val putObjectRequest = PutObjectRequest
      .builder()
      .ifNoneMatch("*")
      .bucket(bucket)
      .key(ensureEndsWithSlash(path))
      .contentLength(0)
      .build()

    s3Client.putObject(putObjectRequest, RequestBody.empty())
  }
    .toEither
    .void
    // If the object already exists, the "ifNoneMatch" condition will fail, triggering this recovery clause
    .recover {
      case ex: S3Exception if "PreconditionFailed".equals(ex.awsErrorDetails().errorCode()) =>
        ()
    }
    .leftMap(ex => FileCreateError(ex, "empty object file"))

}
