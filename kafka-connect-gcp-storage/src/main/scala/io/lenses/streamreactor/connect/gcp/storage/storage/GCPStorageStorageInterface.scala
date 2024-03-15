/*
 * Copyright 2017-2024 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.gcp.storage.storage

import cats.implicits._
import cats.implicits.catsSyntaxEitherId
import cats.implicits.toShow
import com.google.api.gax.paging.Page
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobListOption
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError

import java.io.InputStream
import java.nio.channels.Channels
import java.nio.file.Files
import java.time.Instant
import scala.annotation.tailrec
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Try

class GCPStorageStorageInterface(connectorTaskId: ConnectorTaskId, storage: Storage, avoidReumableUpload: Boolean)
    extends StorageInterface[GCPStorageFileMetadata]
    with LazyLogging {
  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, Unit] = {
    logger.debug(s"[{}] GCP Uploading file from local {} to Storage {}:{}", connectorTaskId.show, source, bucket, path)
    for {
      file <- source.validate.toEither
      _ <- Try {
        val blobId   = BlobId.of(bucket, path)
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        if (avoidReumableUpload) {
          storage.create(blobInfo, Files.readAllBytes(file.toPath))
        } else {
          storage.createFrom(blobInfo, file.toPath)
          //Using(new FileInputStream(file)){
          //  is =>  {
          //    storage.createFrom(blobInfo, is)
          //  }
          //}(_.close())
        }
        logger.info(s"[{}] Completed upload from local {} to Storage {}:{}", connectorTaskId.show, source, bucket, path)
      }.toEither.leftMap { ex: Throwable =>
        logger.error(s"[{}] Failed upload from local {} to Storage {}:{}",
                     connectorTaskId.show,
                     source,
                     bucket,
                     path,
                     ex,
        )
        UploadFailedError(ex, source.file)
      }
    } yield ()

  }

  override def close(): Unit = Try(storage.close()).getOrElse(())

  private def usingBlob[X](bucket: String, path: String)(f: Blob => X): Either[FileLoadError, X] =
    Try {
      val blob = storage.get(BlobId.of(bucket, path))
      f(blob)
    }.toEither.leftMap {
      FileLoadError(_, path)
    }

  override def pathExists(bucket: String, path: String): Either[FileLoadError, Boolean] =
    usingBlob[Boolean](bucket, path) {
      blob =>
        blob.exists().booleanValue()
    }

  override def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream] =
    usingBlob[InputStream](bucket, path) {
      blob =>
        val reader = blob.reader()
        Channels.newInputStream(reader)
    }

  override def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String] =
    usingBlob[String](bucket, path) {
      blob =>
        new String(blob.getContent())
    }

  override def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata] =
    usingBlob[ObjectMetadata](bucket, path) {
      blob =>
        ObjectMetadata(blob.getSize, blob.getCreateTimeOffsetDateTime.toInstant)
    }

  override def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit] = {
    logger.debug(s"[{}] Uploading file from data string ({}) to Storage {}:{}",
                 connectorTaskId.show,
                 data,
                 bucket,
                 path,
    )
    for {
      content <- data.validate.toEither
      _ <- Try {
        val blobId   = BlobId.of(bucket, path)
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        val _        = storage.create(blobInfo, content.getBytes)
        logger.debug(s"[{}] Completed upload from data string ({}) to Storage {}:{}",
                     connectorTaskId.show,
                     data,
                     bucket,
                     path,
        )
      }.toEither.leftMap { ex: Throwable =>
        logger.error(s"[{}] Failed upload from data string ({}) to Storage {}:{}",
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

  override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] =
    if (files.nonEmpty) {
      val blobIds = files.map(BlobId.of(bucket, _))
      Try(storage.delete(blobIds: _*)).toEither.leftMap {
        ex: Throwable =>
          logger.error(s"[{}] Failed to delete files {} from bucket {}", connectorTaskId.show, files, bucket, ex)
          FileDeleteError(ex, files.mkString(","))
      }.map {
        _ => ()
      }
    } else {
      ().asRight
    }

  private def listFilesRecursive[RW, E](
    bucket:                String,
    prefix:                Option[String],
    fnResultWrapperCreate: (Seq[E], Option[Instant]) => RW,
    fnListElementCreate:   Blob => E,
  ): Either[FileListError, Option[RW]] = {

    @tailrec
    def listKeysRecursiveHelper(
      maybePage:       Option[Page[Blob]],
      accumulatedKeys: Seq[E],
      latestCreated:   Option[Instant],
    ): Option[RW] =
      maybePage match {
        case None =>
          Option.when(accumulatedKeys.nonEmpty)(fnResultWrapperCreate(accumulatedKeys, latestCreated))
        case Some(page: Page[Blob]) =>
          val pages = page.getValues.asScala
          val newKeys: Seq[E] = accumulatedKeys ++ pages.map(p => fnListElementCreate(p))
          val newLatestCreated = pages.lastOption.map(_.getCreateTimeOffsetDateTime.toInstant).orElse(latestCreated)
          listKeysRecursiveHelper(Option(page.getNextPage), newKeys, newLatestCreated)
      }

    Try {
      val initialPage: Page[Blob] = storage.list(bucket, prefix.map(BlobListOption.prefix).toSeq: _*)
      listKeysRecursiveHelper(Option(initialPage), Nil, Option.empty)
    }.toEither.leftMap(FileListError(_, bucket, prefix))
  }

  override def listKeysRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfKeysResponse[GCPStorageFileMetadata]]] =
    listFilesRecursive(
      bucket,
      prefix,
      (accumulatedKeys: Seq[String], latestCreated: Option[Instant]) =>
        ListOfKeysResponse[GCPStorageFileMetadata](
          bucket,
          prefix,
          accumulatedKeys,
          GCPStorageFileMetadata(accumulatedKeys.last, latestCreated.get),
        ),
      _.getName,
    )

  override def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[GCPStorageFileMetadata]]] =
    listFilesRecursive(
      bucket,
      prefix,
      (accumulatedKeys: Seq[GCPStorageFileMetadata], _: Option[Instant]) =>
        ListOfMetadataResponse[GCPStorageFileMetadata](
          bucket,
          prefix,
          accumulatedKeys,
          accumulatedKeys.last,
        ),
      p => GCPStorageFileMetadata(p.getName, p.getCreateTimeOffsetDateTime.toInstant),
    )

  override def list(
    bucket:     String,
    prefix:     Option[String],
    lastFile:   Option[GCPStorageFileMetadata],
    numResults: Int,
  ): Either[FileListError, Option[ListOfKeysResponse[GCPStorageFileMetadata]]] = {

    val blobListOptions =
      Seq(BlobListOption.pageSize(numResults.toLong)) ++
        lastFile.map(md => BlobListOption.startOffset(md.file)).toSeq ++
        prefix.map(pf => BlobListOption.prefix(pf)).toSeq

    Try(storage.list(bucket, blobListOptions: _*)).toEither match {
      case Left(ex) => FileListError(ex, bucket, prefix).asLeft
      case Right(page) =>
        val pageValues = page.getValues.asScala
        val keys       = pageValues.map(_.getName).filterNot(f => lastFile.map(_.file).contains(f)).toSeq
        logger.trace(
          s"[${connectorTaskId.show}] Last file: $lastFile, Prefix: $prefix Page: ${pageValues.map(_.getName)}, Keys: $keys",
        )
        pageValues
          .lastOption
          .map(value =>
            ListOfKeysResponse[GCPStorageFileMetadata](
              bucket,
              prefix,
              keys,
              GCPStorageFileMetadata(value.getName, value.getCreateTimeOffsetDateTime.toInstant),
            ),
          ).asRight
    }

  }

  override def seekToFile(
    bucket:       String,
    fileName:     String,
    lastModified: Option[Instant],
  ): Option[GCPStorageFileMetadata] =
    lastModified
      .map(lmValue => GCPStorageFileMetadata(fileName, lmValue))
      .orElse(getMetadata(bucket, fileName).map(oMeta => GCPStorageFileMetadata(fileName, oMeta.lastModified)).toOption)

}
