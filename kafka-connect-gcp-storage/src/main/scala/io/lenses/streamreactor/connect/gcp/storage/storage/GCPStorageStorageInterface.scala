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
import com.google.cloud.storage.Storage.BlobSourceOption
import com.google.cloud.storage.Storage.BlobTargetOption
import com.google.cloud.storage.StorageException
import com.typesafe.scalalogging.LazyLogging
import io.circe.Encoder
import io.circe.syntax.EncoderOps
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoOverwriteExistingObject
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectProtection
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.storage.ExtensionFilter
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.GeneralFileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.storage.FileNotFoundError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.PathError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError

import java.io.InputStream
import java.nio.channels.Channels
import java.nio.file.Files
import java.time.Instant
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsScala
import scala.util.Failure
import scala.util.Success
import scala.util.Try

class GCPStorageStorageInterface(
  connectorTaskId:     ConnectorTaskId,
  storage:             Storage,
  avoidReumableUpload: Boolean,
  extensionFilter:     Option[ExtensionFilter],
) extends StorageInterface[GCPStorageFileMetadata]
    with LazyLogging {
  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] = {
    logger.debug(s"[{}] GCP Uploading file from local {} to Storage {}:{}", connectorTaskId.show, source, bucket, path)
    for {
      file <- source.validate.toEither
      eTag <- Try {
        val blobId   = BlobId.of(bucket, path)
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        val blob = if (avoidReumableUpload) {
          storage.create(blobInfo, Files.readAllBytes(file.toPath))
        } else {
          storage.createFrom(blobInfo, file.toPath)
        }
        logger.debug(s"[{}] Completed upload from local {} to Storage {}:{}",
                     connectorTaskId.show,
                     source,
                     bucket,
                     path,
        )
        String.valueOf(blob.getGeneration)
      }.toEither.leftMap { ex: Throwable =>
        logger.error(s"[{}] Failed upload from local {} to Storage {}:{}. Reason:{}",
                     connectorTaskId.show,
                     source,
                     bucket,
                     path,
                     ex,
        )
        UploadFailedError(ex, source.file)
      }
    } yield eTag

  }

  override def close(): Unit = Try(storage.close()).getOrElse(())

  private def usingBlob[X](bucket: String, path: String)(f: Option[Blob] => X): Either[FileLoadError, X] =
    Try {
      val blob     = storage.get(BlobId.of(bucket, path))
      val optiBlob = Option(blob)
      f(optiBlob)
    }.toEither.leftMap {
      case se: StorageException if se.getCode == 404 =>
        FileNotFoundError(se, path)
      case other =>
        GeneralFileLoadError(other, path)
    }

  override def pathExists(bucket: String, path: String): Either[PathError, Boolean] =
    usingBlob[Boolean](bucket, path) {
      maybeBlob =>
        maybeBlob.nonEmpty && maybeBlob.exists(blob => blob.exists())
    }.leftMap(fileLoadError => PathError(fileLoadError.exception, fileLoadError.fileName))

  override def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream] =
    usingBlob[InputStream](bucket, path) {
      case Some(blob) =>
        val reader = blob.reader()
        Channels.newInputStream(reader)
      case None =>
        throw new IllegalStateException("No/null blob found (file doesn't exist?)")
    }

  override def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String] =
    usingBlob[String](bucket, path) {
      case Some(blob) =>
        new String(blob.getContent())
      case None =>
        throw new IllegalStateException("No/null blob found (file doesn't exist?)")
    }

  override def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata] =
    usingBlob[ObjectMetadata](bucket, path) {
      case Some(blob) =>
        ObjectMetadata(blob.getSize, blob.getCreateTimeOffsetDateTime.toInstant)
      case None =>
        throw new IllegalStateException("No/null blob found (file doesn't exist?)")
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
        val blobId = BlobId.of(bucket, path)
        val blobInfo: BlobInfo = BlobInfo.newBuilder(blobId).build()
        storage.create(blobInfo, content.getBytes)
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

  override def writeBlobToFile[O](
    bucket:           String,
    path:             String,
    objectProtection: ObjectProtection[O],
  )(
    implicit
    encoder: Encoder[O],
  ): Either[UploadError, ObjectWithETag[O]] = {
    val content = objectProtection.wrappedObject.asJson.noSpaces

    Try {
      logger.debug(
        s"[{}] Uploading file from json object ({}) to Storage {}:{}",
        connectorTaskId.show,
        objectProtection.wrappedObject,
        bucket,
        path,
      )

      val blobId = BlobId.of(bucket, path)
      val blobInfo: BlobInfo = BlobInfo.newBuilder(blobId).build()
      val created = objectProtection match {
        case NoOverwriteExistingObject(_) =>
          storage.create(blobInfo, content.getBytes, BlobTargetOption.generationMatch(0L))
        case ObjectWithETag(_, eTag) =>
          storage.create(blobInfo, content.getBytes, BlobTargetOption.generationMatch(eTag.toLong))
        case _ => storage.create(blobInfo, content.getBytes)
      }
      logger.debug(
        s"[{}] Completed upload from data string ({}) to Storage {}:{}",
        connectorTaskId.show,
        objectProtection.wrappedObject,
        bucket,
        path,
      )
      new ObjectWithETag[O](objectProtection.wrappedObject, String.valueOf(created.getGeneration))
    }.toEither.leftMap { ex: Throwable =>
      logger.error(
        s"[{}] Failed upload from data string ({}) to Storage {}:{}",
        connectorTaskId.show,
        objectProtection.wrappedObject,
        bucket,
        path,
        ex,
      )
      FileCreateError(ex, content)
    }
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
          val newKeys: Seq[E] =
            accumulatedKeys ++ pages.filter(p => extensionFilter.forall(_.filter(p.getName))).map(p =>
              fnListElementCreate(p),
            )
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
      (accumulatedKeys: Seq[String], latestCreated: Option[Instant]) => {
        val filteredKeys = filterKeys(accumulatedKeys)
        ListOfKeysResponse[GCPStorageFileMetadata](
          bucket,
          prefix,
          filteredKeys,
          GCPStorageFileMetadata(filteredKeys.last, latestCreated.get),
        )
      },
      _.getName,
    )

  override def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[GCPStorageFileMetadata]]] =
    listFilesRecursive(
      bucket,
      prefix,
      (accumulatedKeys: Seq[GCPStorageFileMetadata], _: Option[Instant]) => {
        val filteredKeys: Seq[GCPStorageFileMetadata] = filterKeys(accumulatedKeys)
        ListOfMetadataResponse[GCPStorageFileMetadata](
          bucket,
          prefix,
          filteredKeys,
          filteredKeys.last,
        )
      },
      p => GCPStorageFileMetadata(p.getName, p.getCreateTimeOffsetDateTime.toInstant),
    )

  trait Filterable[T] {
    def filter(extensionFilter: ExtensionFilter, t: T): Boolean
  }

  implicit val stringFilterable: Filterable[String] = (extensionFilter: ExtensionFilter, t: String) =>
    extensionFilter.filter(t)

  implicit val metadataFilterable: Filterable[GCPStorageFileMetadata] =
    (extensionFilter: ExtensionFilter, t: GCPStorageFileMetadata) => extensionFilter.filter(t.file)

  private def filterKeys[T: Filterable](accumulatedKeys: Seq[T]): Seq[T] = {
    val filterable = implicitly[Filterable[T]]
    extensionFilter
      .map(ex => accumulatedKeys.filter(filterable.filter(ex, _)))
      .getOrElse(accumulatedKeys)
  }

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
        val keys       = filterKeys(pageValues.map(_.getName).toSeq).filterNot(f => lastFile.map(_.file).contains(f))
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

  /**
    * Gets the system name for use in log messages.
    *
    * @return
    */
  override def system(): String = "GCP Storage"

  override def mvFile(
    oldBucket: String,
    oldPath:   String,
    newBucket: String,
    newPath:   String,
    maybeEtag: Option[String],
  ): Either[FileMoveError, Unit] = {

    val sourceBlobId = BlobId.of(oldBucket, oldPath)
    val sourceBlob   = Try(storage.get(sourceBlobId)).map(Option(_)).map(_.filter(_.exists()))
    sourceBlob match {
      case Success(None) =>
        logger.warn("Object ({}/{}) doesn't exist to move", oldBucket, oldPath)
        ().asRight
      case Failure(ex) =>
        logger.error("Object ({}/{}) could not be retrieved", ex)
        FileMoveError(ex, oldPath, newPath).asLeft
      case Success(Some(_: Blob)) =>
        Try {
          val destinationBlobId = BlobId.of(newBucket, newPath)
          val destinationBlob   = Option(storage.get(newBucket, newPath))
          val precondition: Storage.BlobTargetOption = decideMovePrecondition(destinationBlob)

          storage.copy(
            Storage.CopyRequest.newBuilder()
              .setSource(sourceBlobId)
              .setTarget(destinationBlobId, precondition)
              .build(),
          )

          // Delete the original blob to complete the move operation
          storage.delete(sourceBlobId)
        }.toEither.leftMap(FileMoveError(_, oldPath, newPath)).void
    }
  }

  /**
    * Decides the precondition for moving a blob based on the existence of the destination blob.
    *
    * @param destinationBlob An optional Blob object representing the destination blob.
    * @return A Storage.BlobTargetOption indicating the precondition for the move operation.
    *         If the destination blob does not exist, returns Storage.BlobTargetOption.doesNotExist().
    *         If the destination blob exists, returns Storage.BlobTargetOption.generationMatch() with the blob's generation.
    */
  private def decideMovePrecondition(destinationBlob: Option[Blob]): Storage.BlobTargetOption =
    destinationBlob match {
      case None =>
        Storage.BlobTargetOption.doesNotExist()
      case Some(blob) =>
        Storage.BlobTargetOption.generationMatch(blob.getGeneration)
    }

  override def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit] = ().asRight

  override def getBlobAsStringAndEtag(bucket: String, path: String): Either[FileLoadError, (String, String)] =
    usingBlob[(String, String)](bucket, path) {
      case Some(blob) =>
        (new String(blob.getContent()), blob.getGeneration.toString)
      case None =>
        throw new StorageException(404, "No/null blob found (file doesn't exist?)")
    }

  override def deleteFile(bucket: String, file: String, eTag: String): Either[FileDeleteError, Unit] = {
    val blobId = BlobId.of(bucket, file)
    Try(
      storage.delete(blobId, BlobSourceOption.generationMatch(eTag.toLong)),
    ).toEither.leftMap {
      ex: Throwable =>
        logger.error(s"[{}] Failed to delete files {} from bucket {}", connectorTaskId.show, file, bucket, ex)
        FileDeleteError(ex, file)
    }.map {
      _ => ()
    }
  }

}
