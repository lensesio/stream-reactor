/*
 * Copyright 2017-2026 Lenses.io Ltd
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
package io.lenses.streamreactor.connect.datalake.storage
import cats.implicits._
import com.azure.core.http.rest.PagedIterable
import com.azure.core.util.Context
import com.azure.storage.common.ParallelTransferOptions
import com.azure.storage.file.datalake.DataLakeFileClient
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.azure.core.http.HttpHeaderName
import com.azure.storage.file.datalake.models.DataLakeRequestConditions
import com.azure.storage.file.datalake.models.DataLakeStorageException
import com.azure.storage.file.datalake.models.FileReadResponse
import com.azure.storage.file.datalake.models.ListPathsOptions
import com.azure.storage.file.datalake.models.PathHttpHeaders
import com.azure.storage.file.datalake.models.PathItem
import com.azure.storage.file.datalake.options.DataLakePathDeleteOptions
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
import io.lenses.streamreactor.connect.cloud.common.storage._
import io.lenses.streamreactor.connect.datalake.storage.adaptors.DatalakeContinuingPageIterableAdaptor
import io.lenses.streamreactor.connect.datalake.storage.adaptors.DatalakePageIterableAdaptor

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.time.Instant
import scala.util.Try
import scala.util.Using

class DatalakeStorageInterface(
  connectorTaskId: ConnectorTaskId,
  client:          DataLakeServiceClient,
  uploadOptions:   DatalakeUploadOptions = DatalakeUploadOptions.default,
) extends StorageInterface[DatalakeFileMetadata]
    with LazyLogging {

  private val parallelTransferOptions: ParallelTransferOptions = uploadOptions.toParallelTransferOptions

  private def parentDirectory(path: String): Option[String] = {
    val idx = path.lastIndexOf('/')
    if (idx > 0) Some(path.substring(0, idx)) else None
  }

  override def list(
    bucket:        String,
    prefix:        Option[String],
    maybeLastFile: Option[DatalakeFileMetadata],
    numResults:    Int,
  ): Either[FileListError, Option[ListOfKeysResponse[DatalakeFileMetadata]]] =
    Try {
      val cont = {
        for {
          lastFile     <- maybeLastFile
          continuation <- lastFile.continuation
        } yield continuation
      }

      val iter: PagedIterable[PathItem] = cont.map(_.pagedIterable).getOrElse {
        val bucketClient     = client.getFileSystemClient(bucket)
        val listPathsOptions = new ListPathsOptions().setMaxResults(numResults)
        prefix.foreach(listPathsOptions.setPath)
        bucketClient.listPaths(listPathsOptions, null)
      }

      val token = cont.map(_.lastContinuationToken)

      val fileName = maybeLastFile.map(_.file)

      val (maybeToken, results) =
        DatalakeContinuingPageIterableAdaptor.getResults(iter, token, fileName, numResults)
      toListOfKeys(bucket, prefix, maybeToken.map(Continuation(iter, _)), results)

    }.toEither.leftMap {
      ex: Throwable => FileListError(ex, bucket, prefix)
    }

  private def toListOfKeys(
    bucket:       String,
    prefix:       Option[String],
    continuation: Option[Continuation],
    results:      Seq[PathItem],
  ): Option[ListOfKeysResponse[DatalakeFileMetadata]] =
    Option.when(results.nonEmpty)(
      ListOfKeysResponse[DatalakeFileMetadata](
        bucket,
        prefix,
        results.map(_.getName),
        DatalakeFileMetadata(
          file         = results.last.getName,
          lastModified = results.last.getLastModified.toInstant,
          continuation = continuation,
        ),
      ),
    )

  override def close(): Unit = ()

  override def pathExists(bucket: String, path: String): Either[PathError, Boolean] =
    Try(client.getFileSystemClient(bucket).getFileClient(path).exists().booleanValue()).toEither.recover {
      case ex: DataLakeStorageException if ex.getStatusCode == 404 =>
        false
      case ex: DataLakeStorageException if ex.getStatusCode == 403 =>
        logger.warn(
          "ADLS returned 403 for pathExists on {}/{}; treating as absent " +
            "(HNS with SharedKey auth returns 403 for non-existent paths)",
          bucket,
          path,
        )
        false
    }.leftMap(PathError(
      _,
      path,
    ))

  override def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[DatalakeFileMetadata]]] =
    Try {
      val bucketClient     = client.getFileSystemClient(bucket)
      val listPathsOptions = new ListPathsOptions().setRecursive(true)
      prefix.foreach(listPathsOptions.setPath)
      val iter    = bucketClient.listPaths(listPathsOptions, null)
      val results = DatalakePageIterableAdaptor.getResults(iter)
      processObjectsAsFileMeta(
        bucket,
        prefix,
        results.map(pi =>
          DatalakeFileMetadata(
            file         = pi.getName,
            lastModified = pi.getLastModified.toInstant,
            continuation = None,
            size         = Option(pi.getContentLength).map(_.longValue()),
          ),
        ),
      )
    }.toEither.recover {
      case ex: DataLakeStorageException if ex.getStatusCode == 404 =>
        Option.empty
    }.leftMap {
      ex: Throwable => FileListError(ex, bucket, prefix)
    }

  override def listKeysRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfKeysResponse[DatalakeFileMetadata]]] =
    Try {
      val bucketClient     = client.getFileSystemClient(bucket)
      val listPathsOptions = new ListPathsOptions().setRecursive(true)
      prefix.foreach(listPathsOptions.setPath)
      val iter = bucketClient.listPaths(listPathsOptions, null)

      val results = DatalakePageIterableAdaptor.getResults(iter)
      toListOfKeys(bucket, prefix, none, results)
    }
      .toEither.recover {
        case ex: DataLakeStorageException if ex.getStatusCode == 404 =>
          Option.empty
      }.leftMap {
        ex: Throwable => FileListError(ex, bucket, prefix)
      }

  override def seekToFile(
    bucket:       String,
    fileName:     String,
    lastModified: Option[Instant],
  ): Option[DatalakeFileMetadata] = throw new NotImplementedError("Required for source")

  override def getBlob(bucket: String, path: String): Either[GeneralFileLoadError, InputStream] =
    throw new NotImplementedError("Required for source")

  override def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String] =
    Try {
      Using.resource(new ByteArrayOutputStream()) {
        baos =>
          client.getFileSystemClient(bucket).getFileClient(path).read(baos)
          new String(baos.toByteArray)
      }
    }.toEither.leftMap {
      case ex: DataLakeStorageException if ex.getStatusCode == 404 =>
        FileNotFoundError(ex, path)
      case ex =>
        GeneralFileLoadError(ex, path)
    }

  override def getMetadata(bucket: String, path: String): Either[GeneralFileLoadError, ObjectMetadata] =
    throw new NotImplementedError("Required for source")

  private def createFile(bucket: String, path: String): DataLakeFileClient =
    client.getFileSystemClient(bucket).createFile(path, true)

  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] = {
    logger.debug(s"[{}] Uploading file from local {} to Data Lake {}:{}", connectorTaskId.show, source, bucket, path)
    def tryUploadFile(filePath: String, localFilePath: String): Either[Throwable, String] = Try {
      val createFileClient: DataLakeFileClient = createFile(bucket, filePath)
      val response = createFileClient.uploadFromFileWithResponse(
        localFilePath,
        parallelTransferOptions,
        null,                            // PathHttpHeaders
        null,                            // Metadata
        new DataLakeRequestConditions(), // RequestConditions to avoid overwriting
        null,                            // Timeout
        null,                            // Context
      )
      response.getValue.getETag
    }.toEither

    for {
      file <- source.validate.toEither
      eTag <- tryUploadFile(path, file.getPath) match {
        case Right(tag) =>
          logger.debug(s"[{}] Completed upload from local {} to Data Lake {}:{}",
                       connectorTaskId.show,
                       source,
                       bucket,
                       path,
          )
          Right(tag)
        case Left(dse: DataLakeStorageException)
            if dse.getStatusCode == 404 || Option(dse.getMessage).exists(_.contains("PathNotFound")) =>
          parentDirectory(path) match {
            case Some(dir) =>
              createDirectoryIfNotExists(bucket, dir) match {
                case Left(err) => Left(UploadFailedError(err.exception, file))
                case Right(_)  => tryUploadFile(path, file.getPath).leftMap(th => UploadFailedError(th, file))
              }
            case None => Left(UploadFailedError(dse, file))
          }
        case Left(other) =>
          logger.error(s"[{}] Failed upload from local {} to Data Lake {}:{}",
                       connectorTaskId.show,
                       source,
                       bucket,
                       path,
                       other,
          )
          Left(UploadFailedError(other, file))
      }
    } yield eTag

  }

  override def writeStringToFile(bucket: String, path: String, data: UploadableString): Either[UploadError, Unit] = {
    logger.debug(
      s"[${connectorTaskId.show}] Uploading file from data string ({${data.data}}) to datalake $bucket:$path",
    )
    for {
      content <- data.validate.toEither
      _ <- Try {
        val createFileClient: DataLakeFileClient = createFile(bucket, path)
        val bytes = content.getBytes
        Using.resource(new ByteArrayInputStream(bytes)) {
          bais =>
            createFileClient.append(bais, 0, bytes.length.toLong)
        }
        createFileClient.flush(bytes.length.toLong, true)

        logger.debug(s"[${connectorTaskId.show}] Completed upload from data string ($data) to datalake $bucket:$path")
      }.toEither.leftMap {
        ex =>
          logger.error(s"[{connectorTaskId.show}] Failed upload from data string ($data) to datalake $bucket:$path", ex)
          FileCreateError(ex, content)
      }
    } yield ()
  }

  override def deleteFiles(bucket: String, files: Seq[String]): Either[FileDeleteError, Unit] = for {
    cli <- Try(client.getFileSystemClient(bucket)).toEither.leftMap(FileDeleteError(
      _,
      files.headOption.getOrElse("No file"),
    ))
    _ <- files.map {
      file =>
        Try {
          cli.deleteFileIfExists(file)
          ()
        }.toEither.leftMap(FileDeleteError(_, file))
    }.sequence
  } yield ()

  /**
   * Gets the system name for use in log messages.
   *
   * @return
   */
  override def system(): String = "Azure Datalake"

  override def mvFile(
    oldBucket: String,
    oldPath:   String,
    newBucket: String,
    newPath:   String,
    maybeEtag: Option[String],
  ): Either[FileMoveError, Unit] = {
    val conditions = maybeEtag.map(new DataLakeRequestConditions().setIfMatch(_))
    def tryRenamePath(): Either[Throwable, Unit] = Try {
      client.getFileSystemClient(oldBucket).getFileClient(oldPath)
        .renameWithResponse(
          newBucket,
          newPath,
          conditions.orNull,
          null,
          null,
          Context.NONE,
        )
      ()
    }.toEither

    val first: Either[FileMoveError, Unit] = tryRenamePath() match {
      case Right(_) => Right(())
      case Left(dse: DataLakeStorageException)
          if dse.getStatusCode == 404 || Option(dse.getMessage).exists(
            _.contains("RenameDestinationParentPathNotFound"),
          ) =>
        parentDirectory(newPath) match {
          case Some(dir) =>
            createDirectoryIfNotExists(newBucket, dir) match {
              case Left(err) => Left(FileMoveError(err.exception, oldPath, newPath))
              case Right(_)  => tryRenamePath().leftMap(th => FileMoveError(th, oldPath, newPath))
            }
          case None => Left(FileMoveError(dse, oldPath, newPath))
        }
      case Left(other) => Left(FileMoveError(other, oldPath, newPath))
    }

    first match {
      case r @ Right(_)      => r
      case Left(originalErr) =>
        // A 403 on the rename means the operation was auth-rejected, not completed.
        // The source is still present — idempotence check is only meaningful for
        // errors that could represent a crashed-after-commit scenario (e.g. network
        // failure after a successful rename). Skip it entirely for 403.
        originalErr.exception match {
          case dse: DataLakeStorageException if dse.getStatusCode == 403 =>
            Left(originalErr)
          case _ =>
            // Idempotence fallback (mirrors AwsS3StorageInterface.mvFile and
            // GCPStorageStorageInterface.mvFile). When the rename fails but the source is
            // verifiably absent and the destination is verifiably present, treat the call
            // as an idempotent replay of an already-completed move. This is essential for
            // crash-after-Copy-before-lock-update recovery: the recovery handler
            // (PendingOperationsProcessors) will replay CopyOperation on restart, and
            // without this branch the replay would escalate to FatalCloudSinkError and
            // produce a deterministic restart loop on the affected partition.
            val sourceMissing = pathExists(oldBucket, oldPath).fold(_ => false, !_)
            if (!sourceMissing) Left(originalErr)
            else pathExists(newBucket, newPath) match {
              case Right(true) =>
                logger.warn(
                  "Object ({}/{}) missing but destination ({}/{}) exists; treating mvFile as idempotent success",
                  oldBucket,
                  oldPath,
                  newBucket,
                  newPath,
                )
                ().asRight
              case Right(false) =>
                Left(
                  FileMoveError(
                    new IllegalStateException(
                      s"Source $oldBucket/$oldPath and destination $newBucket/$newPath both missing",
                    ),
                    oldPath,
                    newPath,
                  ),
                )
              case Left(_) =>
                // Best-effort verification failed -- preserve the original move error.
                Left(originalErr)
            }
        }
    }
  }

  override def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit] = {
    // Create only the final directory path, not intermediate segments.
    // Azure Data Lake Gen2 with Hierarchical Namespace (HNS) enabled will auto-create
    // parent directories when creating a nested directory.
    //
    // Note: Creating intermediate directory markers can cause issues where the marker
    // is interpreted as an empty file, conflicting with file operations.
    val normalizedPath = Option(path).map(_.trim.stripPrefix("/").stripSuffix("/")).getOrElse("")
    if (normalizedPath.isEmpty) {
      ().asRight
    } else {
      Try {
        val fsClient  = client.getFileSystemClient(bucket)
        val dirClient = fsClient.getDirectoryClient(normalizedPath)
        dirClient.createIfNotExists()
        ()
      }.recover {
        // 409 Conflict: another task concurrently created this parent directory between
        // our failed file-create attempt (404) and this directory-create call.
        // The post-condition we need (the directory existing) is already met, so treat
        // this as success and let the caller retry creating the file.
        case e: DataLakeStorageException if e.getStatusCode == 409 => ()
      }.toEither.leftMap(e => FileCreateError(e, normalizedPath)).void
    }
  }

  override def getBlobAsStringAndEtag(bucket: String, path: String): Either[FileLoadError, (String, String)] =
    Try {
      Using.resource(new ByteArrayOutputStream()) {
        baos =>
          val resp: FileReadResponse = client.getFileSystemClient(bucket).getFileClient(path).readWithResponse(
            baos,
            null,
            null,
            null,
            false,
            null,
            Context.NONE,
          )
          (new String(baos.toByteArray), resp.getDeserializedHeaders.getETag)
      }
    }.toEither.leftMap {
      case ex: DataLakeStorageException if ex.getStatusCode == 404 =>
        FileNotFoundError(ex, path)
      case ex =>
        GeneralFileLoadError(ex, path)
    }

  override def deleteFile(bucket: String, file: String, eTag: String): Either[FileDeleteError, Unit] =
    for {
      cli    <- Try(client.getFileSystemClient(bucket)).toEither.leftMap(e => FileDeleteError(e, file))
      options = new DataLakePathDeleteOptions().setRequestConditions(new DataLakeRequestConditions().setIfMatch(eTag))
      _ <- Try {
        cli.deleteFileIfExistsWithResponse(
          file,
          options,
          null,
          Context.NONE,
        )
      }.toEither.leftMap(FileDeleteError(_, file))
    } yield ()

  /**
   * True when a failed rename represents a NoOverwriteExistingObject create losing the race —
   * i.e. the destination already exists. ADLS surfaces this as 409 PathAlreadyExists or 412.
   * Scoped to NoOverwrite: an ObjectWithETag 412 is an eTag mismatch (fencing) and must NOT
   * be reclassified as recoverable.
   */
  private def isLostNoOverwriteRace[O](
    objectProtection: ObjectProtection[O],
    dse:              DataLakeStorageException,
  ): Boolean =
    objectProtection match {
      case NoOverwriteExistingObject(_) => dse.getStatusCode == 409 || dse.getStatusCode == 412
      case _                            => false
    }

  override def writeBlobToFile[O](
    bucket:           String,
    path:             String,
    objectProtection: ObjectProtection[O],
  )(
    implicit
    encoder: Encoder[O],
  ): Either[UploadError, ObjectWithETag[O]] = {
    logger.debug(
      s"[${connectorTaskId.show}] Uploading file from json object ({${objectProtection.wrappedObject}}) to datalake $bucket:$path",
    )

    val content = objectProtection.wrappedObject.asJson.noSpaces
    val bytes   = content.getBytes

    // Per-task temp path: distinct lockUuid per task instance guarantees no cross-task collision.
    // lockUuid is connectorTaskId.lockUuid = UUID.randomUUID().toString (see ConnectorTaskId.scala
    // line 26 and ConnectorTaskIdTest.scala line 152 — 4 instances produce 4 distinct UUIDs).
    // Do NOT derive lockUuid deterministically from taskId/topic/partition: that would re-introduce
    // a cross-task collision on the same .tmp path.
    val tmpPath = s"$path.tmp.${connectorTaskId.lockUuid}"

    // Precondition applied to the DESTINATION slot (slot 4) of renameWithResponse.
    // IMPORTANT: do NOT apply to the source slot (slot 3) — that would arbitrate on the
    // task-owned .tmp blob (always matches/always absent) and would silently defeat
    // zombie fencing. See plan section "Critical SDK detail — destination conditions slot."
    val destinationConditions: DataLakeRequestConditions = objectProtection match {
      case NoOverwriteExistingObject(_) =>
        // Atomic arbitration: exactly one of N concurrent renames to the same destination
        // wins; the losers fail with EITHER 409 PathAlreadyExists (observed in prod) OR 412
        // Precondition Failed. Both are mapped to the recoverable NonOverwriteFileExistsError
        // (see isLostNoOverwriteRace + tryWriteViaRenameWithDirectoryRecovery), so the loser
        // re-reads and adopts the lock instead of failing the task.
        new DataLakeRequestConditions().setIfNoneMatch("*")
      case ObjectWithETag(_, eTag) =>
        // eTag here is the eTag of the CURRENT DESTINATION (sourced from topicPartitionToETags /
        // granularCache), not the eTag of the .tmp blob. Arbitration is on the destination.
        new DataLakeRequestConditions().setIfMatch(eTag)
      case _ =>
        null
    }

    def deleteTmp(): Unit =
      Try(client.getFileSystemClient(bucket).deleteFileIfExists(tmpPath))
        .failed.foreach(ex =>
          logger.warn(s"[${connectorTaskId.show}] Failed to clean up temp blob $tmpPath: ${ex.getMessage}"),
        )

    def tryWriteViaRename(): Either[Throwable, String] = {
      val fsClient = client.getFileSystemClient(bucket)

      // Step 1: create the temp blob (always overwrite=true — it is task-owned and uncontended)
      // Wrapped in Try so a 404 PathNotFound from createFile is also caught and routed to the
      // parent-directory recovery path in tryWriteViaRenameWithDirectoryRecovery.
      Try(fsClient.createFile(tmpPath, true)).toEither.flatMap { tmpClient =>
        // The rename attempt covers steps 2 (append+flush) and 3 (rename).
        // recoverWith / deleteTmp MUST NOT run after the rename commits — at that point
        // .tmp no longer exists at the source and the destination is durable.
        val renameAttempt: Either[Throwable, com.azure.core.http.rest.Response[DataLakeFileClient]] =
          Try {
            // Step 2: append + flush into the temp blob
            Using.resource(new ByteArrayInputStream(bytes)) { bais =>
              tmpClient.append(bais, 0, bytes.length.toLong)
            }
            tmpClient.flushWithResponse(
              bytes.length.toLong,
              true,  // retainUncommittedData
              false, // close
              new PathHttpHeaders(),
              null, // unconditional flush of temp blob — create already established ownership
              null, // timeout
              Context.NONE,
            )

            // Step 3: atomic rename .tmp -> destination with precondition on DESTINATION.
            tmpClient.renameWithResponse(
              bucket,                // destinationFileSystem
              path,                  // destinationPath
              null,                  // sourceRequestConditions (slot 3) — must remain null
              destinationConditions, // destinationRequestConditions (slot 4) — eTag arbitration lands here
              null,                  // timeout
              Context.NONE,
            )
          }.recoverWith {
            case ex =>
              deleteTmp()
              scala.util.Failure(ex)
          }.toEither

        // Extract the post-rename eTag OUTSIDE the recoverWith scope.
        // If the rename committed but the response carries no ETag header,
        // HttpHeaders.get() returns null and calling .getValue would NPE.
        // That NPE must NOT trigger deleteTmp() (the source .tmp is gone) and
        // must NOT be silently swallowed — surface it as a clear Left so the
        // caller can distinguish ambiguous-success from a genuine rename failure.
        renameAttempt.flatMap(extractPostRenameETag(bucket, path, tmpPath))
      }
    }

    def extractPostRenameETag(
      destBucket:     String,
      destPath:       String,
      sourceTmpPath:  String,
    )(renameResponse: com.azure.core.http.rest.Response[DataLakeFileClient],
    ): Either[Throwable, String] = {
      val maybeETag = for {
        headers <- Option(renameResponse.getHeaders)
        header  <- Option(headers.get(HttpHeaderName.ETAG))
        value   <- Option(header.getValue).filter(_.nonEmpty)
      } yield value
      maybeETag match {
        case Some(eTag) => Right(eTag)
        case None =>
          Left(
            new IllegalStateException(
              s"[${connectorTaskId.show}] renameWithResponse committed $sourceTmpPath -> $destBucket:$destPath " +
                s"but the response contained no ETag header. The destination file is durable. " +
                s"Manual recovery: restart the task to re-read the destination eTag via HEAD.",
            ),
          )
      }
    }

    def tryWriteViaRenameWithDirectoryRecovery(): Either[UploadError, String] =
      tryWriteViaRename() match {
        case Right(eTag) => Right(eTag)

        case Left(dse: DataLakeStorageException)
            if dse.getStatusCode == 404 || Option(dse.getMessage).exists(_.contains("PathNotFound")) =>
          // Parent directory doesn't exist — create it and retry against the .tmp path.
          // The 404 recovery is preserved in the invisible namespace (.tmp), so a crash
          // between create-directory and rename still leaves no 0-byte blob at the destination.
          parentDirectory(path) match {
            case Some(dir) =>
              createDirectoryIfNotExists(bucket, dir) match {
                case Left(err) => Left(FileCreateError(err.exception, content))
                case Right(_)  => tryWriteViaRename().leftMap(ex => FileCreateError(ex, content))
              }
            case None => Left(FileCreateError(dse, content))
          }

        case Left(dse: DataLakeStorageException) if isLostNoOverwriteRace(objectProtection, dse) =>
          // NoOverwriteExistingObject lost the create race: the destination already exists.
          // ADLS Gen2 HNS surfaces this as EITHER 409 PathAlreadyExists (observed in prod on the
          // rename) OR 412 Precondition Failed (setIfNoneMatch("*") arbitration). Both mean
          // "another task won". Recoverable: IndexManagerV2.open re-reads and adopts the lock.
          // This is scoped to NoOverwrite only — an ObjectWithETag 412 (eTag mismatch) is the
          // zombie-fencing signal and falls through to the fatal FileCreateError arm below.
          logger.info(
            s"[${connectorTaskId.show}] Lost no-overwrite create race renaming $tmpPath -> $bucket:$path " +
              s"(status ${dse.getStatusCode}); surfacing recoverable NonOverwriteFileExistsError",
          )
          Left(NonOverwriteFileExistsError(dse, path))

        case Left(dse: DataLakeStorageException) if dse.getStatusCode == 412 =>
          // 412 Precondition Failed on an ObjectWithETag write: eTag mismatch (concurrent write
          // by another task). This is the zombie-fencing mechanism and MUST stay fatal.
          logger.warn(
            s"[${connectorTaskId.show}] Precondition failed (412) renaming $tmpPath -> $bucket:$path",
          )
          Left(FileCreateError(dse, content))

        case Left(other) => Left(FileCreateError(other, content))
      }

    tryWriteViaRenameWithDirectoryRecovery() match {
      case Right(eTag) =>
        logger.debug(
          s"[${connectorTaskId.show}] Completed atomic upload ($content) to datalake $bucket:$path (post-rename eTag=$eTag)",
        )
        Right(new ObjectWithETag[O](objectProtection.wrappedObject, eTag))

      case Left(err) => Left(err)
    }
  }

  /**
   * Updates the lastModified timestamp of a file by updating its metadata.
   * In Azure Datalake Gen2, setting metadata updates the lastModified timestamp.
   * Preserves existing metadata by merging with the new "touched" timestamp.
   *
   * @param bucket The name of the filesystem (container).
   * @param path The path of the file to touch.
   * @return Either a FileTouchError if the operation failed, or Unit if successful.
   */
  override def touchFile(bucket: String, path: String): Either[FileTouchError, Unit] =
    Try {
      val fileClient = client.getFileSystemClient(bucket).getFileClient(path)
      // Retrieve existing metadata to preserve it (setMetadata replaces all metadata)
      // Note: getMetadata can return null when no user-defined metadata exists
      val existingMetadata = fileClient.getProperties.getMetadata
      val metadata = Option(existingMetadata)
        .map(m => new java.util.HashMap[String, String](m))
        .getOrElse(new java.util.HashMap[String, String]())
      metadata.put("touched", java.time.Instant.now().toString)
      fileClient.setMetadata(metadata)
      logger.debug(s"[${connectorTaskId.show}] Touched file $bucket/$path to update lastModified timestamp")
    }.toEither.leftMap(ex => FileTouchError(ex, path)).void
}
