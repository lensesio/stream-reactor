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
package io.lenses.streamreactor.connect.datalake.storage
import cats.implicits._
import com.azure.core.http.rest.PagedIterable
import com.azure.core.util.Context
import com.azure.storage.common.ParallelTransferOptions
import com.azure.storage.file.datalake.DataLakeFileClient
import com.azure.storage.file.datalake.DataLakeServiceClient
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

class DatalakeStorageInterface(connectorTaskId: ConnectorTaskId, client: DataLakeServiceClient)
    extends StorageInterface[DatalakeFileMetadata]
    with LazyLogging {

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
      case ex: DataLakeStorageException if ex.getStatusCode.toString.startsWith("4") =>
        false
    }.leftMap(PathError(
      _,
      path,
    ))

  override def listFileMetaRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfMetadataResponse[DatalakeFileMetadata]]] =
    throw new NotImplementedError("Required for source")

  override def listKeysRecursive(
    bucket: String,
    prefix: Option[String],
  ): Either[FileListError, Option[ListOfKeysResponse[DatalakeFileMetadata]]] =
    Try {
      val bucketClient     = client.getFileSystemClient(bucket)
      val listPathsOptions = new ListPathsOptions()
      prefix.foreach(listPathsOptions.setPath)
      val iter = bucketClient.listPaths(listPathsOptions, null)

      val results = DatalakePageIterableAdaptor.getResults(iter)
      toListOfKeys(bucket, prefix, none, results)
    }
      .toEither.recover {
        case ex: DataLakeStorageException if ex.getStatusCode.toString.startsWith("4") =>
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
    for {
      file <- source.validate.toEither
      eTag <- Try {
        val createFileClient: DataLakeFileClient = createFile(bucket, path)
        val response = createFileClient.uploadFromFileWithResponse(
          file.getPath,
          new ParallelTransferOptions(),
          null,                            // PathHttpHeaders
          null,                            // Metadata
          new DataLakeRequestConditions(), // RequestConditions to avoid overwriting
          null,                            // Timeout
          null,                            // Context
        )
        logger.debug(s"[{}] Completed upload from local {} to Data Lake {}:{}",
                     connectorTaskId.show,
                     source,
                     bucket,
                     path,
        )
        response.getValue.getETag
      }
        .toEither.leftMap { ex =>
          logger.error(s"[{}] Failed upload from local {} to Data Lake {}:{}",
                       connectorTaskId.show,
                       source,
                       bucket,
                       path,
                       ex,
          )
          UploadFailedError(ex, file)
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
    Try(
      client.getFileSystemClient(oldBucket).getFileClient(oldPath)
        .renameWithResponse(
          newBucket,
          newPath,
          conditions.orNull,
          null,
          null,
          Context.NONE,
        ),
    ).toEither.leftMap(
      FileMoveError(_, oldPath, newPath),
    ).void
  }

  override def createDirectoryIfNotExists(bucket: String, path: String): Either[FileCreateError, Unit] = ().asRight

  override def getBlobAsStringAndEtag(bucket: String, path: String): Either[FileLoadError, (String, String)] =
    Try {
      Using.resource(new ByteArrayOutputStream()) {
        baos =>
          val resp: FileReadResponse = client.getFileSystemClient(bucket).getFileClient(path).readWithResponse(
            baos,
            null,
            null,
            null,
            true,
            null,
            Context.NONE,
          )
          (resp.getDeserializedHeaders.getETag, new String(baos.toByteArray))
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

    val content           = objectProtection.wrappedObject.asJson.noSpaces
    val requestConditions = new DataLakeRequestConditions()
    val protection: DataLakeRequestConditions = objectProtection match {
      case NoOverwriteExistingObject(_) => requestConditions.setIfNoneMatch("*")
      case ObjectWithETag(_, eTag)      => requestConditions.setIfMatch(eTag)
      case _                            => requestConditions
    }

    for {
      resp <- Try {
        val createFileClient: DataLakeFileClient = createFile(bucket, path)
        val bytes = content.getBytes
        Using.resource(new ByteArrayInputStream(bytes)) {
          bais =>
            createFileClient.append(bais, 0, bytes.length.toLong)
        }
        val position              = bytes.length.toLong
        val pathHttpHeaders       = new PathHttpHeaders()
        val retainUncommittedData = true
        val close                 = false // or true, if you want to finalize the file
        val context               = Context.NONE

        val response = createFileClient.flushWithResponse(
          position,
          retainUncommittedData,
          close,
          pathHttpHeaders,
          protection, // Correctly passed as DataLakeRequestConditions
          null,       // Timeout duration remains optional
          context,    // Context remains unchanged
        )

        logger.debug(
          s"[${connectorTaskId.show}] Completed upload from data string ($content) to datalake $bucket:$path",
        )
        response
      }.toEither.leftMap {
        ex =>
          logger.error(s"[{connectorTaskId.show}] Failed upload from data string ($content) to datalake $bucket:$path",
                       ex,
          )
          FileCreateError(ex, content)
      }
    } yield new ObjectWithETag[O](objectProtection.wrappedObject, resp.getValue.getETag)
  }
}
