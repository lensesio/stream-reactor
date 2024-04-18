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
package io.lenses.streamreactor.connect.datalake.storage

import cats.implicits._
import com.azure.core.http.rest.PagedIterable
import com.azure.storage.file.datalake.DataLakeFileClient
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.azure.storage.file.datalake.models.DataLakeStorageException
import com.azure.storage.file.datalake.models.ListPathsOptions
import com.azure.storage.file.datalake.models.PathItem
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
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

  override def pathExists(bucket: String, path: String): Either[FileLoadError, Boolean] =
    Try(client.getFileSystemClient(bucket).getFileClient(path).exists().booleanValue()).toEither.recover {
      case ex: DataLakeStorageException if ex.getStatusCode.toString.startsWith("4") =>
        false
    }.leftMap(FileLoadError(
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

  override def getBlob(bucket: String, path: String): Either[FileLoadError, InputStream] =
    throw new NotImplementedError("Required for source")

  override def getBlobAsString(bucket: String, path: String): Either[FileLoadError, String] =
    Try {
      Using.resource(new ByteArrayOutputStream()) {
        baos =>
          client.getFileSystemClient(bucket).getFileClient(path).read(baos)
          new String(baos.toByteArray)
      }
    }.toEither.leftMap(t => FileLoadError(t, path))

  override def getMetadata(bucket: String, path: String): Either[FileLoadError, ObjectMetadata] =
    throw new NotImplementedError("Required for source")

  private def createFile(bucket: String, path: String): DataLakeFileClient =
    client.getFileSystemClient(bucket).createFile(path, true)

  override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, Unit] = {
    logger.debug(s"[{}] Uploading file from local {} to Data Lake {}:{}", connectorTaskId.show, source, bucket, path)
    for {
      file <- source.validate.toEither
      _ <- Try {
        val createFileClient: DataLakeFileClient = createFile(bucket, path)
        createFileClient.uploadFromFile(file.getPath, true)
        logger.debug(s"[{}] Completed upload from local {} to Data Lake {}:{}",
                     connectorTaskId.show,
                     source,
                     bucket,
                     path,
        )
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
    } yield ()

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
}
