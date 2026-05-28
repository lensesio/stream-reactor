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

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.azure.core.http.HttpHeaders
import com.azure.core.http.HttpMethod
import com.azure.core.http.HttpRequest
import com.azure.core.http.rest.Response
import com.azure.core.http.HttpResponse
import reactor.core.publisher.Mono
import reactor.core.publisher.Flux
import java.nio.ByteBuffer
import java.nio.charset.Charset
import com.azure.core.util.Context
import com.azure.core.util.FluxUtil
import com.azure.storage.common.ParallelTransferOptions
import com.azure.storage.file.datalake.DataLakeFileClient
import com.azure.storage.file.datalake.DataLakeDirectoryClient
import com.azure.storage.file.datalake.DataLakeFileSystemClient
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.azure.storage.file.datalake.models.DataLakeRequestConditions
import com.azure.storage.file.datalake.models.PathHttpHeaders
import com.azure.storage.file.datalake.models.DataLakeStorageException
import com.azure.storage.file.datalake.models.DownloadRetryOptions
import com.azure.storage.file.datalake.models.FileRange
import com.azure.storage.file.datalake.models.FileReadAsyncResponse
import com.azure.storage.file.datalake.models.FileReadHeaders
import com.azure.storage.file.datalake.models.FileReadResponse
import com.azure.storage.file.datalake.models.ListPathsOptions
import com.azure.storage.file.datalake.models.PathInfo
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.sink.seek.NoOverwriteExistingObject
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.storage.EmptyContentsStringError
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.GeneralFileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.PathError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import io.lenses.streamreactor.connect.cloud.common.storage.ZeroByteFileError
import io.circe.generic.semiauto._
import io.lenses.streamreactor.connect.datalake.storage.SamplePages.emptyPagedIterable
import io.lenses.streamreactor.connect.datalake.storage.SamplePages.pagedIterable
import io.lenses.streamreactor.connect.datalake.storage.SamplePages.pages
import org.mockito.Answers
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchersSugar
import org.mockito.InOrder
import org.mockito.MockitoSugar
import org.mockito.invocation.InvocationOnMock
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import ch.qos.logback.classic.{ Level => LogbackLevel }
import ch.qos.logback.classic.{ Logger => LogbackLogger }
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.Instant
import scala.annotation.nowarn
import scala.jdk.CollectionConverters.ListHasAsScala

@nowarn
class DatalakeStorageInterfaceTest
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  private val client: DataLakeServiceClient = mock[DataLakeServiceClient](Answers.RETURNS_DEEP_STUBS)

  before {
    reset(client)
  }
  private val instant = Instant.now()

  private val connectorTaskId: ConnectorTaskId = ConnectorTaskId("connector", 1, 1)

  private val storageInterface = new DatalakeStorageInterface(connectorTaskId, client)

  private val bucket = "myBucket"
  private val prefix = "myPrefix".some

  "list" should "retrieve first page of results" in {
    setUpPageIterableReturningMock()
    val datalakeFileMetadata: DatalakeFileMetadata = DatalakeFileMetadata(
      "-1.txt",
      instant,
      none,
    )
    val filesList = storageInterface.list(bucket, prefix, datalakeFileMetadata.some, 10).value.value
    filesList.files should be(Seq("0.txt",
                                  "1.txt",
                                  "2.txt",
                                  "3.txt",
                                  "4.txt",
                                  "5.txt",
                                  "6.txt",
                                  "7.txt",
                                  "8.txt",
                                  "9.txt",
    ))
    filesList.latestFileMetadata.file should be("9.txt")
    filesList.latestFileMetadata.continuation.value.lastContinuationToken should be(pages.head.continuationToken.value)

  }

  private def setUpPageIterableReturningMock() =
    when(
      client.getFileSystemClient(anyString).listPaths(any[ListPathsOptions], any[Duration]),
    ).thenReturn(pagedIterable)

  "list" should "retrieve second page of results" in {
    setUpPageIterableReturningMock()

    val datalakeFileMetadata: DatalakeFileMetadata = DatalakeFileMetadata(
      "9.txt",
      instant,
      Continuation(
        pagedIterable,
        pages.head.continuationToken.value,
      ).some,
    )
    val filesList = storageInterface.list(bucket, prefix, datalakeFileMetadata.some, 10).value.value
    filesList.files should be(Seq(
      "10.txt",
      "11.txt",
      "12.txt",
      "13.txt",
      "14.txt",
      "15.txt",
      "16.txt",
      "17.txt",
      "18.txt",
      "19.txt",
    ))
    filesList.latestFileMetadata.file should be("19.txt")
    filesList.latestFileMetadata.continuation.value.lastContinuationToken should be(pages(1).continuationToken.value)

  }

  "list" should "ignore first page of results when file appears last" in {
    setUpPageIterableReturningMock()

    val datalakeFileMetadata: DatalakeFileMetadata = DatalakeFileMetadata(
      "9.txt",
      instant,
      none,
    )
    val filesList = storageInterface.list(bucket, prefix, datalakeFileMetadata.some, 10).value.value
    filesList.files should be(Seq(
      "10.txt",
      "11.txt",
      "12.txt",
      "13.txt",
      "14.txt",
      "15.txt",
      "16.txt",
      "17.txt",
      "18.txt",
      "19.txt",
    ))
    filesList.latestFileMetadata.file should be("19.txt")
    filesList.latestFileMetadata.continuation.value.lastContinuationToken should be(pages(1).continuationToken.value)

  }

  "list" should "retrieve from middle of page of results" in {
    setUpPageIterableReturningMock()

    val datalakeFileMetadata: DatalakeFileMetadata = DatalakeFileMetadata(
      "25.txt",
      instant,
      Continuation(
        pagedIterable,
        pages(1).continuationToken.value,
      ).some,
    )
    val filesList = storageInterface.list(bucket, prefix, datalakeFileMetadata.some, 10).value.value
    filesList.files should be(Seq(
      "26.txt",
      "27.txt",
      "28.txt",
      "29.txt",
      "30.txt",
      "31.txt",
      "32.txt",
      "33.txt",
      "34.txt",
      "35.txt",
    ))
    filesList.latestFileMetadata.file should be("35.txt")
    filesList.latestFileMetadata.continuation.value.lastContinuationToken should be(pages(2).continuationToken.value)

  }

  "uploadFile" should "return a NonExistingFileError if the source file does not exist" in {
    val source = new File("/non/existing/file")
    val result = storageInterface.uploadFile(UploadableFile(source), "test-bucket", "test-path")

    result should be(Left(NonExistingFileError(source)))
  }

  "uploadFile" should "return a ZeroByteFileError if the source file has zero bytes" in {
    val source = File.createTempFile("empty-file", "")
    val result = storageInterface.uploadFile(UploadableFile(source), "test-bucket", "test-path")

    result should be(Left(ZeroByteFileError(source)))
  }

  "uploadFile" should "return a Right(Unit) if the upload is successful" in {
    val testFile = createTestFile

    val fileClient = mock[DataLakeFileClient]

    val eTag = "myEtag"

    val pathInfo = mock[PathInfo]
    when(pathInfo.getETag).thenReturn(eTag)

    val responsePathInfo: Response[PathInfo] = mock[Response[PathInfo]]
    when(responsePathInfo.getValue).thenReturn(pathInfo)

    when(
      fileClient.uploadFromFileWithResponse(
        anyString(),
        any[ParallelTransferOptions],
        isNull,
        isNull,
        any[DataLakeRequestConditions],
        isNull,
        isNull,
      ),
    ).thenReturn(responsePathInfo)

    val fileSystemClient = mock[DataLakeFileSystemClient]
    when(fileSystemClient.createFile(anyString, anyBoolean)).thenReturn(fileClient)

    when(client.getFileSystemClient("test-bucket")).thenReturn(fileSystemClient)

    storageInterface.uploadFile(UploadableFile(testFile), "test-bucket", "test-path") should be(Right("myEtag"))

    val vInOrder: InOrder = inOrder(client, fileSystemClient, fileClient)
    vInOrder.verify(client).getFileSystemClient("test-bucket")
    vInOrder.verify(fileSystemClient).createFile("test-path", true)
    vInOrder.verify(fileClient).uploadFromFileWithResponse(
      refEq(testFile.getPath),
      any[ParallelTransferOptions],
      isNull,
      isNull,
      any[DataLakeRequestConditions],
      isNull,
      isNull,
    )
  }

  "uploadFile" should "return a Left(UploadFailedError) if there is an exception during upload" in {
    val source = createTestFile
    val bucket = "test-bucket"
    val path   = "test-path"

    when(
      client.getFileSystemClient(bucket)
        .createFile(path, true)
        .uploadFromFileWithResponse(
          anyString(),
          any[ParallelTransferOptions],
          isNull,
          isNull,
          any[DataLakeRequestConditions],
          isNull,
          isNull,
        ),
    ).thenThrow(
      new IllegalStateException("Now remember, walk without rhythm, and we won't attract the worm."),
    )

    val result = storageInterface.uploadFile(UploadableFile(source), bucket, path)

    result.left.value should be(a[UploadFailedError])
  }

  "uploadFile" should "create parent directory and retry on PathNotFound" in {
    val testFile = createTestFile

    val eTag     = "myEtag"
    val pathInfo = mock[PathInfo]
    when(pathInfo.getETag).thenReturn(eTag)
    val resp = mock[Response[PathInfo]]
    when(resp.getValue).thenReturn(pathInfo)

    val fileClient       = mock[DataLakeFileClient]
    val fileSystemClient = mock[DataLakeFileSystemClient]
    val directoryClient  = mock[DataLakeDirectoryClient]

    val bucket = "test-bucket"
    val path   = "a/b/test-path"

    when(client.getFileSystemClient(bucket)).thenReturn(fileSystemClient)
    when(fileSystemClient.createFile(path, true)).thenReturn(fileClient)
    // First attempt throws 404/PathNotFound, second returns success
    val uploadInvocationCount = new java.util.concurrent.atomic.AtomicInteger(0)
    when(
      fileClient.uploadFromFileWithResponse(
        anyString(),
        any[ParallelTransferOptions],
        isNull[PathHttpHeaders],
        isNull,
        any[DataLakeRequestConditions],
        isNull,
        isNull,
      ),
    ).thenAnswer { _: InvocationOnMock =>
      if (uploadInvocationCount.getAndIncrement() == 0)
        throw new DataLakeStorageException("PathNotFound", mockHttpResponse(404), null)
      else resp
    }

    // Ensure parent creation is possible
    when(fileSystemClient.getDirectoryClient(anyString())).thenReturn(directoryClient)
    when(directoryClient.createIfNotExists()).thenReturn(pathInfo)

    val result = storageInterface.uploadFile(UploadableFile(testFile), bucket, path)

    result should be(Right(eTag))
  }

  "mvFile" should "ensure parent and retry rename when parent missing" in {
    val oldBucket = "oldBucket"
    val oldPath   = "old/a.txt"
    val newBucket = "newBucket"
    val newPath   = "a/b/new.txt"

    val fileClient       = mock[DataLakeFileClient]
    val fileSystemClient = mock[DataLakeFileSystemClient]
    val directoryClient  = mock[DataLakeDirectoryClient]
    val response         = mock[Response[DataLakeFileClient]]

    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(fileClient)
    val renameInvocationCount = new java.util.concurrent.atomic.AtomicInteger(0)
    when(fileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any))
      .thenAnswer { _: InvocationOnMock =>
        if (renameInvocationCount.getAndIncrement() == 0)
          throw new DataLakeStorageException("RenameDestinationParentPathNotFound", mockHttpResponse(404), null)
        else response
      }

    // Parent creation under newBucket
    when(client.getFileSystemClient(newBucket)).thenReturn(fileSystemClient)
    when(fileSystemClient.getDirectoryClient(anyString())).thenReturn(directoryClient)
    when(directoryClient.createIfNotExists()).thenReturn(mock[PathInfo])

    val res = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)
    res should be(Right(()))
    // Regression guard: the idempotence fallback must NOT execute when the parent-retry rename
    // succeeds.  pathExists (which calls fileClient.exists()) must never be called here.
    verify(fileClient, never).exists()
  }

  "pathExists" should "return Right(true) if the path exists" in {
    val bucket = "test-bucket"
    val path   = "existing-path"

    when(client.getFileSystemClient(bucket).getFileClient(path).exists()).thenReturn(true)
    val result = storageInterface.pathExists(bucket, path)

    result should be(Right(true))
  }

  "pathExists" should "return Right(false) if the path does not exist" in {
    val bucket = "test-bucket"
    val path   = "non-existing-path"

    when(client.getFileSystemClient(bucket).getFileClient(path).exists()).thenReturn(false)
    val result = storageInterface.pathExists(bucket, path)

    result should be(Right(false))
  }

  "pathExists" should "return a Left(PathError) if there is an exception" in {
    val bucket = "test-bucket"
    val path   = "test-path"

    when(client.getFileSystemClient(bucket).getFileClient(path).exists()).thenThrow(new IllegalStateException(
      "Now, remember, the first step in avoiding a *trap* - is knowing of its existence.",
    ))

    val result = storageInterface.pathExists(bucket, path)

    result.isLeft should be(true)
    result.left.getOrElse(throw new AssertionError("Expected Left")) should be(a[PathError])
  }

  "pathExists" should "return Right(false) when DataLakeStorageException has 404 status" in {
    val bucket = "test-bucket"
    val path   = "missing-path"

    when(client.getFileSystemClient(bucket).getFileClient(path).exists()).thenThrow(
      new DataLakeStorageException("PathNotFound", mockHttpResponse(404), null),
    )

    val result = storageInterface.pathExists(bucket, path)

    result should be(Right(false))
  }

  "pathExists" should "return Left(PathError) when DataLakeStorageException has 401 status" in {
    val bucket = "test-bucket"
    val path   = "unauthorised-path"

    when(client.getFileSystemClient(bucket).getFileClient(path).exists()).thenThrow(
      new DataLakeStorageException("AuthenticationFailed", mockHttpResponse(401), null),
    )

    val result = storageInterface.pathExists(bucket, path)

    result.isLeft should be(true)
    result.left.getOrElse(throw new AssertionError("Expected Left")) should be(a[PathError])
  }

  "pathExists" should "return Right(false) when DataLakeStorageException has 403 status (ADLS HNS)" in {
    val bucket = "test-bucket"
    val path   = "forbidden-path"

    when(client.getFileSystemClient(bucket).getFileClient(path).exists()).thenThrow(
      new DataLakeStorageException("AuthorizationFailure", mockHttpResponse(403), null),
    )

    val result = storageInterface.pathExists(bucket, path)

    result should be(Right(false))
  }

  private def createTestFile = {
    val source = File.createTempFile("a-file", "")
    Files.writeString(source.toPath, "real file content", StandardOpenOption.WRITE)
    source
  }

  private final class TestHttpResponse(req: HttpRequest, status: Int) extends HttpResponse(req) {
    override def getStatusCode: Int         = status
    override def getHeaders:    HttpHeaders = new HttpHeaders()
    override def getHeaderValue(name: String): String = null
    override def getBodyAsByteArray: Mono[Array[Byte]] = Mono.just(Array.emptyByteArray)
    override def getBody:            Flux[ByteBuffer]  = Flux.empty()
    override def getBodyAsString(charset: Charset): Mono[String] = Mono.just("")
    override def getBodyAsString(): Mono[String] = Mono.just("")
  }

  private def mockHttpResponse(status: Int): HttpResponse =
    new TestHttpResponse(new HttpRequest(HttpMethod.PUT, "https://example.com"), status)

  "listKeysRecursive" should "return a list of keys when successful" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    setUpPageIterableReturningMock()

    val result = storageInterface.listKeysRecursive(bucket, prefix)

    val metadata: ListOfKeysResponse[DatalakeFileMetadata] = result.value.value
    metadata.files.size should be(100)

  }

  "listKeysRecursive" should "set recursive flag on ListPathsOptions" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    val fsClient      = client.getFileSystemClient(bucket)
    val optionsCaptor = ArgumentCaptor.forClass(classOf[ListPathsOptions])
    when(fsClient.listPaths(optionsCaptor.capture(), any[Duration])).thenReturn(pagedIterable)

    storageInterface.listKeysRecursive(bucket, prefix)

    optionsCaptor.getValue.isRecursive should be(true)
  }

  "listKeysRecursive" should "return None when no keys are found" in {
    val bucket = "test-bucket"
    val prefix = Some("non-existing-prefix")

    when(
      client.getFileSystemClient(anyString).listPaths(any[ListPathsOptions], any[Duration]),
    ).thenReturn(emptyPagedIterable)

    val result = storageInterface.listKeysRecursive(bucket, prefix)

    result.value should be(None)
  }

  "listKeysRecursive" should "return a Left(FileListError) if there is an exception" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    when(client.getFileSystemClient(bucket).listPaths(any[ListPathsOptions], any[Duration])).thenThrow(
      new IllegalStateException("I know she has come to test him. No man has ever survived being tested with the box."),
    )

    val result = storageInterface.listKeysRecursive(bucket, prefix)

    result.left.value should be(a[FileListError])
  }

  "listKeysRecursive" should "return Right(None) when DataLakeStorageException has 404 status" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    when(client.getFileSystemClient(bucket).listPaths(any[ListPathsOptions], any[Duration])).thenThrow(
      new DataLakeStorageException("PathNotFound", mockHttpResponse(404), null),
    )

    val result = storageInterface.listKeysRecursive(bucket, prefix)

    result.value should be(None)
  }

  "listKeysRecursive" should "propagate 403 Forbidden as Left(FileListError)" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    when(client.getFileSystemClient(bucket).listPaths(any[ListPathsOptions], any[Duration])).thenThrow(
      new DataLakeStorageException("AuthorizationFailure", mockHttpResponse(403), null),
    )

    val result = storageInterface.listKeysRecursive(bucket, prefix)

    result.left.value should be(a[FileListError])
  }

  "listKeysRecursive" should "propagate 401 Unauthorized as Left(FileListError)" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    when(client.getFileSystemClient(bucket).listPaths(any[ListPathsOptions], any[Duration])).thenThrow(
      new DataLakeStorageException("AuthenticationFailed", mockHttpResponse(401), null),
    )

    val result = storageInterface.listKeysRecursive(bucket, prefix)

    result.left.value should be(a[FileListError])
  }

  "listFileMetaRecursive" should "return metadata for all files when successful" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    setUpPageIterableReturningMock()

    val result = storageInterface.listFileMetaRecursive(bucket, prefix)

    val metadata: ListOfMetadataResponse[DatalakeFileMetadata] = result.value.value
    metadata.files.size should be(100)
    metadata.files.foreach { fm =>
      fm.file should not be empty
      fm.lastModified should not be null
    }
    metadata.latestFileMetadata.file should be("99.txt")
  }

  "listFileMetaRecursive" should "set recursive flag on ListPathsOptions" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    val fsClient      = client.getFileSystemClient(bucket)
    val optionsCaptor = ArgumentCaptor.forClass(classOf[ListPathsOptions])
    when(fsClient.listPaths(optionsCaptor.capture(), any[Duration])).thenReturn(pagedIterable)

    storageInterface.listFileMetaRecursive(bucket, prefix)

    optionsCaptor.getValue.isRecursive should be(true)
  }

  "listFileMetaRecursive" should "return None when no files are found" in {
    val bucket = "test-bucket"
    val prefix = Some("non-existing-prefix")

    when(
      client.getFileSystemClient(anyString).listPaths(any[ListPathsOptions], any[Duration]),
    ).thenReturn(emptyPagedIterable)

    val result = storageInterface.listFileMetaRecursive(bucket, prefix)

    result.value should be(None)
  }

  "listFileMetaRecursive" should "return a Left(FileListError) if there is an exception" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    when(client.getFileSystemClient(bucket).listPaths(any[ListPathsOptions], any[Duration])).thenThrow(
      new IllegalStateException("The mystery of life isn't a problem to solve, but a reality to experience."),
    )

    val result = storageInterface.listFileMetaRecursive(bucket, prefix)

    result.left.value should be(a[FileListError])
  }

  "listFileMetaRecursive" should "return Right(None) when DataLakeStorageException has 404 status" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    when(client.getFileSystemClient(bucket).listPaths(any[ListPathsOptions], any[Duration])).thenThrow(
      new DataLakeStorageException("PathNotFound", mockHttpResponse(404), null),
    )

    val result = storageInterface.listFileMetaRecursive(bucket, prefix)

    result.value should be(None)
  }

  "listFileMetaRecursive" should "propagate 403 Forbidden as Left(FileListError)" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    when(client.getFileSystemClient(bucket).listPaths(any[ListPathsOptions], any[Duration])).thenThrow(
      new DataLakeStorageException("AuthorizationFailure", mockHttpResponse(403), null),
    )

    val result = storageInterface.listFileMetaRecursive(bucket, prefix)

    result.left.value should be(a[FileListError])
  }

  "listFileMetaRecursive" should "propagate 401 Unauthorized as Left(FileListError)" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    when(client.getFileSystemClient(bucket).listPaths(any[ListPathsOptions], any[Duration])).thenThrow(
      new DataLakeStorageException("AuthenticationFailed", mockHttpResponse(401), null),
    )

    val result = storageInterface.listFileMetaRecursive(bucket, prefix)

    result.left.value should be(a[FileListError])
  }

  "getBlobAsString" should "return the blob content as a string when successful" in {
    val bucket = "test-bucket"
    val path   = "test-path"

    val expectedContent = "Kwisatz Haderach"
    when(client.getFileSystemClient(bucket).getFileClient(path).read(any[ByteArrayOutputStream])).thenAnswer {
      byteArrayOutputStream: ByteArrayOutputStream =>
        byteArrayOutputStream.write(expectedContent.getBytes)
        byteArrayOutputStream.flush()
    }
    val result = storageInterface.getBlobAsString(bucket, path)

    result.value should be(expectedContent)
  }

  it should "return a Left(FileLoadError) if there is an exception" in {
    val bucket = "test-bucket"
    val path   = "test-path"

    when(client.getFileSystemClient(bucket).getFileClient(path).read(any[ByteArrayOutputStream])).thenThrow(
      new IllegalStateException("We have wormsign the likes of which even God has never seen."),
    )
    val result = storageInterface.getBlobAsString(bucket, path)

    result.left.value should be(a[GeneralFileLoadError])
  }

  "getBlobAsStringAndEtag" should "return the etag and the blob content as a string when successful" in {
    val bucket = "test-bucket"
    val path   = "test-path"

    val expectedEtag    = "etag"
    val expectedContent = "Kwisatz Haderach"
    when(
      client.getFileSystemClient(bucket)
        .getFileClient(path)
        .readWithResponse(
          any[ByteArrayOutputStream],
          any[FileRange],
          any[DownloadRetryOptions],
          any[DataLakeRequestConditions],
          any[Boolean],
          any[Duration],
          any[Context],
        ),
    ).thenAnswer {
      byteArrayOutputStream: ByteArrayOutputStream =>
        byteArrayOutputStream.write(expectedContent.getBytes)
        byteArrayOutputStream.flush()

        new FileReadResponse(
          new FileReadAsyncResponse(
            new HttpRequest(HttpMethod.GET, "https://test-url"),
            200,
            new HttpHeaders(),
            FluxUtil.toFluxByteBuffer(new ByteArrayInputStream("".getBytes())),
            new FileReadHeaders().setETag(expectedEtag),
          ),
        )
    }
    val result = storageInterface.getBlobAsStringAndEtag(bucket, path)

    result.value should be((expectedContent, expectedEtag))
  }

  "writeStringToFile" should "upload the data string to the specified path when successful" in {
    val bucket = "test-bucket"
    val path   = "test-path"
    val data   = "Test data"

    var readFromIS: Option[String] = Option.empty
    when(
      client.getFileSystemClient(bucket).createFile(path, true).append(any[ByteArrayInputStream], anyLong, anyLong),
    ).thenAnswer {
      (inputStream: ByteArrayInputStream, _: Long, _: Long) =>
        readFromIS = new String(inputStream.readAllBytes()).some
        ()
    }

    val result = storageInterface.writeStringToFile(bucket, path, UploadableString(data))

    result.value should be(())
    readFromIS.value should be(data)
  }

  "writeStringToFile" should "return a Left(EmptyContentsStringError) if the data string is empty" in {
    val bucket = "test-bucket"
    val path   = "test-path"
    val data   = ""

    val result = storageInterface.writeStringToFile(bucket, path, UploadableString(data))

    result.left.value should be(EmptyContentsStringError(data))
  }

  "writeStringToFile" should "return a Left(FileCreateError) if there is an exception" in {
    val bucket = "test-bucket"
    val path   = "test-path"
    val data   = "Test data"

    when(client.getFileSystemClient(bucket).createFile(path, true)).thenThrow(new IllegalStateException(
      "Behold, as a wild ass in the desert, go I forth to my work.",
    ))
    val result = storageInterface.writeStringToFile(bucket, path, UploadableString(data))

    result.left.value should be(a[FileCreateError])
  }

  "deleteFiles" should "delete the specified files when successful" in {
    val bucket = "test-bucket"
    val files  = Seq("file1", "file2", "file3")

    val mockFileSystem = mock[DataLakeFileSystemClient]
    when(client.getFileSystemClient(bucket)).thenReturn(mockFileSystem)
    when(mockFileSystem.deleteFileIfExists(anyString())).thenReturn(true)

    val result = storageInterface.deleteFiles(bucket, files)

    result.value should be(())

    verify(mockFileSystem).deleteFileIfExists("file1")
    verify(mockFileSystem).deleteFileIfExists("file2")
    verify(mockFileSystem).deleteFileIfExists("file3")
  }

  "writeStringToFile" should "return a Left(FileDeleteError) if there is an exception when getting the file system client" in {
    val bucket = "test-bucket"
    val files  = Seq("file1", "file2", "file3")

    when(client.getFileSystemClient(bucket)).thenThrow(
      new IllegalStateException("Your highness, there must be some mistake. I never requested your presence."),
    )

    val result = storageInterface.deleteFiles(bucket, files)

    result.left.value should be(a[FileDeleteError])
  }

  "writeStringToFile" should "return a Left(FileDeleteError) if there is an exception when deleting files" in {
    val bucket = "test-bucket"
    val files  = Seq("file1", "file2", "file3")

    val mockFileSystem = mock[DataLakeFileSystemClient]
    when(client.getFileSystemClient(bucket)).thenReturn(mockFileSystem)
    when(mockFileSystem.deleteFileIfExists(anyString())).thenThrow(
      new IllegalStateException("We are the secret of the universe. We are the secret."),
    )

    val result = storageInterface.deleteFiles(bucket, files)

    result.left.value should be(a[FileDeleteError])
  }

  "mvFile" should "move a file from one bucket to another successfully" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val fileClient = mock[DataLakeFileClient]
    val response   = mock[Response[DataLakeFileClient]]
    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(fileClient)
    when(fileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any)).thenReturn(response)

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result should be(Right(()))
    verify(fileClient).renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any)
    // Pins that the idempotence fallback is a true fall-through that NEVER runs on the success path:
    // pathExists must NOT be called when renameWithResponse succeeds on the first attempt.
    verify(fileClient, never).exists()
  }

  "mvFile" should "return a FileMoveError if rename fails" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val fileClient = mock[DataLakeFileClient]
    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(fileClient)
    when(fileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any)).thenThrow(
      new DataLakeStorageException("Rename failed", mockHttpResponse(500), null),
    )

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])
    verify(fileClient).renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any)
  }

  "mvFile" should "return a FileMoveError if the old file does not exist" in {
    val oldBucket = "oldBucket"
    val oldPath   = "nonExistingPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenThrow(new DataLakeStorageException(
      "File not found",
      mockHttpResponse(404),
      null,
    ))

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])
  }

  "mvFile" should "be idempotent when source is missing and destination is present" in {
    // Pins parity with AwsS3StorageInterface.mvFile and GCPStorageStorageInterface.mvFile.
    // Required for crash-after-Copy-before-lock-update recovery: without this branch the
    // pending-CopyOperation replay on restart would escalate to FatalCloudSinkError and
    // produce a deterministic restart loop on the affected partition.
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val srcFileClient  = mock[DataLakeFileClient]
    val destFileClient = mock[DataLakeFileClient]

    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(srcFileClient)
    when(client.getFileSystemClient(newBucket).getFileClient(newPath)).thenReturn(destFileClient)

    when(srcFileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any)).thenThrow(
      new DataLakeStorageException("Rename failed", mockHttpResponse(500), null),
    )
    when(srcFileClient.exists()).thenReturn(false)
    when(destFileClient.exists()).thenReturn(true)

    // Capture WARN logs emitted by DatalakeStorageInterface during the call.
    val dsiLogger    = LoggerFactory.getLogger(classOf[DatalakeStorageInterface]).asInstanceOf[LogbackLogger]
    val listAppender = new ListAppender[ILoggingEvent]()
    listAppender.start()
    dsiLogger.addAppender(listAppender)

    val result =
      try storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)
      finally dsiLogger.detachAppender(listAppender)

    result should be(Right(()))

    // Must emit exactly one WARN containing both source and destination paths so operators
    // have a clear signal that a crash-after-Copy recovery replay succeeded.
    val warnMessages = listAppender.list.asScala
      .filter(_.getLevel == LogbackLevel.WARN)
      .map(_.getFormattedMessage)
    warnMessages.exists(msg => msg.contains(oldPath) && msg.contains(newPath)) shouldBe true
  }

  "mvFile" should "return FileMoveError when rename fails and neither source nor destination exists" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val srcFileClient  = mock[DataLakeFileClient]
    val destFileClient = mock[DataLakeFileClient]

    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(srcFileClient)
    when(client.getFileSystemClient(newBucket).getFileClient(newPath)).thenReturn(destFileClient)

    when(srcFileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any)).thenThrow(
      new DataLakeStorageException("Rename failed", mockHttpResponse(500), null),
    )
    when(srcFileClient.exists()).thenReturn(false)
    when(destFileClient.exists()).thenReturn(false)

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])
    val moveErr = result.left.value.asInstanceOf[FileMoveError]
    moveErr.exception should be(a[IllegalStateException])
    moveErr.exception.getMessage should include("both missing")
  }

  "mvFile" should "preserve original error when source presence check fails" in {
    // Defensive case: if the best-effort pathExists check itself fails (e.g. transient
    // auth error), the caller must see the ORIGINAL move error, not a fabricated one.
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val srcFileClient = mock[DataLakeFileClient]

    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(srcFileClient)

    when(srcFileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any)).thenThrow(
      new DataLakeStorageException("Rename failed", mockHttpResponse(500), null),
    )
    // pathExists returns Left -> pathExists.fold(_ => false, !_) yields false -> sourceMissing=false -> return original error
    when(srcFileClient.exists()).thenThrow(new IllegalStateException("transient auth error"))

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])
  }

  // ADLS rename 403/409 non-masking guard tests
  //
  // The idempotence fallback in mvFile only applies when (a) the rename fails AND
  // (b) the source object is verifiably absent AND (c) the destination is present.
  // A 403 Forbidden or 409 Conflict response means the rename was actively rejected
  // by the server; in these cases the source is typically still present, so the
  // idempotence branch is NOT entered and the original error surfaces.
  // These tests pin that contract to prevent future regressions where broader
  // idempotence application could silently swallow genuine access/conflict errors.

  "mvFile" should "surface 403 Forbidden as FileMoveError and NOT mask it as idempotent success" in {
    // 403 = Forbidden (e.g. insufficient permission to rename the source path).
    // Source IS still present, so idempotence check must NOT trigger.
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val srcFileClient  = mock[DataLakeFileClient]
    val destFileClient = mock[DataLakeFileClient]

    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(srcFileClient)
    when(client.getFileSystemClient(newBucket).getFileClient(newPath)).thenReturn(destFileClient)

    val forbiddenException =
      new DataLakeStorageException("AuthorizationPermissionMismatch", mockHttpResponse(403), null)
    when(srcFileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any))
      .thenThrow(forbiddenException)

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])
    val moveErr = result.left.value.asInstanceOf[FileMoveError]
    moveErr.exception should be(a[DataLakeStorageException])
    moveErr.exception.asInstanceOf[DataLakeStorageException].getStatusCode should be(403)
    // With the 403 short-circuit, neither source nor dest should be probed.
    verify(srcFileClient, never).exists()
    verify(destFileClient, never).exists()
  }

  "mvFile" should "surface original 403 error when rename fails 403 and source pathExists also returns 403 (HNS quirk)" in {
    // Regression test: pathExists now treats 403 as absent (Right(false)) for the HNS+SharedKey quirk.
    // Without the 403 guard in mvFile, a 403 rename failure followed by pathExists returning Right(false)
    // (source "absent") and destFileClient.exists() returning true would cause a false idempotent success.
    // The guard short-circuits before any existence probing.
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val srcFileClient  = mock[DataLakeFileClient]
    val destFileClient = mock[DataLakeFileClient]

    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(srcFileClient)
    when(client.getFileSystemClient(newBucket).getFileClient(newPath)).thenReturn(destFileClient)

    val forbiddenException =
      new DataLakeStorageException("AuthorizationPermissionMismatch", mockHttpResponse(403), null)
    when(srcFileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any))
      .thenThrow(forbiddenException)
    // HNS quirk: exists() also throws 403 for a non-existent path under SharedKey auth.
    when(srcFileClient.exists()).thenThrow(
      new DataLakeStorageException("AuthorizationPermissionMismatch", mockHttpResponse(403), null),
    )
    // Destination is present — without the fix this would cause a false idempotent success.
    when(destFileClient.exists()).thenReturn(true)

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])
    val moveErr = result.left.value.asInstanceOf[FileMoveError]
    moveErr.exception should be(a[DataLakeStorageException])
    moveErr.exception.asInstanceOf[DataLakeStorageException].getStatusCode should be(403)
    // The 403 guard short-circuits before any existence probing.
    verify(srcFileClient, never).exists()
    verify(destFileClient, never).exists()
  }

  "mvFile" should "surface 409 Conflict as FileMoveError and NOT mask it as idempotent success" in {
    // 409 = Conflict (e.g. concurrent rename or lease conflict on ADLS).
    // Source IS still present; idempotence must NOT trigger.
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val srcFileClient  = mock[DataLakeFileClient]
    val destFileClient = mock[DataLakeFileClient]

    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(srcFileClient)
    when(client.getFileSystemClient(newBucket).getFileClient(newPath)).thenReturn(destFileClient)

    val conflictException =
      new DataLakeStorageException("LeaseIdMismatchWithBlobOperation", mockHttpResponse(409), null)
    when(srcFileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any))
      .thenThrow(conflictException)
    // Source file is still present — concurrent rename was rejected, not completed.
    when(srcFileClient.exists()).thenReturn(true)

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])
    val moveErr = result.left.value.asInstanceOf[FileMoveError]
    moveErr.exception should be(a[DataLakeStorageException])
    moveErr.exception.asInstanceOf[DataLakeStorageException].getStatusCode should be(409)
    verify(destFileClient, never).exists()
  }

  "mvFile" should "surface 500 Internal Server Error as FileMoveError when source is still present" in {
    // Defensive: a 500 during rename with source still present must not be swallowed.
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val srcFileClient  = mock[DataLakeFileClient]
    val destFileClient = mock[DataLakeFileClient]

    when(client.getFileSystemClient(oldBucket).getFileClient(oldPath)).thenReturn(srcFileClient)
    when(client.getFileSystemClient(newBucket).getFileClient(newPath)).thenReturn(destFileClient)

    when(srcFileClient.renameWithResponse(eqTo(newBucket), eqTo(newPath), any, any, any, any)).thenThrow(
      new DataLakeStorageException("InternalError", mockHttpResponse(500), null),
    )
    when(srcFileClient.exists()).thenReturn(true)

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])
    verify(destFileClient, never).exists()
  }

  // Test data case class for writeBlobToFile tests
  case class TestIndexFile(owner: String, offset: Option[Long])
  implicit val testIndexFileEncoder: io.circe.Encoder[TestIndexFile] = deriveEncoder[TestIndexFile]

  // ── writeBlobToFile — atomic .tmp + rename implementation ────────────────
  //
  // The implementation writes via a per-task temp blob (.lock.tmp.<lockUuid>)
  // and atomically renames it to the destination.  The eTag arbitration
  // condition lands in the DESTINATION slot (slot 4) of renameWithResponse,
  // never in the source slot (slot 3).

  private def setupAtomicWriteHappyPath(
    bucket:         String,
    path:           String,
    postRenameETag: String,
  ): (DataLakeFileSystemClient, DataLakeFileClient, Response[DataLakeFileClient]) = {
    val tmpPath       = s"$path.tmp.${connectorTaskId.lockUuid}"
    val fsClient      = mock[DataLakeFileSystemClient]
    val tmpClient     = mock[DataLakeFileClient]
    val flushResp     = mock[Response[PathInfo]]
    val pathInfo      = mock[PathInfo]
    val renameResp    = mock[Response[DataLakeFileClient]]
    val renameHeaders = new HttpHeaders()
    renameHeaders.set("ETag", postRenameETag)

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    // .tmp is always created with overwrite=true (task-owned, uncontended)
    when(fsClient.createFile(tmpPath, true)).thenReturn(tmpClient)
    doAnswer((_: InvocationOnMock) => ()).when(tmpClient).append(any[ByteArrayInputStream], anyLong, anyLong)
    when(pathInfo.getETag).thenReturn("tmp-etag") // .tmp eTag (not returned to caller)
    when(flushResp.getValue).thenReturn(pathInfo)
    when(
      tmpClient.flushWithResponse(
        anyLong,
        anyBoolean,
        anyBoolean,
        any[PathHttpHeaders],
        isNull[DataLakeRequestConditions],
        isNull[java.time.Duration],
        any[Context],
      ),
    ).thenReturn(flushResp)
    when(renameResp.getHeaders).thenReturn(renameHeaders)

    (fsClient, tmpClient, renameResp)
  }

  "writeBlobToFile" should "successfully create file with NoOverwriteExistingObject: uses .tmp + rename, eTag from headers" in {
    val bucket        = "test-bucket"
    val path          = "test-path/index.lock"
    val tmpPath       = s"$path.tmp.${connectorTaskId.lockUuid}"
    val postRenameTag = "post-rename-etag"
    val testData      = TestIndexFile("owner-123", Some(100L))

    val (fsClient, tmpClient, renameResp) = setupAtomicWriteHappyPath(bucket, path, postRenameTag)
    when(
      tmpClient.renameWithResponse(eqTo(bucket), eqTo(path), any, any, any, any),
    ).thenReturn(renameResp)

    val result = storageInterface.writeBlobToFile(bucket, path, NoOverwriteExistingObject(testData))

    result.isRight should be(true)
    result.value.wrappedObject should be(testData)
    // eTag must come from rename response headers, not from flush response
    result.value.eTag should be(postRenameTag)

    // Verify .tmp was created (overwrite=true) and rename was called
    verify(fsClient).createFile(tmpPath, true)
    verify(tmpClient).renameWithResponse(eqTo(bucket), eqTo(path), any, any, any, any)
  }

  "writeBlobToFile" should "NoOverwriteExistingObject: precondition lands in destination slot (slot 4), source slot (slot 3) is null" in {
    val bucket        = "test-bucket"
    val path          = "test-path/index.lock"
    val postRenameTag = "post-rename-etag"
    val testData      = TestIndexFile("owner-123", Some(100L))

    val (_, tmpClient, renameResp) = setupAtomicWriteHappyPath(bucket, path, postRenameTag)

    val srcConditionsCaptor  = ArgumentCaptor.forClass(classOf[DataLakeRequestConditions])
    val destConditionsCaptor = ArgumentCaptor.forClass(classOf[DataLakeRequestConditions])

    when(
      tmpClient.renameWithResponse(anyString(), anyString(), any, any, any, any),
    ).thenReturn(renameResp)

    storageInterface.writeBlobToFile(bucket, path, NoOverwriteExistingObject(testData))

    // Critical: verify 6-arg renameWithResponse and capture both condition slots
    verify(tmpClient).renameWithResponse(
      anyString(),
      anyString(),
      srcConditionsCaptor.capture(),  // slot 3 — must be null
      destConditionsCaptor.capture(), // slot 4 — must carry setIfNoneMatch("*")
      any,
      any,
    )

    srcConditionsCaptor.getValue should be(null)
    destConditionsCaptor.getValue should not be null
    destConditionsCaptor.getValue.getIfNoneMatch should be("*")
  }

  "writeBlobToFile" should "return FileCreateError when NoOverwriteExistingObject and destination exists (412 from rename)" in {
    val bucket   = "test-bucket"
    val path     = "test-path/index.lock"
    val tmpPath  = s"$path.tmp.${connectorTaskId.lockUuid}"
    val testData = TestIndexFile("owner-123", Some(100L))

    val fsClient  = mock[DataLakeFileSystemClient]
    val tmpClient = mock[DataLakeFileClient]
    val flushResp = mock[Response[PathInfo]]
    val pathInfo  = mock[PathInfo]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.createFile(tmpPath, true)).thenReturn(tmpClient)
    doAnswer((_: InvocationOnMock) => ()).when(tmpClient).append(any[ByteArrayInputStream], anyLong, anyLong)
    when(pathInfo.getETag).thenReturn("tmp-etag")
    when(flushResp.getValue).thenReturn(pathInfo)
    when(
      tmpClient.flushWithResponse(
        anyLong,
        anyBoolean,
        anyBoolean,
        any[PathHttpHeaders],
        isNull[DataLakeRequestConditions],
        isNull[java.time.Duration],
        any[Context],
      ),
    ).thenReturn(flushResp)

    // Rename fails with 412 (destination already exists — NoOverwrite condition not met)
    when(tmpClient.renameWithResponse(eqTo(bucket), eqTo(path), any, any, any, any)).thenThrow(
      new DataLakeStorageException("ConditionNotMet", mockHttpResponse(412), null),
    )

    val result = storageInterface.writeBlobToFile(bucket, path, NoOverwriteExistingObject(testData))

    result.isLeft should be(true)
    result.left.value should be(a[FileCreateError])
  }

  "writeBlobToFile" should "successfully update file with ObjectWithETag: rename with setIfMatch on destination slot" in {
    val bucket        = "test-bucket"
    val path          = "test-path/index.lock"
    val existingTag   = "existing-etag"
    val postRenameTag = "post-rename-etag"
    val testData      = TestIndexFile("owner-123", Some(200L))

    val (fsClient, tmpClient, renameResp) = setupAtomicWriteHappyPath(bucket, path, postRenameTag)
    when(
      tmpClient.renameWithResponse(eqTo(bucket), eqTo(path), any, any, any, any),
    ).thenReturn(renameResp)

    val result = storageInterface.writeBlobToFile(bucket, path, ObjectWithETag(testData, existingTag))

    result.isRight should be(true)
    result.value.wrappedObject should be(testData)
    result.value.eTag should be(postRenameTag)
  }

  "writeBlobToFile" should "ObjectWithETag: setIfMatch(destETag) lands in destination slot (slot 4), source slot (slot 3) is null" in {
    val bucket        = "test-bucket"
    val path          = "test-path/index.lock"
    val existingTag   = "existing-etag"
    val postRenameTag = "post-rename-etag"
    val testData      = TestIndexFile("owner-123", Some(200L))

    val (_, tmpClient, renameResp) = setupAtomicWriteHappyPath(bucket, path, postRenameTag)

    val srcConditionsCaptor  = ArgumentCaptor.forClass(classOf[DataLakeRequestConditions])
    val destConditionsCaptor = ArgumentCaptor.forClass(classOf[DataLakeRequestConditions])

    when(tmpClient.renameWithResponse(anyString(), anyString(), any, any, any, any)).thenReturn(renameResp)

    storageInterface.writeBlobToFile(bucket, path, ObjectWithETag(testData, existingTag))

    verify(tmpClient).renameWithResponse(
      anyString(),
      anyString(),
      srcConditionsCaptor.capture(),  // slot 3 — must be null
      destConditionsCaptor.capture(), // slot 4 — must carry setIfMatch(existingTag)
      any,
      any,
    )

    srcConditionsCaptor.getValue should be(null)
    destConditionsCaptor.getValue should not be null
    // eTag on destination conditions is the CURRENT DESTINATION eTag (existingTag), not the .tmp eTag
    destConditionsCaptor.getValue.getIfMatch should be(existingTag)
  }

  "writeBlobToFile" should "return FileCreateError when ObjectWithETag and eTag mismatch (412 on rename)" in {
    val bucket      = "test-bucket"
    val path        = "test-path/index.lock"
    val existingTag = "old-etag"
    val tmpPath     = s"$path.tmp.${connectorTaskId.lockUuid}"
    val testData    = TestIndexFile("owner-123", Some(200L))

    val fsClient  = mock[DataLakeFileSystemClient]
    val tmpClient = mock[DataLakeFileClient]
    val flushResp = mock[Response[PathInfo]]
    val pathInfo  = mock[PathInfo]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.createFile(tmpPath, true)).thenReturn(tmpClient)
    doAnswer((_: InvocationOnMock) => ()).when(tmpClient).append(any[ByteArrayInputStream], anyLong, anyLong)
    when(pathInfo.getETag).thenReturn("tmp-etag")
    when(flushResp.getValue).thenReturn(pathInfo)
    when(
      tmpClient.flushWithResponse(
        anyLong,
        anyBoolean,
        anyBoolean,
        any[PathHttpHeaders],
        isNull[DataLakeRequestConditions],
        isNull[java.time.Duration],
        any[Context],
      ),
    ).thenReturn(flushResp)

    when(tmpClient.renameWithResponse(eqTo(bucket), eqTo(path), any, any, any, any)).thenThrow(
      new DataLakeStorageException("ConditionNotMet", mockHttpResponse(412), null),
    )

    val result = storageInterface.writeBlobToFile(bucket, path, ObjectWithETag(testData, existingTag))

    result.isLeft should be(true)
    result.left.value should be(a[FileCreateError])
  }

  "writeBlobToFile" should "clean up .tmp on append failure" in {
    val bucket   = "test-bucket"
    val path     = "test-path/index.lock"
    val tmpPath  = s"$path.tmp.${connectorTaskId.lockUuid}"
    val testData = TestIndexFile("owner-123", Some(100L))

    val fsClient  = mock[DataLakeFileSystemClient]
    val tmpClient = mock[DataLakeFileClient]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.createFile(tmpPath, true)).thenReturn(tmpClient)
    doAnswer((_: InvocationOnMock) => throw new RuntimeException("append failed"))
      .when(tmpClient).append(any[ByteArrayInputStream], anyLong, anyLong)

    val result = storageInterface.writeBlobToFile(bucket, path, NoOverwriteExistingObject(testData))

    result.isLeft should be(true)
    result.left.value should be(a[FileCreateError])
    // .tmp cleanup must be attempted
    verify(fsClient).deleteFileIfExists(tmpPath)
  }

  "writeBlobToFile" should "clean up .tmp on flush failure" in {
    val bucket   = "test-bucket"
    val path     = "test-path/index.lock"
    val tmpPath  = s"$path.tmp.${connectorTaskId.lockUuid}"
    val testData = TestIndexFile("owner-123", Some(100L))

    val fsClient  = mock[DataLakeFileSystemClient]
    val tmpClient = mock[DataLakeFileClient]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.createFile(tmpPath, true)).thenReturn(tmpClient)
    doAnswer((_: InvocationOnMock) => ()).when(tmpClient).append(any[ByteArrayInputStream], anyLong, anyLong)
    when(
      tmpClient.flushWithResponse(
        anyLong,
        anyBoolean,
        anyBoolean,
        any[PathHttpHeaders],
        isNull[DataLakeRequestConditions],
        isNull[java.time.Duration],
        any[Context],
      ),
    ).thenThrow(new RuntimeException("flush failed"))

    val result = storageInterface.writeBlobToFile(bucket, path, NoOverwriteExistingObject(testData))

    result.isLeft should be(true)
    result.left.value should be(a[FileCreateError])
    verify(fsClient).deleteFileIfExists(tmpPath)
  }

  "writeBlobToFile" should "clean up .tmp on rename failure (non-412)" in {
    val bucket   = "test-bucket"
    val path     = "test-path/index.lock"
    val tmpPath  = s"$path.tmp.${connectorTaskId.lockUuid}"
    val testData = TestIndexFile("owner-123", Some(100L))

    val fsClient  = mock[DataLakeFileSystemClient]
    val tmpClient = mock[DataLakeFileClient]
    val flushResp = mock[Response[PathInfo]]
    val pathInfo  = mock[PathInfo]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.createFile(tmpPath, true)).thenReturn(tmpClient)
    doAnswer((_: InvocationOnMock) => ()).when(tmpClient).append(any[ByteArrayInputStream], anyLong, anyLong)
    when(pathInfo.getETag).thenReturn("tmp-etag")
    when(flushResp.getValue).thenReturn(pathInfo)
    when(
      tmpClient.flushWithResponse(
        anyLong,
        anyBoolean,
        anyBoolean,
        any[PathHttpHeaders],
        isNull[DataLakeRequestConditions],
        isNull[java.time.Duration],
        any[Context],
      ),
    ).thenReturn(flushResp)
    when(tmpClient.renameWithResponse(eqTo(bucket), eqTo(path), any, any, any, any)).thenThrow(
      new DataLakeStorageException("ServerError", mockHttpResponse(500), null),
    )

    val result = storageInterface.writeBlobToFile(bucket, path, NoOverwriteExistingObject(testData))

    result.isLeft should be(true)
    result.left.value should be(a[FileCreateError])
    verify(fsClient).deleteFileIfExists(tmpPath)
  }

  "writeBlobToFile" should "return FileCreateError (not NPE) when rename succeeds but response has no ETag header, and NOT call deleteTmp" in {
    val bucket   = "test-bucket"
    val path     = "test-path/index.lock"
    val tmpPath  = s"$path.tmp.${connectorTaskId.lockUuid}"
    val testData = TestIndexFile("owner-123", Some(100L))

    val fsClient   = mock[DataLakeFileSystemClient]
    val tmpClient  = mock[DataLakeFileClient]
    val flushResp  = mock[Response[PathInfo]]
    val pathInfo   = mock[PathInfo]
    val renameResp = mock[Response[DataLakeFileClient]]
    // No ETag header set — simulates an ADLS response that omits the ETag field
    val emptyHeaders = new HttpHeaders()

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.createFile(tmpPath, true)).thenReturn(tmpClient)
    doAnswer((_: InvocationOnMock) => ()).when(tmpClient).append(any[ByteArrayInputStream], anyLong, anyLong)
    when(pathInfo.getETag).thenReturn("tmp-etag")
    when(flushResp.getValue).thenReturn(pathInfo)
    when(
      tmpClient.flushWithResponse(
        anyLong,
        anyBoolean,
        anyBoolean,
        any[PathHttpHeaders],
        isNull[DataLakeRequestConditions],
        isNull[java.time.Duration],
        any[Context],
      ),
    ).thenReturn(flushResp)
    when(renameResp.getHeaders).thenReturn(emptyHeaders)
    when(tmpClient.renameWithResponse(eqTo(bucket), eqTo(path), any, any, any, any)).thenReturn(renameResp)

    val result = storageInterface.writeBlobToFile(bucket, path, NoOverwriteExistingObject(testData))

    result.isLeft should be(true)
    result.left.value should be(a[FileCreateError])
    // The rename committed — .tmp was moved to the destination, so deleteTmp MUST NOT be called
    verify(fsClient, times(0)).deleteFileIfExists(tmpPath)
  }

  "writeBlobToFile" should "return FileCreateError when rename succeeds but ETag header value is empty, and NOT call deleteTmp" in {
    val bucket   = "test-bucket"
    val path     = "test-path/index.lock"
    val tmpPath  = s"$path.tmp.${connectorTaskId.lockUuid}"
    val testData = TestIndexFile("owner-123", Some(100L))

    val fsClient   = mock[DataLakeFileSystemClient]
    val tmpClient  = mock[DataLakeFileClient]
    val flushResp  = mock[Response[PathInfo]]
    val pathInfo   = mock[PathInfo]
    val renameResp = mock[Response[DataLakeFileClient]]
    // ETag header present but empty
    val emptyETagHeaders = new HttpHeaders()
    emptyETagHeaders.set("ETag", "")

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.createFile(tmpPath, true)).thenReturn(tmpClient)
    doAnswer((_: InvocationOnMock) => ()).when(tmpClient).append(any[ByteArrayInputStream], anyLong, anyLong)
    when(pathInfo.getETag).thenReturn("tmp-etag")
    when(flushResp.getValue).thenReturn(pathInfo)
    when(
      tmpClient.flushWithResponse(
        anyLong,
        anyBoolean,
        anyBoolean,
        any[PathHttpHeaders],
        isNull[DataLakeRequestConditions],
        isNull[java.time.Duration],
        any[Context],
      ),
    ).thenReturn(flushResp)
    when(renameResp.getHeaders).thenReturn(emptyETagHeaders)
    when(tmpClient.renameWithResponse(eqTo(bucket), eqTo(path), any, any, any, any)).thenReturn(renameResp)

    val result = storageInterface.writeBlobToFile(bucket, path, NoOverwriteExistingObject(testData))

    result.isLeft should be(true)
    result.left.value should be(a[FileCreateError])
    // The rename committed — .tmp was moved to the destination, so deleteTmp MUST NOT be called
    verify(fsClient, times(0)).deleteFileIfExists(tmpPath)
  }

  "writeBlobToFile" should "create parent directory and retry .tmp create on PathNotFound" in {
    val bucket   = "test-bucket"
    val path     = "a/b/index.lock"
    val tmpPath  = s"$path.tmp.${connectorTaskId.lockUuid}"
    val testData = TestIndexFile("owner-123", Some(100L))

    val fsClient        = mock[DataLakeFileSystemClient]
    val tmpClient       = mock[DataLakeFileClient]
    val directoryClient = mock[DataLakeDirectoryClient]
    val pathInfo        = mock[PathInfo]
    val postRenameTag   = "post-rename-etag"

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)

    val createInvocationCount = new java.util.concurrent.atomic.AtomicInteger(0)
    when(fsClient.createFile(tmpPath, true)).thenAnswer { _: InvocationOnMock =>
      if (createInvocationCount.getAndIncrement() == 0)
        throw new DataLakeStorageException("PathNotFound", mockHttpResponse(404), null)
      else tmpClient
    }

    doAnswer((_: InvocationOnMock) => ()).when(tmpClient).append(any[ByteArrayInputStream], anyLong, anyLong)
    when(pathInfo.getETag).thenReturn("tmp-etag")
    val flushResp = mock[Response[PathInfo]]
    when(flushResp.getValue).thenReturn(pathInfo)
    when(
      tmpClient.flushWithResponse(
        anyLong,
        anyBoolean,
        anyBoolean,
        any[PathHttpHeaders],
        isNull[DataLakeRequestConditions],
        isNull[java.time.Duration],
        any[Context],
      ),
    ).thenReturn(flushResp)

    val renameResp    = mock[Response[DataLakeFileClient]]
    val renameHeaders = new HttpHeaders()
    renameHeaders.set("ETag", postRenameTag)
    when(renameResp.getHeaders).thenReturn(renameHeaders)
    when(tmpClient.renameWithResponse(eqTo(bucket), eqTo(path), any, any, any, any)).thenReturn(renameResp)

    when(fsClient.getDirectoryClient(anyString())).thenReturn(directoryClient)
    when(directoryClient.createIfNotExists()).thenReturn(pathInfo)

    val result = storageInterface.writeBlobToFile(bucket, path, NoOverwriteExistingObject(testData))

    result.isRight should be(true)
    result.value.wrappedObject should be(testData)
    result.value.eTag should be(postRenameTag)

    // .tmp create was called twice: first attempt 404, second succeeds
    verify(fsClient, times(2)).createFile(tmpPath, true)
    verify(directoryClient).createIfNotExists()
  }

  // ── createDirectoryIfNotExists ────────────────────────────────────────────

  "createDirectoryIfNotExists" should "return Right(()) when the directory is created successfully" in {
    val bucket    = "test-bucket"
    val path      = "a/b/c"
    val fsClient  = mock[DataLakeFileSystemClient]
    val dirClient = mock[DataLakeDirectoryClient]
    val pathInfo  = mock[PathInfo]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.getDirectoryClient(path)).thenReturn(dirClient)
    when(dirClient.createIfNotExists()).thenReturn(pathInfo)

    val result = storageInterface.createDirectoryIfNotExists(bucket, path)

    result should be(Right(()))
    verify(dirClient).createIfNotExists()
  }

  "createDirectoryIfNotExists" should "return Right(()) when createIfNotExists throws 409 (concurrent creation)" in {
    val bucket    = "test-bucket"
    val path      = "a/b/c"
    val fsClient  = mock[DataLakeFileSystemClient]
    val dirClient = mock[DataLakeDirectoryClient]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.getDirectoryClient(path)).thenReturn(dirClient)
    when(dirClient.createIfNotExists()).thenThrow(
      new DataLakeStorageException("PathAlreadyExists", mockHttpResponse(409), null),
    )

    val result = storageInterface.createDirectoryIfNotExists(bucket, path)

    result should be(Right(()))
  }

  "createDirectoryIfNotExists" should "return Left(FileCreateError) when createIfNotExists throws a non-409 exception" in {
    val bucket    = "test-bucket"
    val path      = "a/b/c"
    val fsClient  = mock[DataLakeFileSystemClient]
    val dirClient = mock[DataLakeDirectoryClient]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.getDirectoryClient(path)).thenReturn(dirClient)
    when(dirClient.createIfNotExists()).thenThrow(
      new DataLakeStorageException("InternalError", mockHttpResponse(500), null),
    )

    val result = storageInterface.createDirectoryIfNotExists(bucket, path)

    result.isLeft should be(true)
    result.left.value should be(a[FileCreateError])
  }

  // ── getBlobAsObject — EmptyFileError short-circuit ────────────────────────

  "getBlobAsObject" should "return Left(EmptyFileError) with eTag preserved when body is length-0" in {
    import io.lenses.streamreactor.connect.cloud.common.storage.EmptyFileError
    import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexFile
    import io.circe.generic.semiauto.deriveDecoder
    implicit val indexFileDecoder: io.circe.Decoder[IndexFile] = IndexFile.indexFileDecoder

    val bucket = "test-bucket"
    val path   = "test-path/index.lock"
    val eTag   = "the-etag"

    val fsClient    = mock[DataLakeFileSystemClient]
    val fileClient  = mock[DataLakeFileClient]
    val readResp    = mock[FileReadResponse]
    val readHeaders = mock[FileReadHeaders]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.getFileClient(path)).thenReturn(fileClient)
    when(readHeaders.getETag).thenReturn(eTag)
    when(readResp.getDeserializedHeaders).thenReturn(readHeaders)

    // readWithResponse writes empty bytes — simulates a 0-byte blob
    doAnswer { inv: InvocationOnMock =>
      val baos = inv.getArgument[ByteArrayOutputStream](0)
      baos.write(Array.empty[Byte])
      readResp
    }.when(fileClient).readWithResponse(any[ByteArrayOutputStream], any, any, any, anyBoolean, any, any)

    val result = storageInterface.getBlobAsObject[IndexFile](bucket, path)

    result.isLeft should be(true)
    val err = result.left.value
    err should be(a[EmptyFileError])
    err.asInstanceOf[EmptyFileError].fileName should be(path)
    err.asInstanceOf[EmptyFileError].eTag should be(eTag)
  }

  "getBlobAsObject" should "fall through to decode for non-empty body (not treated as empty)" in {
    import io.lenses.streamreactor.connect.cloud.common.storage.GeneralFileLoadError
    import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexFile
    implicit val indexFileDecoder: io.circe.Decoder[IndexFile] = IndexFile.indexFileDecoder

    val bucket = "test-bucket"
    val path   = "test-path/index.lock"
    val eTag   = "the-etag"

    val fsClient    = mock[DataLakeFileSystemClient]
    val fileClient  = mock[DataLakeFileClient]
    val readResp    = mock[FileReadResponse]
    val readHeaders = mock[FileReadHeaders]

    when(client.getFileSystemClient(bucket)).thenReturn(fsClient)
    when(fsClient.getFileClient(path)).thenReturn(fileClient)
    when(readHeaders.getETag).thenReturn(eTag)
    when(readResp.getDeserializedHeaders).thenReturn(readHeaders)

    // Whitespace-only body must NOT be treated as empty (length > 0)
    doAnswer { inv: InvocationOnMock =>
      val baos = inv.getArgument[ByteArrayOutputStream](0)
      baos.write("   ".getBytes)
      readResp
    }.when(fileClient).readWithResponse(any[ByteArrayOutputStream], any, any, any, anyBoolean, any, any)

    val result = storageInterface.getBlobAsObject[IndexFile](bucket, path)

    // Falls through to decode which fails for invalid JSON — not EmptyFileError
    result.isLeft should be(true)
    result.left.value should be(a[GeneralFileLoadError])
  }

}
