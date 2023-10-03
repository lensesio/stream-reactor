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
package io.lenses.streamreactor.connect.datalake.storage

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.azure.storage.file.datalake.DataLakeFileClient
import com.azure.storage.file.datalake.DataLakeFileSystemClient
import com.azure.storage.file.datalake.DataLakeServiceClient
import com.azure.storage.file.datalake.models.ListPathsOptions
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.storage.EmptyContentsStringError
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfKeysResponse
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import io.lenses.streamreactor.connect.cloud.common.storage.ZeroByteFileError
import io.lenses.streamreactor.connect.datalake.storage.SamplePages.emptyPagedIterable
import io.lenses.streamreactor.connect.datalake.storage.SamplePages.pagedIterable
import io.lenses.streamreactor.connect.datalake.storage.SamplePages.pages
import org.mockito.Answers
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchersSugar
import org.mockito.InOrder
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.Instant

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
    doNothing.when(fileClient).uploadFromFile(anyString(), anyBoolean)

    val fileSystemClient = mock[DataLakeFileSystemClient]
    when(fileSystemClient.createFileIfNotExists(anyString())).thenReturn(fileClient)

    when(client.getFileSystemClient("test-bucket")).thenReturn(fileSystemClient)

    storageInterface.uploadFile(UploadableFile(testFile), "test-bucket", "test-path") should be(Right(()))

    val vInOrder: InOrder = inOrder(client, fileSystemClient, fileClient)
    vInOrder.verify(client).getFileSystemClient("test-bucket")
    vInOrder.verify(fileSystemClient).createFileIfNotExists("test-path")
    vInOrder.verify(fileClient).uploadFromFile(testFile.getPath, true)
  }

  "uploadFile" should "return a Left(UploadFailedError) if there is an exception during upload" in {
    val source = createTestFile
    val bucket = "test-bucket"
    val path   = "test-path"

    when(client.getFileSystemClient(bucket).createFileIfNotExists(path).uploadFromFile(anyString(),
                                                                                       ArgumentMatchers.eq(true),
    )).thenThrow(
      new IllegalStateException("Now remember, walk without rhythm, and we won't attract the worm."),
    )

    val result = storageInterface.uploadFile(UploadableFile(source), bucket, path)

    result.isLeft should be(true)
    result.left.getOrElse(throw new AssertionError("Expected Left")) should be(a[UploadFailedError])
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

  "pathExists" should "return a Left(FileLoadError) if there is an exception" in {
    val bucket = "test-bucket"
    val path   = "test-path"

    when(client.getFileSystemClient(bucket).getFileClient(path).exists()).thenThrow(new IllegalStateException(
      "Now, remember, the first step in avoiding a *trap* - is knowing of its existence.",
    ))

    val result = storageInterface.pathExists(bucket, path)

    result.isLeft should be(true)
    result.left.getOrElse(throw new AssertionError("Expected Left")) should be(a[FileLoadError])
  }

  private def createTestFile = {
    val source = File.createTempFile("a-file", "")
    Files.writeString(source.toPath, "real file content", StandardOpenOption.WRITE)
    source
  }

  "listKeysRecursive" should "return a list of keys when successful" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    setUpPageIterableReturningMock()

    val result = storageInterface.listKeysRecursive(bucket, prefix)

    val metadata: ListOfKeysResponse[DatalakeFileMetadata] = result.value.value
    metadata.files.size should be(100)

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

    result.left.value should be(a[FileLoadError])
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

}
