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
package io.lenses.streamreactor.connect.gcp.storage

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
import cats.implicits.catsSyntaxOptionId
import com.google.cloud.ReadChannel
import com.google.cloud.RestorableState
import com.google.cloud.storage.Blob
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.BlobInfo
import com.google.cloud.storage.Storage
import com.google.cloud.storage.Storage.BlobListOption
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.ObjectMetadata
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.storage._
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageFileMetadata
import io.lenses.streamreactor.connect.gcp.storage.storage.GCPStorageStorageInterface
import SamplePages.emptyPage
import SamplePages.pages
import cats.implicits.none
import com.google.cloud.storage.CopyWriter
import com.google.cloud.storage.Storage.CopyRequest
import org.mockito.Answers
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayInputStream
import java.io.File
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.time.Instant
import java.time.OffsetDateTime
import scala.jdk.CollectionConverters.SeqHasAsJava

class GCPStorageStorageInterfaceTest
    extends AnyFlatSpec
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  private val client: Storage = mock[Storage](Answers.RETURNS_DEEP_STUBS)

  before {
    reset(client)
  }

  private val connectorTaskId: ConnectorTaskId = ConnectorTaskId("connector", 1, 1)

  private val storageInterface =
    new GCPStorageStorageInterface(connectorTaskId, client, false, Option.empty)

  private val bucket = "test-bucket"
  //private val prefix = "test-prefix".some
  private val path = "test-path"

  "uploadFile" should "return a NonExistingFileError if the source file does not exist" in {
    val source = new File("/non/existing/file")
    val result = storageInterface.uploadFile(UploadableFile(source), bucket, "test-path")

    result should be(Left(NonExistingFileError(source)))
  }

  "uploadFile" should "return a ZeroByteFileError if the source file has zero bytes" in {
    val source = File.createTempFile("empty-file", "")
    val result = storageInterface.uploadFile(UploadableFile(source), bucket, path)

    result should be(Left(ZeroByteFileError(source)))
  }

  "uploadFile" should "return a Right(Unit) if the upload is successful" in {
    storageInterface.uploadFile(UploadableFile(createTestFile), bucket, path).value should not be empty

    verify(client).createFrom(
      argMatching[BlobInfo] {
        case blobInfo: BlobInfo if blobInfo.getName.equals(path) && blobInfo.getBucket.equals(bucket) =>
      },
      any[Path],
    )
  }

  "uploadFile" should "return a Left(UploadFailedError) if there is an exception during upload" in {
    val source = createTestFile

    when(
      client.createFrom(
        argMatching[BlobInfo] {
          case blobInfo: BlobInfo if blobInfo.getName.equals(path) && blobInfo.getBucket.equals(bucket) =>
        },
        any[Path],
      ),
    ).thenThrow(
      new IllegalStateException("Now remember, walk without rhythm, and we won't attract the worm."),
    )

    val result = storageInterface.uploadFile(UploadableFile(source), bucket, path)

    result.isLeft should be(true)
    result.left.getOrElse(throw new AssertionError("Expected Left")) should be(a[UploadFailedError])
  }

  "pathExists" should "return Right(true) if the path exists" in {

    val mockBlob = mock[Blob]
    when(mockBlob.exists()).thenReturn(true)
    mockGetBlobInvocation(mockBlob)

    val result = storageInterface.pathExists(bucket, path)

    result should be(Right(true))
  }

  "pathExists" should "return Right(false) if the path does not exist" in {
    val path = "non-existing-path"

    val mockBlob = mock[Blob]
    when(mockBlob.exists()).thenReturn(false)
    mockGetBlobInvocation(mockBlob)

    val result = storageInterface.pathExists(bucket, path)

    result should be(Right(false))
  }

  private def mockGetBlobInvocation(mockBlob: Blob) =
    when(client.get(argMatching[BlobId] {
      case id: BlobId if id.getName.equals(path) && id.getBucket.equals(bucket) =>
    })).thenReturn(mockBlob)

  "pathExists" should "return a Left(FileLoadError) if there is an exception" in {

    when(client.get(argMatching[BlobId] {
      case id: BlobId if id.getName.equals(path) && id.getBucket.equals(bucket) =>
    })).thenThrow(new IllegalStateException(
      "Now, remember, the first step in avoiding a *trap* - is knowing of its existence.",
    ))

    val result = storageInterface.pathExists(bucket, path)

    result.left.value should be(a[FileLoadError])
  }

  "getBlob" should "return the blob content as a stream when successful" in {

    val expectedContent = "Kwisatz Haderach"

    val readChannel: ReadChannel = mockReadChannel(expectedContent)

    val mockBlob = mock[Blob]
    when(mockBlob.reader()).thenReturn(readChannel)
    mockGetBlobInvocation(mockBlob)

    val result = storageInterface.getBlob(bucket, path)

    val is: InputStream = result.value
    val asString = new String(org.apache.commons.io.IOUtils.readFully(is, 16))
    asString should be(expectedContent)
  }

  private def mockReadChannel(expectedContent: String): ReadChannel = {
    val channel = Channels.newChannel(new ByteArrayInputStream(expectedContent.getBytes()))
    new ReadChannel {
      override def close(): Unit = channel.close()

      override def seek(position: Long): Unit = ()

      override def setChunkSize(chunkSize: Int): Unit = ()

      override def capture(): RestorableState[ReadChannel] = ???

      override def read(dst: ByteBuffer): Int = channel.read(dst)

      override def isOpen: Boolean = channel.isOpen
    }
  }

  "getBlobAsString" should "return the blob content as a string when successful" in {

    val expectedContent = "Kwisatz Haderach"

    val mockBlob = mock[Blob]
    when(mockBlob.getContent()).thenReturn(expectedContent.getBytes)
    mockGetBlobInvocation(mockBlob)

    val result = storageInterface.getBlobAsString(bucket, path)

    result.value should be(expectedContent)
  }

  "getBlobAsString" should "return a Left(FileLoadError) if there is an exception" in {
    val bucket = "test-bucket"
    val path   = "test-path"

    val mockBlob = mock[Blob]
    when(mockBlob.getContent()).thenThrow(
      new IllegalStateException("We have wormsign the likes of which even God has never seen."),
    )
    mockGetBlobInvocation(mockBlob)

    val result = storageInterface.getBlobAsString(bucket, path)

    result.left.value should be(a[FileLoadError])
  }

  "writeStringToFile" should "upload the data string to the specified path when successful" in {
    val bucket = "test-bucket"
    val path   = "test-path"
    val data   = "Test data"

    when(
      client.create(any[BlobInfo], any[Array[Byte]]),
    ).thenAnswer {
      (_: BlobInfo, _: Array[Byte]) =>
        mock[Blob]
    }

    val result = storageInterface.writeStringToFile(bucket, path, UploadableString(data))

    result.value should be(())

    verify(client).create(
      argMatching[BlobInfo] {
        case bi: BlobInfo if bi.getName == path && bi.getBlobId.getBucket == bucket =>
      },
      argMatching[Array[Byte]] {
        case bArray: Array[Byte] if bArray sameElements data.getBytes =>
      },
    )
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

    when(
      client.create(any[BlobInfo], any[Array[Byte]]),
    ).thenThrow(new IllegalStateException(
      "Behold, as a wild ass in the desert, go I forth to my work.",
    ))
    val result = storageInterface.writeStringToFile(bucket, path, UploadableString(data))

    result.left.value should be(a[FileCreateError])
  }

  "deleteFiles" should "delete the specified files when successful" in {
    val bucket = "test-bucket"
    val files  = Seq("file1", "file2", "file3")

    val boolist = List(true, true, true).map(Boolean.box).asJava
    when(client.delete(
      BlobId.of(bucket, "file1"),
      BlobId.of(bucket, "file2"),
      BlobId.of(bucket, "file3"),
    )).thenReturn(boolist)

    val result = storageInterface.deleteFiles(bucket, files)

    result.value should be(())

    verify(client).delete(
      BlobId.of(bucket, "file1"),
      BlobId.of(bucket, "file2"),
      BlobId.of(bucket, "file3"),
    )
  }

  "listKeysRecursive" should "return a list of keys when successful" in {
    val bucket = "test-bucket"
    val prefix = "test-prefix"

    doReturn(pages.head).when(client).list(bucket, BlobListOption.prefix(prefix))
    val result = storageInterface.listKeysRecursive(bucket, prefix.some)

    val metadata: ListOfKeysResponse[GCPStorageFileMetadata] = result.value.value
    metadata.files.size should be(100)

  }
  "listKeysRecursive" should "return None when no keys are found" in {
    val bucket = "test-bucket"
    val prefix = "non-existing-prefix"

    doReturn(emptyPage).when(client).list(bucket, BlobListOption.prefix(prefix))

    val result = storageInterface.listKeysRecursive(bucket, prefix.some)

    result.value should be(None)
  }

  "listKeysRecursive" should "return a Left(FileListError) if there is an exception" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    doThrow(
      new IllegalStateException("I know she has come to test him. No man has ever survived being tested with the box."),
    ).when(client).list(bucket, BlobListOption.prefix(prefix.orNull))

    val result = storageInterface.listKeysRecursive(bucket, prefix)

    result.left.value should be(a[FileListError])
  }

  "getMetadata" should "return the blob metadata when successful" in {

    val expectedSize         = 100L
    val expectedLastModified = OffsetDateTime.now()

    val mockBlob = mock[Blob]
    when(mockBlob.getCreateTimeOffsetDateTime).thenReturn(expectedLastModified)
    when(mockBlob.getSize).thenReturn(expectedSize)
    mockGetBlobInvocation(mockBlob)

    val result = storageInterface.getMetadata(bucket, path)
    result.value should be(ObjectMetadata(expectedSize, expectedLastModified.toInstant))
  }

  "getMetadata" should "return a Left(FileLoadError) if there is an exception" in {
    val bucket = "test-bucket"
    val path   = "test-path"

    val mockBlob = mock[Blob]
    when(mockBlob.getContent()).thenThrow(
      new IllegalStateException("We have wormsign the likes of which even God has never seen."),
    )
    mockGetBlobInvocation(mockBlob)

    val result = storageInterface.getMetadata(bucket, path)
    result.left.value should be(a[FileLoadError])
  }

  "listFileMetaRecursive" should "return a list of metadata when successful" in {
    val bucket = "test-bucket"
    val prefix = "test-prefix"

    doReturn(pages.head).when(client).list(bucket, BlobListOption.prefix(prefix))
    val result = storageInterface.listFileMetaRecursive(bucket, prefix.some)

    val metadata: ListOfMetadataResponse[GCPStorageFileMetadata] = result.value.value
    metadata.files.size should be(100)
    metadata.latestFileMetadata should be(metadata.files.last)
  }

  "listFileMetaRecursive" should "return None when no keys are found" in {
    val bucket = "test-bucket"
    val prefix = "non-existing-prefix"

    doReturn(emptyPage).when(client).list(bucket, BlobListOption.prefix(prefix))

    val result = storageInterface.listFileMetaRecursive(bucket, prefix.some)

    result.value should be(None)
  }

  "listFileMetaRecursive" should "return a Left(FileListError) if there is an exception" in {
    val bucket = "test-bucket"
    val prefix = Some("test-prefix")

    doThrow(
      new IllegalStateException("The spice must flow."),
    ).when(client).list(bucket, BlobListOption.prefix(prefix.orNull))

    val result = storageInterface.listFileMetaRecursive(bucket, prefix)

    result.left.value should be(a[FileListError])
  }

  "list" should "return a list of keys following last file" in {
    val bucket       = "test-bucket"
    val prefix       = "test-prefix"
    val numResults   = 10
    val lastFileMeta = GCPStorageFileMetadata("myFilename", Instant.now())

    doReturn(pages.head).when(client).list(
      bucket,
      BlobListOption.pageSize(numResults.toLong),
      BlobListOption.startOffset("myFilename"),
      BlobListOption.prefix(prefix),
    )
    val result = storageInterface.list(bucket, prefix.some, lastFileMeta.some, numResults)

    verify(client).list(bucket,
                        BlobListOption.pageSize(10.toLong),
                        BlobListOption.startOffset("myFilename"),
                        BlobListOption.prefix(prefix),
    )

    val metadata: ListOfKeysResponse[GCPStorageFileMetadata] = result.value.value
    metadata.files.size should be(10)
    metadata.latestFileMetadata.file should be(metadata.files.last)
  }

  "list" should "return None when no more keys are found" in {
    val bucket       = "test-bucket"
    val prefix       = "non-existing-prefix"
    val numResults   = 10
    val lastFileMeta = GCPStorageFileMetadata("myFilename", Instant.now())

    doReturn(emptyPage).when(client).list(
      bucket,
      BlobListOption.pageSize(numResults.toLong),
      BlobListOption.startOffset("myFilename"),
      BlobListOption.prefix(prefix),
    )
    val result = storageInterface.list(bucket, prefix.some, lastFileMeta.some, numResults)

    verify(client).list(bucket,
                        BlobListOption.pageSize(10.toLong),
                        BlobListOption.startOffset("myFilename"),
                        BlobListOption.prefix(prefix),
    )
    result.value should be(None)
  }

  "list" should "return a Left(FileListError) if there is an exception" in {
    val bucket       = "test-bucket"
    val prefix       = "test-prefix"
    val numResults   = 10
    val lastFileMeta = GCPStorageFileMetadata("myFilename", Instant.now())

    doThrow(
      new IllegalStateException("The spice must flow."),
    ).when(client).list(
      bucket,
      BlobListOption.pageSize(numResults.toLong),
      BlobListOption.startOffset("myFilename"),
      BlobListOption.prefix(prefix),
    )

    val result = storageInterface.list(bucket, prefix.some, lastFileMeta.some, numResults)

    verify(client).list(bucket,
                        BlobListOption.pageSize(10.toLong),
                        BlobListOption.startOffset("myFilename"),
                        BlobListOption.prefix(prefix),
    )
    result.left.value should be(a[FileListError])
  }

  "close" should "close" in {
    storageInterface.close()

    verify(client).close()
  }

  "mvFile" should "move a file from one bucket to another successfully" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val sourceBlobId      = BlobId.of(oldBucket, oldPath)
    val destinationBlobId = BlobInfo.newBuilder(BlobId.of(newBucket, newPath)).build()

    mockBlobExistence(sourceBlobId)

    when(client.copy(any[Storage.CopyRequest])).thenReturn(mock[CopyWriter])
    when(client.delete(sourceBlobId)).thenReturn(true)
    val result: Either[FileMoveError, Unit] = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)
    result.value should be(())

    val copyRequestCaptor: ArgumentCaptor[Storage.CopyRequest] = ArgumentCaptor.forClass(classOf[Storage.CopyRequest])
    verify(client).copy(copyRequestCaptor.capture())
    val capturedCopyRequest = copyRequestCaptor.getValue
    capturedCopyRequest.getSource should be(sourceBlobId)
    capturedCopyRequest.getTarget should be(destinationBlobId)

    verify(client).delete(sourceBlobId)
  }

  private def mockBlobExistence(sourceBlobId: BlobId) = {
    val blob = mock[Blob]
    when(blob.exists()).thenReturn(true)
    when(client.get(sourceBlobId)).thenReturn(blob)
    ()
  }
  private def mockBlobNonExistence1(sourceBlobId: BlobId) = {
    val blob = mock[Blob]
    when(blob.exists()).thenReturn(false)
    when(client.get(sourceBlobId)).thenReturn(blob)
    ()
  }
  private def mockBlobNonExistence2(sourceBlobId: BlobId) = {
    val blob = null
    when(client.get(sourceBlobId)).thenReturn(blob)
    ()
  }

  "mvFile" should "return a FileMoveError if copy fails" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val sourceBlobId      = BlobId.of(oldBucket, oldPath)
    val destinationBlobId = BlobInfo.newBuilder(BlobId.of(newBucket, newPath)).build()

    mockBlobExistence(sourceBlobId)

    when(client.copy(any[Storage.CopyRequest])).thenThrow(new RuntimeException("Copy failed"))

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])

    val copyRequestCaptor: ArgumentCaptor[Storage.CopyRequest] = ArgumentCaptor.forClass(classOf[Storage.CopyRequest])
    verify(client).copy(copyRequestCaptor.capture())
    val capturedCopyRequest = copyRequestCaptor.getValue
    capturedCopyRequest.getSource should be(sourceBlobId)
    capturedCopyRequest.getTarget should be(destinationBlobId)

    verify(client, never).delete(sourceBlobId)
  }
  "mvFile" should "return a FileMoveError if delete fails" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val sourceBlobId      = BlobId.of(oldBucket, oldPath)
    val destinationBlobId = BlobInfo.newBuilder(BlobId.of(newBucket, newPath)).build()

    mockBlobExistence(sourceBlobId)

    when(client.copy(any[Storage.CopyRequest])).thenReturn(mock[CopyWriter])
    when(client.delete(sourceBlobId)).thenThrow(new RuntimeException("Delete failed"))

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.isLeft should be(true)
    result.left.value should be(a[FileMoveError])

    val copyRequestCaptor: ArgumentCaptor[Storage.CopyRequest] = ArgumentCaptor.forClass(classOf[Storage.CopyRequest])
    verify(client).copy(copyRequestCaptor.capture())
    val capturedCopyRequest = copyRequestCaptor.getValue
    capturedCopyRequest.getSource should be(sourceBlobId)
    capturedCopyRequest.getTarget should be(destinationBlobId)

    verify(client).delete(sourceBlobId)
  }

  "mvFile" should "use doesNotExist precondition if the target file does not exist" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val sourceBlobId = BlobId.of(oldBucket, oldPath)

    mockBlobExistence(sourceBlobId)

    when(client.get(newBucket, newPath)).thenReturn(null) // Target does not exist
    when(client.copy(any[Storage.CopyRequest])).thenReturn(mock[CopyWriter])
    when(client.delete(sourceBlobId)).thenReturn(true)

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)
    result.value should be(())

    val copyRequestCaptor: ArgumentCaptor[Storage.CopyRequest] = ArgumentCaptor.forClass(classOf[Storage.CopyRequest])
    verify(client).copy(copyRequestCaptor.capture())
    val capturedCopyRequest = copyRequestCaptor.getValue
    capturedCopyRequest.getTargetOptions should contain(Storage.BlobTargetOption.doesNotExist())

    verify(client).delete(sourceBlobId)
  }

  "mvFile" should "use generationMatch precondition if the target file exists" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val sourceBlobId = BlobId.of(oldBucket, oldPath)

    mockBlobExistence(sourceBlobId)

    val destinationBlob = mock[Blob]
    when(destinationBlob.getGeneration).thenReturn(123L)

    when(client.get(newBucket, newPath)).thenReturn(destinationBlob) // Target exists
    when(client.copy(any[Storage.CopyRequest])).thenReturn(mock[CopyWriter])
    when(client.delete(sourceBlobId)).thenReturn(true)

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)
    result.value should be(())

    val copyRequestCaptor: ArgumentCaptor[Storage.CopyRequest] = ArgumentCaptor.forClass(classOf[Storage.CopyRequest])
    verify(client).copy(copyRequestCaptor.capture())
    val capturedCopyRequest = copyRequestCaptor.getValue
    capturedCopyRequest.getTargetOptions should contain(Storage.BlobTargetOption.generationMatch(123L))

    verify(client).delete(sourceBlobId)
  }
  "mvFile" should "return FileMoveError if the source file does not exist after check" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val sourceBlobId      = BlobId.of(oldBucket, oldPath)
    val destinationBlobId = BlobInfo.newBuilder(BlobId.of(newBucket, newPath)).build()

    mockBlobExistence(sourceBlobId)

    // Simulate a failure during the copy operation (e.g., source file does not exist)
    when(client.copy(any[Storage.CopyRequest])).thenThrow(new RuntimeException("Source file does not exist"))

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    // Verify that the result is a FileMoveError
    result.left.value should be(a[FileMoveError])
    result.left.value.message() should include("Source file does not exist")

    // Verify that the copy was attempted
    val copyRequestCaptor: ArgumentCaptor[Storage.CopyRequest] = ArgumentCaptor.forClass(classOf[Storage.CopyRequest])
    verify(client).copy(copyRequestCaptor.capture())
    val capturedCopyRequest = copyRequestCaptor.getValue
    capturedCopyRequest.getSource should be(sourceBlobId)
    capturedCopyRequest.getTarget should be(destinationBlobId)

    // Verify that delete was not called, as the copy failed
    verify(client, never).delete(sourceBlobId)
  }
  "mvFile" should "return no error if the source file does not exist initially (1)" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val sourceBlobId = BlobId.of(oldBucket, oldPath)

    mockBlobNonExistence1(sourceBlobId)

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.value should be(())

    verify(client, never).copy(any[CopyRequest])
    verify(client, never).delete(sourceBlobId)
  }

  "mvFile" should "return no error if the source file does not exist initially (2)" in {
    val oldBucket = "oldBucket"
    val oldPath   = "oldPath"
    val newBucket = "newBucket"
    val newPath   = "newPath"

    val sourceBlobId = BlobId.of(oldBucket, oldPath)

    mockBlobNonExistence2(sourceBlobId)

    // Simulate a failure during the copy operation (e.g., source file does not exist)
    when(client.copy(any[Storage.CopyRequest])).thenThrow(new RuntimeException("Source file does not exist"))

    val result = storageInterface.mvFile(oldBucket, oldPath, newBucket, newPath, none)

    result.value should be(())

    verify(client, never).copy(any[CopyRequest])
    verify(client, never).delete(sourceBlobId)
  }

  private def createTestFile = {
    val source = File.createTempFile("a-file", "")
    Files.writeString(source.toPath, "real file content", StandardOpenOption.WRITE)
    source
  }

}
