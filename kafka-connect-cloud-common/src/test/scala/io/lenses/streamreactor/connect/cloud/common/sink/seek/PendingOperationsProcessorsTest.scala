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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.data.NonEmptyList
import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
class PendingOperationsProcessorsTest
    extends AnyFunSuiteLike
    with MockitoSugar
    with ArgumentMatchersSugar
    with Matchers
    with EitherValues
    with BeforeAndAfter {
  private val storageInterface: StorageInterface[TestFileMetadata] = mock[StorageInterface[TestFileMetadata]]
  private val tempLocalFile  = mock[File]
  private val tempRemoteFile = "my-temp-remote-file"

  private val topicPartition = Topic("topic1").withPartition(0)

  private val fnIndexUpdate: (
    TopicPartition,
    Option[Offset],
    Option[PendingState],
  ) => Either[NonFatalCloudSinkError, Option[Offset]] =
    mock[(TopicPartition, Option[Offset], Option[PendingState]) => Either[NonFatalCloudSinkError, Option[Offset]]]

  private val pendingOperationsProcessors = new PendingOperationsProcessors(storageInterface)

  before {
    reset(storageInterface, fnIndexUpdate, tempLocalFile)

    when(fnIndexUpdate.apply(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, lastCommittedOffset: Option[Offset], _: Option[PendingState]) =>
        lastCommittedOffset.asRight[NonFatalCloudSinkError],
      )
  }

  test("processPendingOperations should process all operations and update the index successfully") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("source1", tempLocalFile, "dest1"),
        DeleteOperation("source2", tempRemoteFile, "etag2"),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], any[String], any[String]))
      .thenReturn(Right("etag1"))
    when(storageInterface.deleteFile(any[String], any[String], any[String]))
      .thenReturn(Right(()))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result shouldBe Right(Some(Offset(100)))

    val inOrderVerifier = Mockito.inOrder(storageInterface, fnIndexUpdate)
    inOrderVerifier.verify(storageInterface).uploadFile(any[UploadableFile], anyString(), anyString())
    inOrderVerifier.verify(storageInterface).deleteFile(anyString(), anyString(), any[String])
    inOrderVerifier.verify(fnIndexUpdate).apply(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]])
  }

  test("processPendingOperations should return no pending operations if upload fails due to missing files") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("source1", tempLocalFile, "dest1"),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result shouldBe Right(Some(Offset(50)))

    val inOrderVerifier = Mockito.inOrder(storageInterface, fnIndexUpdate)
    inOrderVerifier.verify(storageInterface).uploadFile(any[UploadableFile], anyString(), anyString())
  }

  test("processPendingOperations should return error if upload fails") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        UploadOperation("source1", tempLocalFile, "dest1"),
      ),
    )

    when(storageInterface.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempLocalFile)))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result shouldBe Right(Some(Offset(50)))

    val inOrderVerifier = Mockito.inOrder(storageInterface, fnIndexUpdate)
    inOrderVerifier.verify(storageInterface).uploadFile(any[UploadableFile], anyString(), anyString())
    inOrderVerifier.verify(fnIndexUpdate, times(0)).apply(any[TopicPartition],
                                                          any[Option[Offset]],
                                                          any[Option[PendingState]],
    )
  }

  test("processPendingOperations should skip further operations if delete fails") {
    val pendingState = PendingState(
      pendingOffset = Offset(100),
      pendingOperations = NonEmptyList.of(
        DeleteOperation("source1", "bucket1", "etag1"),
        CopyOperation("source2", "bucket1", "dest2", "etag2"),
      ),
    )

    when(storageInterface.deleteFile(anyString(), anyString(), any[String]))
      .thenReturn(Left(FileDeleteError(new Exception("Delete failed"), "source1")))

    val result = pendingOperationsProcessors.processPendingOperations(
      topicPartition,
      Some(Offset(50)),
      pendingState,
      fnIndexUpdate,
    )

    result.left.value.message() shouldBe "Unable to resume processOperations: Delete failed"

    verify(storageInterface).deleteFile(anyString(), anyString(), any[String])
    verifyNoInteractions(fnIndexUpdate)
  }
}
