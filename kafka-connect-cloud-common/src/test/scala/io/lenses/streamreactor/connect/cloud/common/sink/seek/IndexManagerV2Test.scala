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
import cats.data.Validated
import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import io.circe.Decoder
import io.circe.Encoder
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.seek.deprecated.IndexManagerV1
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.storage.FileNotFoundError
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.storage.PathError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchers.anyString
import org.scalatest.Assertion
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

class IndexManagerV2Test
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  implicit val validator:        CloudLocationValidator = (location: CloudLocation) => Validated.valid(location)
  implicit val indexFileDecoder: Decoder[IndexFile]     = IndexFile.indexFileDecoder
  implicit val indexFileEncoder: Encoder[IndexFile]     = IndexFile.indexFileEncoder
  implicit val storageInterface: StorageInterface[_]    = mock[StorageInterface[_]]
  implicit val connectorTaskId:  ConnectorTaskId        = mock[ConnectorTaskId]

  private val oldIndexManager             = mock[IndexManagerV1]
  private val bucketAndPrefixFn           = mock[TopicPartition => Either[SinkError, CloudLocation]]
  private val pendingOperationsProcessors = mock[PendingOperationsProcessors]
  private val indexesDirectoryName        = ".indexes2"

  private var indexManagerV2: IndexManagerV2 = _

  before {
    indexManagerV2 = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
    )(storageInterface, connectorTaskId)

    reset(storageInterface, connectorTaskId, oldIndexManager, bucketAndPrefixFn, pendingOperationsProcessors)
  }

  test("open should return offsets for all topic partitions when they are successfully opened") {
    val topicPartitions = Set(
      Topic("topic1").withPartition(0),
      Topic("topic2").withPartition(1),
    )

    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val result: Either[SinkError, Map[TopicPartition, Option[Offset]]] =
      runOpenForOffset(topicPartitions, bucketAndPrefix)

    result shouldBe Right(Map(
      Topic("topic1").withPartition(0) -> Some(Offset(100)),
      Topic("topic2").withPartition(1) -> Some(Offset(100)),
    ))
  }

  private def runOpenForOffset(topicPartitions: Set[TopicPartition], bucketAndPrefix: CloudLocation) = {
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(storageInterface.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(storageInterface.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    indexManagerV2.open(topicPartitions)
  }

  test("open should create a new index file if none exists for the topic partition") {
    val topicPartition  = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val path            = ".indexes/.locks/topic1/0.lock"

    val result: Either[SinkError, Map[TopicPartition, Option[Offset]]] = runOpen(topicPartition, bucketAndPrefix, path)

    result shouldBe Right(Map(topicPartition -> None))
  }

  private def runOpen(topicPartition: TopicPartition, bucketAndPrefix: CloudLocation, path: String) = {
    when(oldIndexManager.seekOffsetsForTopicPartition(topicPartition)).thenReturn(Option.empty.asRight)
    when(bucketAndPrefixFn(topicPartition)).thenReturn(Right(bucketAndPrefix))
    when(storageInterface.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(storageInterface.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Left(FileNotFoundError(new Exception("Not found"), path)))
    when(
      storageInterface.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "etag")))

    indexManagerV2.open(Set(topicPartition))
  }

  test("update should update the index file with the new offset and pending state") {

    val topicPartition  = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val path            = ".indexes/.locks/topic1/0.lock"

    val openResult: Either[SinkError, Map[TopicPartition, Option[Offset]]] =
      runOpen(topicPartition, bucketAndPrefix, path)
    openResult shouldBe Right(Map(topicPartition -> None))

    // Mock bucketAndPrefixFn to return the correct CloudLocation
    when(bucketAndPrefixFn(topicPartition)).thenReturn(Right(bucketAndPrefix))

    // Mock getBlobAsObject to simulate the index file being found
    when(storageInterface.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    // Mock writeBlobToFile to simulate a successful write
    when(
      storageInterface.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "newEtag")))

    // Call the update method
    val result = indexManagerV2.update(topicPartition, Some(Offset(200)), None)

    // Assert the result is as expected
    result shouldBe Right(Some(Offset(200)))

    // Verify interactions with the storage interface
    verify(storageInterface).getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder))
    verify(storageInterface, times(2)).writeBlobToFile[IndexFile](anyString(),
                                                                  anyString(),
                                                                  any[ObjectWithETag[IndexFile]],
    )(ArgumentMatchers.eq(indexFileEncoder))
  }

  test("getSeekedOffsetForTopicPartition should return the seeked offset for a given topic partition") {

    val topicPartition  = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val openResult: Either[SinkError, Map[TopicPartition, Option[Offset]]] =
      runOpenForOffset(Set(topicPartition), bucketAndPrefix)
    openResult shouldBe Right(Map(topicPartition -> Some(Offset(100))))

    val result = indexManagerV2.getSeekedOffsetForTopicPartition(topicPartition)

    result shouldBe Some(Offset(100))
  }

  test("getSeekedOffsetForTopicPartition should return None if no offset exists for the topic partition") {
    val topicPartition = Topic("topic1").withPartition(0)

    val result = indexManagerV2.getSeekedOffsetForTopicPartition(topicPartition)

    result shouldBe None
  }

  test("IndexManagerV2 uses directoryFileName parameter in lock file path") {
    val (storageInterface, oldIndexManager, pendingProcessors, bucketAndPrefixFn, directoryFileName, topicPartition) =
      setupMocksForLockFilePathTest()

    val indexManager = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingProcessors,
      directoryFileName,
    )(storageInterface, ConnectorTaskId("connector", 1, 0))

    indexManager.open(Set(topicPartition))
    verifyLockFilePathUsed(storageInterface, directoryFileName, topicPartition, "connector")
  }

  private def setupMocksForLockFilePathTest() = {
    val storageInterface  = mock[StorageInterface[_]]
    val oldIndexManager   = mock[IndexManagerV1]
    val pendingProcessors = mock[PendingOperationsProcessors]
    val directoryFileName = "custom-index-dir"
    val topicPartition    = Topic("my-topic").withPartition(2)
    val bucketAndPrefixFn: TopicPartition => Either[SinkError, CloudLocation] =
      _ => Right(CloudLocation("bucket", None))

    when(storageInterface.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(storageInterface.getBlobAsObject[IndexFile](anyString(), anyString())(any[Decoder[IndexFile]]))
      .thenReturn(Left(FileNotFoundError(new Exception("Not found"), "somepath")))
    when(oldIndexManager.seekOffsetsForTopicPartition(any[TopicPartition])).thenReturn(None.asRight)
    when(storageInterface.writeBlobToFile(anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
      any[Encoder[IndexFile]],
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("owner", None, None), "etag")))

    (storageInterface, oldIndexManager, pendingProcessors, bucketAndPrefixFn, directoryFileName, topicPartition)
  }

  private def verifyLockFilePathUsed(
    storageInterface:  StorageInterface[_],
    directoryFileName: String,
    topicPartition:    TopicPartition,
    connectorName:     String,
  ): Assertion = {
    val pathCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(storageInterface).getBlobAsObject[IndexFile](anyString(), pathCaptor.capture())(any[Decoder[IndexFile]])
    val usedPath = pathCaptor.getValue
    usedPath should be(
      s"$directoryFileName/${connectorName}/.locks/${topicPartition.topic}/${topicPartition.partition}.lock",
    )
  }

  test("generateLockFilePath uses the provided directoryFileName") {
    val connectorTaskId   = ConnectorTaskId("my-connector", 1, 0)
    val topicPartition    = Topic("my-topic").withPartition(5)
    val directoryFileName = "my-index-dir"

    val lockFilePath = IndexManagerV2.generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)

    lockFilePath should be(s"$directoryFileName/my-connector/.locks/Topic(my-topic)/5.lock")
  }

  test("open should successfully process pending operations and preserve correct eTag for subsequent updates") {
    val topicPartition  = Topic("my-topic").withPartition(0)
    val bucketAndPrefix = CloudLocation("my-bucket", "prefix".some)

    val pendingOperations = NonEmptyList.of(
      CopyOperation(
        bucket = "my-bucket",
        source =
          ".temp-upload/Topic(my-topic)/0/uuid/my-topic/0/000000000773.avro",
        destination = "my-topic/0/000000000773.avro",
        eTag        = "\"copy-etag\"",
      ),
      DeleteOperation(
        bucket = "my-bucket",
        source =
          ".temp-upload/Topic(my-topic)/0/uuid/my-topic/0/000000000773.avro",
        eTag = "\"copy-etag\"",
      ),
    )

    val pendingState = PendingState(
      pendingOffset     = Offset(773),
      pendingOperations = pendingOperations,
    )

    val indexFile = IndexFile(
      owner           = "old-owner",
      committedOffset = Some(Offset(733)),
      pendingState    = Some(pendingState),
    )

    val objectWithETag = ObjectWithETag(indexFile, "original-etag")

    val si = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(topicPartition)).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(objectWithETag))

    when(si.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(Right(()))
    when(si.deleteFile(anyString(), anyString(), anyString()))
      .thenReturn(Right(()))

    // Return incrementing eTags to simulate GCS generation progression.
    // open() will call update() twice during pending ops (checkpoint after copy, final after delete),
    // then once more for the subsequent update() call.
    val writeResponses = new java.util.concurrent.atomic.AtomicInteger(0)
    val eTags          = Array("etag-after-copy", "etag-after-delete", "etag-after-subsequent-update")
    val offsets        = Array(Some(Offset(733)), Some(Offset(773)), Some(Offset(900)))
    when(
      si.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenAnswer { (_: org.mockito.invocation.InvocationOnMock) =>
      val idx = writeResponses.getAndIncrement()
      Right(ObjectWithETag(IndexFile("owner", offsets(idx), None), eTags(idx)))
    }

    val realPendingOperationsProcessors = new PendingOperationsProcessors(si)

    val realIndexManagerV2 = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      realPendingOperationsProcessors,
      indexesDirectoryName,
    )(si, connectorTaskId)

    val result = realIndexManagerV2.open(Set(topicPartition))
    result.isRight shouldBe true
    result.value shouldBe Map(topicPartition -> Some(Offset(773)))

    // Verify a subsequent update() succeeds (it would fail with stale eTag before the fix).
    val updateResult = realIndexManagerV2.update(topicPartition, Some(Offset(900)), None)
    updateResult.isRight shouldBe true
    updateResult.value shouldBe Some(Offset(900))

    // The third writeBlobToFile call (for the subsequent update) should use "etag-after-delete" (the latest),
    // not "original-etag" (the stale one). Capture and verify.
    val captor = ArgumentCaptor.forClass(classOf[ObjectWithETag[IndexFile]])
    verify(si, times(3)).writeBlobToFile[IndexFile](anyString(), anyString(), captor.capture())(
      ArgumentMatchers.eq(indexFileEncoder),
    )
    val thirdCall = captor.getAllValues.get(2)
    thirdCall.eTag shouldBe "etag-after-delete"
  }

  test("open with cancelPending on upload failure should clear pending state and allow subsequent updates") {
    val topicPartition  = Topic("my-topic").withPartition(0)
    val bucketAndPrefix = CloudLocation("my-bucket", "prefix".some)
    val tempFile        = new java.io.File("/nonexistent/path/to/staging-file")

    val pendingOperations = NonEmptyList.of[FileOperation](
      UploadOperation("my-bucket", tempFile, ".temp-upload/my-topic/0/staging.avro"),
      CopyOperation("my-bucket", ".temp-upload/my-topic/0/staging.avro", "my-topic/0/000000000500.avro", "placeholder"),
      DeleteOperation("my-bucket", ".temp-upload/my-topic/0/staging.avro", "placeholder"),
    )

    val pendingState = PendingState(
      pendingOffset     = Offset(500),
      pendingOperations = pendingOperations,
    )

    val indexFile = IndexFile(
      owner           = "dead-worker-owner",
      committedOffset = Some(Offset(450)),
      pendingState    = Some(pendingState),
    )

    val si = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(topicPartition)).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(indexFile, "original-etag")))

    // Upload will fail with NonExistingFileError (local file from dead worker doesn't exist)
    when(si.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(tempFile)))

    // writeBlobToFile: first call clears pending state (cancelPending), second is the subsequent update
    val cancelWriteResponses = new java.util.concurrent.atomic.AtomicInteger(0)
    val cancelETags          = Array("etag-after-cancel", "etag-after-update")
    when(
      si.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenAnswer { (_: org.mockito.invocation.InvocationOnMock) =>
      val idx  = cancelWriteResponses.getAndIncrement()
      val eTag = cancelETags(idx)
      Right(ObjectWithETag(IndexFile("owner", Some(Offset(450)), None), eTag))
    }

    val realPendingOperationsProcessors = new PendingOperationsProcessors(si)
    val realIndexManagerV2 = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      realPendingOperationsProcessors,
      indexesDirectoryName,
    )(si, connectorTaskId)

    val result = realIndexManagerV2.open(Set(topicPartition))
    result.isRight shouldBe true
    // cancelPending with further ops calls fnIndexUpdate which returns the committed offset
    result.value shouldBe Map(topicPartition -> Some(Offset(450)))

    // Verify subsequent update works (would fail if eTag was stale)
    val updateResult = realIndexManagerV2.update(topicPartition, Some(Offset(600)), None)
    updateResult.isRight shouldBe true

    // The second writeBlobToFile should use "etag-after-cancel", not "original-etag"
    val captor = ArgumentCaptor.forClass(classOf[ObjectWithETag[IndexFile]])
    verify(si, times(2)).writeBlobToFile[IndexFile](anyString(), anyString(), captor.capture())(
      ArgumentMatchers.eq(indexFileEncoder),
    )
    captor.getAllValues.get(1).eTag shouldBe "etag-after-cancel"
  }

  test("open should handle many partitions concurrently without Index not found errors") {
    val numPartitions   = 50
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val topicPartitions = (0 until numPartitions).map(i => Topic("stress-topic").withPartition(i)).toSet

    val si = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))

    // Each partition has pending state to force the codepath through processPendingOperations -> update()
    topicPartitions.foreach { tp =>
      val pendingOps = NonEmptyList.of(
        CopyOperation("bucket", s".temp/${tp.partition}/staging.avro", s"final/${tp.partition}/data.avro", "copy-etag"),
        DeleteOperation("bucket", s".temp/${tp.partition}/staging.avro", "copy-etag"),
      )
      val idx = IndexFile("old-owner",
                          Some(Offset(tp.partition.toLong * 10)),
                          Some(PendingState(Offset(tp.partition.toLong * 10 + 5), pendingOps)),
      )
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith(s"${tp.partition}.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(idx, s"etag-${tp.partition}")))
    }

    when(si.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(Right(()))
    when(si.deleteFile(anyString(), anyString(), anyString()))
      .thenReturn(Right(()))

    val writeCounter = new java.util.concurrent.atomic.AtomicInteger(0)
    when(
      si.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenAnswer { (_: org.mockito.invocation.InvocationOnMock) =>
      val count = writeCounter.incrementAndGet()
      Right(ObjectWithETag(IndexFile("owner", Some(Offset(count.toLong)), None), s"etag-write-$count"))
    }

    val realPendingOperationsProcessors = new PendingOperationsProcessors(si)
    val realIndexManagerV2 = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      realPendingOperationsProcessors,
      indexesDirectoryName,
    )(si, connectorTaskId)

    val result = realIndexManagerV2.open(topicPartitions)
    result.isRight shouldBe true
    result.value.size shouldBe numPartitions

    // Verify all partitions can be updated after open (would fail with "Index not found" before the fix)
    topicPartitions.foreach { tp =>
      val updateResult = realIndexManagerV2.update(tp, Some(Offset(9999)), None)
      withClue(s"update for partition ${tp.partition} should succeed: ") {
        updateResult.isRight shouldBe true
      }
    }
  }

  test("migrateOldPathIfExists should move old path to new path when old path exists") {
    val topicPartition  = Topic("test-topic").withPartition(0)
    val bucketAndPrefix = CloudLocation("test-bucket", "test-prefix".some)
    val oldPath         = ".indexes2/.locks/Topic(test-topic)/0.lock"
    val newPath         = ".indexes2/connector-name/.locks/Topic(test-topic)/0.lock"

    // Mock bucketAndPrefixFn to return the CloudLocation
    when(bucketAndPrefixFn(topicPartition)).thenReturn(Right(bucketAndPrefix))

    // Mock pathExists to return true (old path exists)
    when(storageInterface.pathExists("test-bucket", oldPath)).thenReturn(Right(true))

    // Mock mvFile to return success - use flexible matchers to handle path variations
    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), ArgumentMatchers.eq(None)))
      .thenReturn(Right(()))

    // Mock the rest of the open flow
    when(storageInterface.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Left(FileNotFoundError(new Exception("Not found"), newPath)))
    when(oldIndexManager.seekOffsetsForTopicPartition(topicPartition)).thenReturn(Option.empty.asRight)
    when(
      storageInterface.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "etag")))

    val result = indexManagerV2.open(Set(topicPartition))

    result shouldBe Right(Map(topicPartition -> None))

    // Verify that pathExists was called with the old path
    verify(storageInterface).pathExists("test-bucket", oldPath)

    // Verify that mvFile was called to move from old path to new path
    verify(storageInterface).mvFile(anyString(), anyString(), anyString(), anyString(), ArgumentMatchers.eq(None))
  }

  test("migrateOldPathIfExists should do nothing when old path does not exist") {
    val topicPartition  = Topic("test-topic").withPartition(0)
    val bucketAndPrefix = CloudLocation("test-bucket", "test-prefix".some)
    val oldPath         = ".indexes2/.locks/Topic(test-topic)/0.lock"
    val newPath         = ".indexes2/connector-name/.locks/Topic(test-topic)/0.lock"

    // Mock bucketAndPrefixFn to return the CloudLocation
    when(bucketAndPrefixFn(topicPartition)).thenReturn(Right(bucketAndPrefix))

    // Mock pathExists to return false (old path does not exist)
    when(storageInterface.pathExists("test-bucket", oldPath)).thenReturn(Right(false))

    // Mock the rest of the open flow
    when(storageInterface.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Left(FileNotFoundError(new Exception("Not found"), newPath)))
    when(oldIndexManager.seekOffsetsForTopicPartition(topicPartition)).thenReturn(Option.empty.asRight)
    when(
      storageInterface.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "etag")))

    val result = indexManagerV2.open(Set(topicPartition))

    result shouldBe Right(Map(topicPartition -> None))

    // Verify that pathExists was called with the old path
    verify(storageInterface).pathExists("test-bucket", oldPath)

    // Verify that mvFile was NOT called since old path doesn't exist
    verify(storageInterface, never).mvFile(anyString(),
                                           anyString(),
                                           anyString(),
                                           anyString(),
                                           ArgumentMatchers.eq(None),
    )
  }

  test("migrateOldPathIfExists should propagate error when pathExists returns an error") {
    val topicPartition  = Topic("test-topic").withPartition(0)
    val bucketAndPrefix = CloudLocation("test-bucket", "test-prefix".some)
    val oldPath         = ".indexes2/.locks/Topic(test-topic)/0.lock"
    val pathError       = PathError(new RuntimeException("Storage error"), oldPath)

    // Mock bucketAndPrefixFn to return the CloudLocation
    when(bucketAndPrefixFn(topicPartition)).thenReturn(Right(bucketAndPrefix))

    // Mock pathExists to return an error
    when(storageInterface.pathExists("test-bucket", oldPath)).thenReturn(Left(pathError))

    val result = indexManagerV2.open(Set(topicPartition))

    result.isLeft shouldBe true
    result.left.getOrElse(throw new RuntimeException("Expected Left but got Right")) shouldBe a[FatalCloudSinkError]
    result.left.getOrElse(throw new RuntimeException("Expected Left but got Right")).asInstanceOf[
      FatalCloudSinkError,
    ].message shouldBe "error loading file (.indexes2/.locks/Topic(test-topic)/0.lock) Storage error"

    // Verify that pathExists was called with the old path
    verify(storageInterface).pathExists("test-bucket", oldPath)

    // Verify that mvFile was NOT called since pathExists failed
    verify(storageInterface, never).mvFile(anyString(),
                                           anyString(),
                                           anyString(),
                                           anyString(),
                                           ArgumentMatchers.eq(None),
    )
  }

  test("migrateOldPathIfExists should propagate error when mvFile returns an error") {
    val topicPartition  = Topic("test-topic").withPartition(0)
    val bucketAndPrefix = CloudLocation("test-bucket", "test-prefix".some)
    val oldPath         = ".indexes2/.locks/Topic(test-topic)/0.lock"
    val newPath         = ".indexes2/connector-name/.locks/Topic(test-topic)/0.lock"
    val moveError       = FileMoveError(new RuntimeException("Move error"), oldPath, newPath)

    // Mock bucketAndPrefixFn to return the CloudLocation
    when(bucketAndPrefixFn(topicPartition)).thenReturn(Right(bucketAndPrefix))

    // Mock pathExists to return true (old path exists)
    when(storageInterface.pathExists("test-bucket", oldPath)).thenReturn(Right(true))

    // Mock mvFile to return an error - use anyString() to match any path
    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), ArgumentMatchers.eq(None)))
      .thenReturn(Left(moveError))

    val result = indexManagerV2.open(Set(topicPartition))

    result.isLeft shouldBe true
    result.left.getOrElse(throw new RuntimeException("Expected Left but got Right")) shouldBe a[FatalCloudSinkError]
    result.left.getOrElse(throw new RuntimeException("Expected Left but got Right")).asInstanceOf[
      FatalCloudSinkError,
    ].message shouldBe "error moving file from (.indexes2/.locks/Topic(test-topic)/0.lock) to (.indexes2/connector-name/.locks/Topic(test-topic)/0.lock) Move error"

    // Verify that pathExists was called with the old path
    verify(storageInterface).pathExists("test-bucket", oldPath)

    // Verify that mvFile was called and failed
    verify(storageInterface).mvFile(anyString(), anyString(), anyString(), anyString(), ArgumentMatchers.eq(None))
  }
}
