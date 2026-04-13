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
import io.lenses.streamreactor.connect.cloud.common.storage.GeneralFileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.storage.PathError
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString

import java.time.Instant
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
    reset(storageInterface, connectorTaskId, oldIndexManager, bucketAndPrefixFn, pendingOperationsProcessors)

    indexManagerV2 = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(storageInterface, connectorTaskId)
  }

  after {
    if (indexManagerV2 != null) indexManagerV2.close()
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
    when(storageInterface.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

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
    when(storageInterface.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

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
      gcIntervalSeconds = Int.MaxValue,
    )(storageInterface, ConnectorTaskId("connector", 1, 0))

    try {
      indexManager.open(Set(topicPartition))
      verifyLockFilePathUsed(storageInterface, directoryFileName, topicPartition, "connector")
    } finally indexManager.close()
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
    when(storageInterface.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

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
    when(si.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

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
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
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
    } finally realIndexManagerV2.close()
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
    when(si.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

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
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
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
    } finally realIndexManagerV2.close()
  }

  test("open should handle many partitions concurrently without Index not found errors") {
    val numPartitions   = 50
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val topicPartitions = (0 until numPartitions).map(i => Topic("stress-topic").withPartition(i)).toSet

    val si = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

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
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
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
    } finally realIndexManagerV2.close()
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
    when(storageInterface.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

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
    when(storageInterface.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

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

  // Phase 1c: Granular lock CRUD tests

  test("generateGranularLockFilePath should produce path under partition subdirectory") {
    val taskId = ConnectorTaskId("connector", 1, 0)
    val tp     = Topic("topic").withPartition(0)
    val path   = IndexManagerV2.generateGranularLockFilePath(taskId, tp, "date%3D12_00", ".indexes")
    path shouldBe ".indexes/connector/.locks/Topic(topic)/0/date%3D12_00.lock"
  }

  test("getSeekedOffsetForPartitionKey returns None when no granular lock exists") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    runOpenForOffset(Set(tp), bucketAndPrefix)

    // After open, override getBlobAsObject to return FileNotFoundError for granular lock paths
    when(
      storageInterface.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date=12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Left(FileNotFoundError(new Exception("Not found"), "granular-path")))

    indexManagerV2.getSeekedOffsetForPartitionKey(tp, "date=12_00") shouldBe Right(None)
  }

  test("updateForPartitionKey creates a granular lock and getSeekedOffsetForPartitionKey reads it") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    runOpenForOffset(Set(tp), bucketAndPrefix)

    when(
      storageInterface.writeBlobToFile[IndexFile](anyString(), anyString(), any[NoOverwriteExistingObject[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "granular-etag")))

    indexManagerV2.ensureGranularLock(tp, "date=12_00")

    when(
      storageInterface.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "granular-etag-2")))

    val result = indexManagerV2.updateForPartitionKey(tp, "date=12_00", Some(Offset(100)), None)
    result shouldBe Right(Some(Offset(100)))

    indexManagerV2.getSeekedOffsetForPartitionKey(tp, "date=12_00") shouldBe Right(Some(Offset(100)))
  }

  test("updateMasterLock writes globalSafeOffset - 1 to master lock path") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    runOpenForOffset(Set(tp), bucketAndPrefix)

    val captor = ArgumentCaptor.forClass(classOf[ObjectWithETag[IndexFile]])
    when(
      storageInterface.writeBlobToFile[IndexFile](anyString(), anyString(), captor.capture())(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(49)), None), "new-etag")))

    val result = indexManagerV2.updateMasterLock(tp, Offset(50))
    result shouldBe Right(())

    val captured = captor.getValue
    captured.wrappedObject.committedOffset shouldBe Some(Offset(49))
    captured.wrappedObject.pendingState shouldBe None
  }

  test("updateMasterLock writes committedOffset = None when globalSafeOffset is 0") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    runOpenForOffset(Set(tp), bucketAndPrefix)

    val captor = ArgumentCaptor.forClass(classOf[ObjectWithETag[IndexFile]])
    when(
      storageInterface.writeBlobToFile[IndexFile](anyString(), anyString(), captor.capture())(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "new-etag")))

    val result = indexManagerV2.updateMasterLock(tp, Offset(0))
    result shouldBe Right(())

    val captured = captor.getValue
    captured.wrappedObject.committedOffset shouldBe None
    captured.wrappedObject.pendingState shouldBe None
  }

  test("cleanUpObsoleteLocks is no-op when no granular locks exist") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    runOpenForOffset(Set(tp), bucketAndPrefix)

    val result = indexManagerV2.cleanUpObsoleteLocks(tp, Offset(50), Set.empty)
    result shouldBe Right(())
  }

  // Phase 1g: Sanitize utility tests

  test("sanitize URL-encodes slashes and special characters") {
    import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
    WriterManager.sanitize("2024/01/01") shouldBe "2024%2F01%2F01"
  }

  test("sanitize is deterministic") {
    import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
    WriterManager.sanitize("test/value") shouldBe WriterManager.sanitize("test/value")
  }

  test("sanitize is collision-resistant") {
    import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
    WriterManager.sanitize("a/b") should not be WriterManager.sanitize("a_b")
    WriterManager.sanitize("x_y") should not be WriterManager.sanitize("x") + "_" + WriterManager.sanitize("y")
  }

  test("derivePartitionKey is collision-resistant for underscores in values and field names") {
    import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
    import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
    import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
    val fieldA  = ValuePartitionField(PartitionNamePath("a"))
    val fieldZ  = ValuePartitionField(PartitionNamePath("z"))
    val fieldYZ = ValuePartitionField(PartitionNamePath("y_z"))
    val key1    = WriterManager.derivePartitionKey(Map(fieldA -> "x_y", fieldZ -> "1"))
    val key2    = WriterManager.derivePartitionKey(Map(fieldA -> "x", fieldYZ -> "1"))
    key1 should not be key2
  }

  test("partitionKey derivation sorts by field name") {
    import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
    import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
    import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
    val dateField = ValuePartitionField(PartitionNamePath("date"))
    val hourField = ValuePartitionField(PartitionNamePath("hour"))
    val key1      = WriterManager.derivePartitionKey(Map(dateField -> "2024", hourField -> "12"))
    val key2      = WriterManager.derivePartitionKey(Map(hourField -> "12", dateField -> "2024"))
    key1 shouldBe key2
  }

  // Lazy loading and cache tests
  //
  // This group verifies the behavior introduced to avoid eager startup reads:
  //   - open() must NOT enumerate or read any granular lock files (no listKeysRecursive call)
  //   - getSeekedOffsetForPartitionKey fetches from storage on the first call (cache miss)
  //   - subsequent calls for the same key return the cached value without touching storage
  //   - evictGranularLock removes a single entry; the next lookup re-fetches from storage
  //   - evictAllGranularLocks removes all entries for a topic-partition
  //   - the granular cache is unbounded and grows without automatic eviction

  test("open should NOT read granular locks eagerly") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // Old code called listKeysRecursive to enumerate granular lock files under the partition prefix.
      // With lazy loading that call is gone; verifying it was never made proves eagerness was removed.
      verify(si, never).listKeysRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  test("getSeekedOffsetForPartitionKey loads on demand on cache miss") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    // Master lock for open()
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    // Now mock the granular lock path to return a specific offset
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "granular-etag")))

    try {
      val result = im.getSeekedOffsetForPartitionKey(tp, "date%3D12_00")
      result shouldBe Right(Some(Offset(200)))

      verify(si).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  test("getSeekedOffsetForPartitionKey returns cached value on cache hit") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "granular-etag")))

    try {
      im.getSeekedOffsetForPartitionKey(tp, "date%3D12_00") shouldBe Right(Some(Offset(200)))
      im.getSeekedOffsetForPartitionKey(tp, "date%3D12_00") shouldBe Right(Some(Offset(200)))

      // getBlobAsObject for the granular path should be called only once (second call hits cache)
      verify(si, times(1)).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  test("evictGranularLock removes entry from cache") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "granular-etag")))

    im.getSeekedOffsetForPartitionKey(tp, "date%3D12_00") shouldBe Right(Some(Offset(200)))

    im.evictGranularLock(tp, "date%3D12_00")

    // After eviction, the next call should trigger another storage read
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(300)), None), "granular-etag-2")))

    try {
      im.getSeekedOffsetForPartitionKey(tp, "date%3D12_00") shouldBe Right(Some(Offset(300)))

      verify(si, times(2)).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  test("evictAllGranularLocks removes all entries for a topic-partition") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    // Load 3 granular locks
    Seq("pk1", "pk2", "pk3").foreach { pk =>
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains(s"/0/$pk.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), s"etag-$pk")))
      im.getSeekedOffsetForPartitionKey(tp, pk) shouldBe Right(Some(Offset(200)))
    }

    try {
      im.granularCacheSize shouldBe 3

      im.evictAllGranularLocks(tp)

      im.granularCacheSize shouldBe 0
    } finally im.close()
  }

  test("granular cache grows unbounded without automatic eviction") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    Seq("pk1", "pk2", "pk3").foreach { pk =>
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains(s"/0/$pk.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), s"etag-$pk")))
    }

    im.getSeekedOffsetForPartitionKey(tp, "pk1") shouldBe Right(Some(Offset(200)))
    im.getSeekedOffsetForPartitionKey(tp, "pk2") shouldBe Right(Some(Offset(200)))
    im.granularCacheSize shouldBe 2

    // Loading a third entry does NOT evict pk1 — cache is unbounded
    im.getSeekedOffsetForPartitionKey(tp, "pk3") shouldBe Right(Some(Offset(200)))
    im.granularCacheSize shouldBe 3

    try {
      // pk1 is still cached — no re-read from storage needed
      im.getSeekedOffsetForPartitionKey(tp, "pk1") shouldBe Right(Some(Offset(200)))
      verify(si, times(1)).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk1.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  // --- ensureGranularLock tests ---

  test("ensureGranularLock succeeds and populates cache when lock file already exists in storage") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    // tryOpen finds the granular lock -- it already exists from a prior run
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "existing-granular-etag")))

    val result = im.ensureGranularLock(tp, "date%3D12_00")
    result shouldBe Right(())

    // writeBlobToFile should NOT have been called for the granular lock path since the file already existed
    verify(si, never).writeBlobToFile[IndexFile](
      anyString(),
      ArgumentMatchers.contains("/0/date%3D12_00.lock"),
      any[NoOverwriteExistingObject[IndexFile]],
    )(ArgumentMatchers.eq(indexFileEncoder))

    try {
      // The cache should be populated from the tryOpen, so getSeekedOffsetForPartitionKey is a cache hit
      im.getSeekedOffsetForPartitionKey(tp, "date%3D12_00") shouldBe Right(Some(Offset(50)))
      // Verify getBlobAsObject for the granular path was called exactly once (by ensureGranularLock),
      // not twice (no additional call from getSeekedOffsetForPartitionKey)
      verify(si, times(1)).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  test("ensureGranularLock creates file when it does not exist") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    // tryOpen returns FileNotFoundError -- file does not exist yet
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/date%3D12_00.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Left(FileNotFoundError(new Exception("Not found"), "granular-path")))
    when(
      si.writeBlobToFile[IndexFile](anyString(),
                                    ArgumentMatchers.contains("/0/date%3D12_00.lock"),
                                    any[NoOverwriteExistingObject[IndexFile]],
      )(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "new-granular-etag")))

    try {
      val result = im.ensureGranularLock(tp, "date%3D12_00")
      result shouldBe Right(())

      verify(si).writeBlobToFile[IndexFile](
        anyString(),
        ArgumentMatchers.contains("/0/date%3D12_00.lock"),
        any[NoOverwriteExistingObject[IndexFile]],
      )(ArgumentMatchers.eq(indexFileEncoder))
    } finally im.close()
  }

  test("ensureGranularLock resolves PendingState and caches the resolved offset") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    val pp = mock[PendingOperationsProcessors]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "master-etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pp,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    val pendingOps = NonEmptyList.of[FileOperation](
      CopyOperation("bucket", "temp-path", "final-path", "placeholder"),
      DeleteOperation("bucket", "temp-path", "placeholder"),
    )
    val pendingIndexFile = IndexFile("lockOwner", Some(Offset(80)), Some(PendingState(Offset(90), pendingOps)))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(pendingIndexFile, "granular-etag-v1")))

    // Mock writeBlobToFile so the fnUpdate callback (updateForPartitionKey) can write the resolved lock
    when(
      si.writeBlobToFile[IndexFile](anyString(),
                                    ArgumentMatchers.contains("/0/pk-pending.lock"),
                                    any[ObjectWithETag[IndexFile]],
      )(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(90)), None), "granular-etag-v2")))

    // processPendingOperations invokes fnUpdate to write the resolved offset, then returns it
    when(
      pp.processPendingOperations(
        ArgumentMatchers.eq(tp),
        ArgumentMatchers.eq(Some(Offset(80))),
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
      ),
    ).thenAnswer { (invocation: org.mockito.invocation.InvocationOnMock) =>
      val fnUpdate = invocation.getArgument[(
        TopicPartition,
        Option[Offset],
        Option[PendingState],
      ) => Either[SinkError, Option[Offset]]](3)
      fnUpdate(tp, Some(Offset(90)), None)
    }

    try {
      val result = im.ensureGranularLock(tp, "pk-pending")
      result shouldBe Right(())

      verify(pp).processPendingOperations(
        ArgumentMatchers.eq(tp),
        ArgumentMatchers.eq(Some(Offset(80))),
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
      )

      // getSeekedOffsetForPartitionKey should be a cache hit (no second storage read)
      // and should return the resolved offset, not the stale pre-pending one
      val seeked = im.getSeekedOffsetForPartitionKey(tp, "pk-pending")
      seeked shouldBe Right(Some(Offset(90)))

      verify(si, times(1)).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  test("ensureGranularLock evicts cache entry when PendingState resolution fails") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    val pp = mock[PendingOperationsProcessors]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "master-etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pp,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    val pendingOps = NonEmptyList.of[FileOperation](
      CopyOperation("bucket", "temp-path", "final-path", "placeholder"),
      DeleteOperation("bucket", "temp-path", "placeholder"),
    )
    val pendingIndexFile = IndexFile("lockOwner", Some(Offset(80)), Some(PendingState(Offset(90), pendingOps)))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(pendingIndexFile, "granular-etag-v1")))

    when(
      pp.processPendingOperations(
        ArgumentMatchers.eq(tp),
        ArgumentMatchers.eq(Some(Offset(80))),
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
      ),
    ).thenReturn(Left(FatalCloudSinkError("transient cloud error", tp)))

    try {
      val result = im.ensureGranularLock(tp, "pk-pending")
      result.isLeft shouldBe true

      im.granularCacheSize shouldBe 0
    } finally im.close()
  }

  test("ensureGranularLock resolves PendingState on retry re-read after write conflict") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    val pp = mock[PendingOperationsProcessors]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "master-etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pp,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    val pendingOps = NonEmptyList.of[FileOperation](
      CopyOperation("bucket", "temp-path", "final-path", "placeholder"),
      DeleteOperation("bucket", "temp-path", "placeholder"),
    )
    val pendingLock = Right(
      ObjectWithETag(
        IndexFile("lockOwner", Some(Offset(80)), Some(PendingState(Offset(90), pendingOps))),
        "granular-etag-conflict",
      ),
    )
    val notFound: Either[FileNotFoundError, ObjectWithETag[IndexFile]] =
      Left(FileNotFoundError(new Exception("Not found"), "granular-path"))

    // First tryOpen returns FileNotFoundError, retry re-read returns lock with PendingState
    org.mockito.Mockito.doReturn(notFound, pendingLock)
      .when(si).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-conflict.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )

    // Write fails (another task created it between read and write)
    org.mockito.Mockito.doReturn(Left(FileCreateError(new Exception("conflict"), "granular-path")))
      .when(si).writeBlobToFile[IndexFile](anyString(),
                                           ArgumentMatchers.contains("/0/pk-conflict.lock"),
                                           any[NoOverwriteExistingObject[IndexFile]],
      )(
        ArgumentMatchers.eq(indexFileEncoder),
      )

    when(
      pp.processPendingOperations(
        ArgumentMatchers.eq(tp),
        ArgumentMatchers.eq(Some(Offset(80))),
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
      ),
    ).thenReturn(Right(Some(Offset(90))))

    try {
      val result = im.ensureGranularLock(tp, "pk-conflict")
      result shouldBe Right(())

      verify(pp).processPendingOperations(
        ArgumentMatchers.eq(tp),
        ArgumentMatchers.eq(Some(Offset(80))),
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
      )
    } finally im.close()
  }

  // --- cleanUpObsoleteLocks tests ---

  test("cleanUpObsoleteLocks evicts cache immediately but defers cloud deletion to background drain") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      Seq("pk-old", "pk-new").zip(Seq(Offset(50), Offset(200))).foreach { case (pk, offset) =>
        when(
          si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains(s"/0/$pk.lock"))(
            ArgumentMatchers.eq(indexFileDecoder),
          ),
        )
          .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(offset), None), s"etag-$pk")))
        im.getSeekedOffsetForPartitionKey(tp, pk) shouldBe Right(Some(offset))
      }

      im.granularCacheSize shouldBe 2

      val result = im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty)
      result shouldBe Right(())

      // Cache should be evicted immediately
      im.granularCacheSize shouldBe 1

      // But deleteFiles should NOT have been called yet (deferred to background drain)
      verify(si, never).deleteFiles(anyString(), any[Seq[String]])

      // Now trigger the drain
      when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))
      im.drainGcQueue()

      // After drain, deleteFiles should have been called with pk-old path
      verify(si).deleteFiles(anyString(), ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("pk-old"))))
      verify(si, never).deleteFiles(anyString(), ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("pk-new"))))
    } finally {
      im.close()
    }
  }

  test("cleanUpObsoleteLocks does NOT delete eTag-only entries for active writers") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // tryOpen returns FileNotFoundError for the granular lock -- file does not exist yet
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-etag-only.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Left(FileNotFoundError(new Exception("Not found"), "pk-etag-only-path")))
      when(
        si.writeBlobToFile[IndexFile](anyString(),
                                      ArgumentMatchers.contains("/0/pk-etag-only.lock"),
                                      any[NoOverwriteExistingObject[IndexFile]],
        )(
          ArgumentMatchers.eq(indexFileEncoder),
        ),
      ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "etag-only")))

      im.ensureGranularLock(tp, "pk-etag-only") shouldBe Right(())

      // eTag-only entries have offset = None, so offset.exists(_.value < X) is false.
      // They must NOT be deleted because they belong to active writers that haven't committed yet.
      val result = im.cleanUpObsoleteLocks(tp, Offset(50), Set.empty)
      result shouldBe Right(())

      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally {
      im.close()
    }
  }

  test("cleanUpObsoleteLocks skips partition keys in the active set") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // Load a granular lock with a low committed offset (would normally be deleted)
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-active.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(30)), None), "etag-active")))
      im.getSeekedOffsetForPartitionKey(tp, "pk-active") shouldBe Right(Some(Offset(30)))

      // pk-active is in the active set, so it must not be deleted even though offset 30 < 100
      val result = im.cleanUpObsoleteLocks(tp, Offset(100), Set("pk-active"))
      result shouldBe Right(())

      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
      im.granularCacheSize shouldBe 1
    } finally {
      im.close()
    }
  }

  test("drainGcQueue batches deletes according to gcBatchSize") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
      gcBatchSize       = 2,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // Load 3 granular locks: 2 obsolete (below threshold) and 1 current (above threshold)
      Seq("pk-old-1", "pk-old-2").foreach { pk =>
        when(
          si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains(s"/0/$pk.lock"))(
            ArgumentMatchers.eq(indexFileDecoder),
          ),
        )
          .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(10)), None), s"etag-$pk")))
        im.getSeekedOffsetForPartitionKey(tp, pk) shouldBe Right(Some(Offset(10)))
      }
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains(s"/0/pk-current.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "etag-current")))
      im.getSeekedOffsetForPartitionKey(tp, "pk-current") shouldBe Right(Some(Offset(200)))

      im.granularCacheSize shouldBe 3

      val result = im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty)
      result shouldBe Right(())

      // 2 obsolete items evicted from cache, 1 current remains
      im.granularCacheSize shouldBe 1

      im.drainGcQueue()

      // With gcBatchSize=2 and 2 items, deleteFiles should be called once (batch of 2)
      verify(si, times(1)).deleteFiles(anyString(), any[Seq[String]])
    } finally {
      im.close()
    }
  }

  test("close() performs final drain of queued GC items") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    // Load a granular lock into the cache
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-final.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(10)), None), "etag-pk-final")))
    im.getSeekedOffsetForPartitionKey(tp, "pk-final") shouldBe Right(Some(Offset(10)))

    im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty) shouldBe Right(())

    // deleteFiles not called yet
    verify(si, never).deleteFiles(anyString(), any[Seq[String]])

    // close() should shut down the executor and drain remaining items
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))
    im.close()

    verify(si).deleteFiles(anyString(), ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("pk-final"))))
  }

  // --- updateMasterLock fencing tests ---

  test("updateMasterLock does NOT refresh eTag on write failure, preserving fencing") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag-v1")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    when(
      si.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenReturn(Left(FileCreateError(new Exception("eTag mismatch"), "content")))

    val firstResult = im.updateMasterLock(tp, Offset(101))
    firstResult.isLeft shouldBe true

    // The eTag should NOT have been refreshed from storage -- no re-read after failure
    val captor = ArgumentCaptor.forClass(classOf[ObjectWithETag[IndexFile]])
    when(si.writeBlobToFile[IndexFile](anyString(), anyString(), captor.capture())(
      ArgumentMatchers.eq(indexFileEncoder),
    )).thenReturn(Left(FileCreateError(new Exception("eTag mismatch again"), "content")))

    val secondResult = im.updateMasterLock(tp, Offset(101))
    secondResult.isLeft shouldBe true

    try {
      // The second write should still use the original stale eTag (fencing preserved)
      captor.getValue.eTag shouldBe "etag-v1"
    } finally im.close()
  }

  // --- granular cache grows unbounded without error ---

  test("granular cache holds arbitrary number of entries when no eviction occurs") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "master-etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // Load pk1 into cache
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk1.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "etag-pk1")))
      im.getSeekedOffsetForPartitionKey(tp, "pk1") shouldBe Right(Some(Offset(200)))

      // Load pk2 -- cache size is now 2; both entries remain (no automatic eviction)
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk2.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(300)), None), "etag-pk2")))
      im.getSeekedOffsetForPartitionKey(tp, "pk2") shouldBe Right(Some(Offset(300)))

      im.granularCacheSize shouldBe 2

      // pk1 is still in cache -- updateForPartitionKey should succeed, not FatalCloudSinkError
      when(
        si.writeBlobToFile[IndexFile](anyString(),
                                      ArgumentMatchers.contains("/0/pk1.lock"),
                                      any[ObjectWithETag[IndexFile]],
        )(
          ArgumentMatchers.eq(indexFileEncoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(250)), None), "etag-pk1-v2")))
      val result = im.updateForPartitionKey(tp, "pk1", Some(Offset(250)), None)
      result shouldBe Right(Some(Offset(250)))
    } finally im.close()
  }

  // --- close() GC drain test ---

  test("close() drains remaining GC queue items") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "master-etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    // gcIntervalSeconds very large so the background timer never fires during this test
    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    // Load two granular locks into cache
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-old.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "etag-old")))
    im.getSeekedOffsetForPartitionKey(tp, "pk-old") shouldBe Right(Some(Offset(50)))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-active.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "etag-active")))
    im.getSeekedOffsetForPartitionKey(tp, "pk-active") shouldBe Right(Some(Offset(200)))

    // Enqueue pk-old for GC (offset 50 < globalSafeOffset 100, not in active set)
    im.cleanUpObsoleteLocks(tp, Offset(100), Set("pk-active")) shouldBe Right(())

    // Without calling drainGcQueue(), call close() which should drain the queue
    im.close()

    // Verify deleteFiles was called with the enqueued path
    verify(si).deleteFiles(ArgumentMatchers.eq("bucket"),
                           ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("pk-old"))),
    )
  }

  test("close() drains GC queue even when evictAllGranularLocks was called first (real shutdown sequence)") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "master-etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-old.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "etag-old")))
    im.getSeekedOffsetForPartitionKey(tp, "pk-old") shouldBe Right(Some(Offset(50)))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-active.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "etag-active")))
    im.getSeekedOffsetForPartitionKey(tp, "pk-active") shouldBe Right(Some(Offset(200)))

    im.cleanUpObsoleteLocks(tp, Offset(100), Set("pk-active")) shouldBe Right(())

    // Simulate WriterManager.close(): evict granular caches but do NOT clear seekedOffsets.
    // Before the fix, WriterManager.close() also called clearTopicPartitionState here,
    // which removed seekedOffsets entries and caused drainGcQueue to discard everything.
    im.evictAllGranularLocks(tp)

    im.close()

    verify(si).deleteFiles(ArgumentMatchers.eq("bucket"),
                           ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("pk-old"))),
    )
  }

  test("close() final drain discards all items when clearTopicPartitionState was called first (regression)") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "master-etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-old.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "etag-old")))
    im.getSeekedOffsetForPartitionKey(tp, "pk-old") shouldBe Right(Some(Offset(50)))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-active.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "etag-active")))
    im.getSeekedOffsetForPartitionKey(tp, "pk-active") shouldBe Right(Some(Offset(200)))

    im.cleanUpObsoleteLocks(tp, Offset(100), Set("pk-active")) shouldBe Right(())

    // Simulate the OLD CloudSinkTask.close() sequence: WriterManager.close() followed by
    // clearTopicPartitionState for every partition. This empties seekedOffsets so the final
    // drainGcQueue() in close() discards all items as "partition no longer owned."
    im.evictAllGranularLocks(tp)
    im.clearTopicPartitionState(tp)

    im.close()

    // The final drain should have discarded the item — deleteFiles must NOT be called.
    verify(si, never).deleteFiles(anyString(), any[Seq[String]])
  }

  // --- loadGranularLock PendingState tests ---

  test(
    "loadGranularLock evicts cache entry when processPendingOperations fails, allowing retry to re-read from storage",
  ) {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    val pp = mock[PendingOperationsProcessors]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    // Master lock for open()
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "master-etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pp,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    val pendingOps = NonEmptyList.of[FileOperation](
      UploadOperation("bucket", new java.io.File("staging"), "temp-path"),
      CopyOperation("bucket", "temp-path", "final-path", "placeholder"),
      DeleteOperation("bucket", "temp-path", "placeholder"),
    )
    val pendingIndexFile = IndexFile("lockOwner", Some(Offset(80)), Some(PendingState(Offset(90), pendingOps)))

    // First call: granular lock has PendingState
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(pendingIndexFile, "granular-etag-v1")))

    // processPendingOperations fails (transient cloud error)
    when(
      pp.processPendingOperations(
        ArgumentMatchers.eq(tp),
        ArgumentMatchers.eq(Some(Offset(80))),
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
      ),
    ).thenReturn(Left(FatalCloudSinkError("transient cloud error", tp)))

    val firstResult = im.getSeekedOffsetForPartitionKey(tp, "pk-pending")
    firstResult.isLeft shouldBe true

    // The poisoned cache entry should have been evicted
    im.granularCacheSize shouldBe 0

    // Second call: retry should re-read from storage, this time the PendingState was resolved externally
    val resolvedIndexFile = IndexFile("lockOwner", Some(Offset(90)), None)
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(resolvedIndexFile, "granular-etag-v2")))

    val secondResult = im.getSeekedOffsetForPartitionKey(tp, "pk-pending")
    secondResult shouldBe Right(Some(Offset(90)))

    try {
      // Verify storage was read twice (once per call, no poisoned cache hit)
      verify(si, times(2)).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  test("loadGranularLock resolves PendingState successfully and caches the resolved offset") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    val pp = mock[PendingOperationsProcessors]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "master-etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pp,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    val pendingOps = NonEmptyList.of[FileOperation](
      UploadOperation("bucket", new java.io.File("staging"), "temp-path"),
      CopyOperation("bucket", "temp-path", "final-path", "placeholder"),
      DeleteOperation("bucket", "temp-path", "placeholder"),
    )
    val pendingIndexFile = IndexFile("lockOwner", Some(Offset(80)), Some(PendingState(Offset(90), pendingOps)))

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(pendingIndexFile, "granular-etag-v1")))

    // processPendingOperations succeeds and returns the resolved offset
    when(
      pp.processPendingOperations(
        ArgumentMatchers.eq(tp),
        ArgumentMatchers.eq(Some(Offset(80))),
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
      ),
    ).thenReturn(Right(Some(Offset(90))))

    try {
      val result = im.getSeekedOffsetForPartitionKey(tp, "pk-pending")
      result shouldBe Right(Some(Offset(90)))

      // Subsequent call should hit the cache (processPendingOperations updates it via fnUpdate,
      // but in this mock scenario the cache was populated by the initial put with eTag; the
      // returned offset is from processPendingOperations, not from cache -- verify no second storage read)
      verify(si, times(1)).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  // --- GC reclaim tests (cache-gated deletion) ---

  test("drainGcQueue skips reclaimed keys when a new writer re-populates the cache before drain") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // Load a granular lock into the cache
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-reclaim.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "etag-reclaim-v1")))
      im.getSeekedOffsetForPartitionKey(tp, "pk-reclaim") shouldBe Right(Some(Offset(50)))
      im.granularCacheSize shouldBe 1

      // Enqueue for GC (removes from cache)
      im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty) shouldBe Right(())
      im.granularCacheSize shouldBe 0

      // Simulate a new writer reclaiming this key (re-reads from storage, populates cache)
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-reclaim.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "etag-reclaim-v2")))
      im.ensureGranularLock(tp, "pk-reclaim") shouldBe Right(())
      im.granularCacheSize shouldBe 1

      // Drain: the key is back in the cache, so drainGcQueue should skip the delete
      when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))
      im.drainGcQueue()

      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally {
      im.close()
    }
  }

  test("drainGcQueue deletes un-reclaimed keys but skips reclaimed ones in the same batch") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // Load two granular locks
      Seq("pk-gone", "pk-reclaimed").foreach { pk =>
        when(
          si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains(s"/0/$pk.lock"))(
            ArgumentMatchers.eq(indexFileDecoder),
          ),
        )
          .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(30)), None), s"etag-$pk")))
        im.getSeekedOffsetForPartitionKey(tp, pk) shouldBe Right(Some(Offset(30)))
      }
      im.granularCacheSize shouldBe 2

      // Enqueue both for GC
      im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty) shouldBe Right(())
      im.granularCacheSize shouldBe 0

      // Reclaim only pk-reclaimed (simulating a new writer)
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-reclaimed.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(30)), None), "etag-reclaimed-v2")))
      im.ensureGranularLock(tp, "pk-reclaimed") shouldBe Right(())
      im.granularCacheSize shouldBe 1

      // Drain: pk-gone should be deleted, pk-reclaimed should be skipped
      when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))
      im.drainGcQueue()

      val pathCaptor = ArgumentCaptor.forClass(classOf[Seq[String]])
      verify(si, times(1)).deleteFiles(anyString(), pathCaptor.capture())
      val deletedPaths = pathCaptor.getValue
      deletedPaths should have size 1
      deletedPaths.head should include("pk-gone")
      deletedPaths.head should not include "pk-reclaimed"
    } finally {
      im.close()
    }
  }

  // --- Orphan sweep tests ---

  private def createSweepTestManager(
    si:                     StorageInterface[_],
    gcSweepEnabled:         Boolean = true,
    gcSweepIntervalSeconds: Int     = 86400,
    gcSweepMinAgeSeconds:   Int     = 3600,
    gcSweepMaxReads:        Int     = 1000,
  ): IndexManagerV2 =
    new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds      = Int.MaxValue,
      gcSweepEnabled         = gcSweepEnabled,
      gcSweepIntervalSeconds = gcSweepIntervalSeconds,
      gcSweepMinAgeSeconds   = gcSweepMinAgeSeconds,
      gcSweepMaxReads        = gcSweepMaxReads,
    )(si, connectorTaskId)

  private def setupSweepMocks(
    si:              StorageInterface[_],
    tp:              TopicPartition,
    bucketAndPrefix: CloudLocation,
  ): Unit = {
    when(bucketAndPrefixFn(any[TopicPartition]))
      .thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString()))
      .thenReturn(Right(false))
    when(
      si.getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.endsWith(s"${tp.partition}.lock"),
      )(ArgumentMatchers.eq(indexFileDecoder)),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))
    when(
      si.getBlobAsObject[IndexManagerV2.SweepMarker](
        anyString(),
        ArgumentMatchers.contains("sweep-marker"),
      )(any[Decoder[IndexManagerV2.SweepMarker]]),
    ).thenReturn(Left(FileNotFoundError(new Exception("Not found"), "sweep-marker")))
    val _ = when(si.writeStringToFile(anyString(), anyString(), any[UploadableString]))
      .thenReturn(Right(()))
  }

  test("sweep enqueues orphaned lock files below master lock offset") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime      = Instant.now().minusSeconds(7200)
    val orphanPath   = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/orphan-key.lock"
    val orphanMeta   = TestFileMetadata(orphanPath, oldTime)
    val listResponse = ListOfMetadataResponse("bucket", Some("prefix"), Seq(orphanMeta), orphanMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("orphan-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "orphan-etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      verify(si, times(1)).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test("sweep skips files younger than gcSweepMinAgeSeconds") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val recentTime   = Instant.now()
    val recentPath   = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/recent-key.lock"
    val recentMeta   = TestFileMetadata(recentPath, recentTime)
    val listResponse = ListOfMetadataResponse("bucket", Some("prefix"), Seq(recentMeta), recentMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )

    val im = createSweepTestManager(si, gcSweepMinAgeSeconds = 3600)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()

      verify(si, never).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("recent-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  test("sweep skips lock files already in granularCache") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime      = Instant.now().minusSeconds(7200)
    val cachedPath   = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/cached-key.lock"
    val cachedMeta   = TestFileMetadata(cachedPath, oldTime)
    val listResponse = ListOfMetadataResponse("bucket", Some("prefix"), Seq(cachedMeta), cachedMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )

    // Load cached-key into the granular cache
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("cached-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "cached-etag")))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.ensureGranularLock(tp, "cached-key") shouldBe Right(())

      // Reset the mock to track new invocations
      reset(si)
      setupSweepMocks(si, tp, bucketAndPrefix)
      org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                             any[Option[String]],
      )

      im.sweepOrphanedLocks()

      verify(si, never).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("cached-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  test("sweep skips lock files with committedOffset above master offset") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime      = Instant.now().minusSeconds(7200)
    val highPath     = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/high-key.lock"
    val highMeta     = TestFileMetadata(highPath, oldTime)
    val listResponse = ListOfMetadataResponse("bucket", Some("prefix"), Seq(highMeta), highMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("high-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(150)), None), "high-etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test("sweep enqueues orphaned lock at exactly master offset (off-by-one regression)") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime      = Instant.now().minusSeconds(7200)
    val exactPath    = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/exact-key.lock"
    val exactMeta    = TestFileMetadata(exactPath, oldTime)
    val listResponse = ListOfMetadataResponse("bucket", Some("prefix"), Seq(exactMeta), exactMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )
    // Orphan's committedOffset == master lock's committedOffset (both 100).
    // Since seekedOffsets stores globalSafeOffset - 1, this is equivalent to
    // granularOffset < globalSafeOffset, so the orphan should be swept.
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("exact-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "exact-etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      verify(si, times(1)).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test("sweep skips lock files with no committedOffset") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime      = Instant.now().minusSeconds(7200)
    val noOffPath    = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/no-offset.lock"
    val noOffMeta    = TestFileMetadata(noOffPath, oldTime)
    val listResponse = ListOfMetadataResponse("bucket", Some("prefix"), Seq(noOffMeta), noOffMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("no-offset.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "no-off-etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test("sweep respects gcSweepMaxReads cap across all TPs") {
    val tp1             = Topic("topic1").withPartition(0)
    val tp2             = Topic("topic1").withPartition(1)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag0")))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("1.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag1")))
    when(
      si.getBlobAsObject[IndexManagerV2.SweepMarker](anyString(), ArgumentMatchers.contains("sweep-marker"))(
        any[Decoder[IndexManagerV2.SweepMarker]],
      ),
    )
      .thenReturn(Left(FileNotFoundError(new Exception("Not found"), "sweep-marker")))
    when(si.writeStringToFile(anyString(), anyString(), any[UploadableString])).thenReturn(Right(()))

    val oldTime = Instant.now().minusSeconds(7200)

    def makeLockFiles(tp: TopicPartition, count: Int): ListOfMetadataResponse[TestFileMetadata] = {
      val files = (1 to count).map { i =>
        val path = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/pk-$i.lock"
        TestFileMetadata(path, oldTime)
      }
      ListOfMetadataResponse("bucket", Some("prefix"), files, files.head)
    }

    // Return 3 lock files for each TP listing
    val allFiles = makeLockFiles(tp1, 3)
    org.mockito.Mockito.doReturn(Right(Some(allFiles))).when(si).listFileMetaRecursive(anyString(), any[Option[String]])

    // Each lock file read returns a low offset (below master)
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.matches(".*pk-\\d+\\.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(10)), None), "pk-etag")))

    val im = createSweepTestManager(si, gcSweepMaxReads = 2)
    try {
      im.open(Set(tp1, tp2))
      im.sweepOrphanedLocks()

      // Only 2 lock file reads should have been made total
      verify(si, times(2)).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.matches(".*pk-\\d+\\.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
    } finally im.close()
  }

  test("sweep reads and respects marker file timing") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    // Marker says next run is in the future
    val futureMarker = IndexManagerV2.SweepMarker(System.currentTimeMillis(), System.currentTimeMillis() + 86400000L)
    when(
      si.getBlobAsObject[IndexManagerV2.SweepMarker](anyString(), ArgumentMatchers.contains("sweep-marker"))(
        any[Decoder[IndexManagerV2.SweepMarker]],
      ),
    )
      .thenReturn(Right(ObjectWithETag(futureMarker, "marker-etag")))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()

      // Should NOT have listed files (marker says not yet due)
      verify(si, never).listFileMetaRecursive(anyString(), any[Option[String]])

      // Now change marker to be in the past
      val pastMarker =
        IndexManagerV2.SweepMarker(System.currentTimeMillis() - 172800000L, System.currentTimeMillis() - 86400000L)
      when(
        si.getBlobAsObject[IndexManagerV2.SweepMarker](anyString(), ArgumentMatchers.contains("sweep-marker"))(
          any[Decoder[IndexManagerV2.SweepMarker]],
        ),
      )
        .thenReturn(Right(ObjectWithETag(pastMarker, "marker-etag")))
      when(si.writeStringToFile(anyString(), anyString(), any[UploadableString])).thenReturn(Right(()))
      org.mockito.Mockito.doReturn(Right(None)).when(si).listFileMetaRecursive(anyString(), any[Option[String]])

      im.sweepOrphanedLocks()

      // Should have listed files now
      verify(si, times(1)).listFileMetaRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  test("sweep writes marker before scanning") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)
    org.mockito.Mockito.doReturn(Right(None)).when(si).listFileMetaRecursive(anyString(), any[Option[String]])

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()

      val inOrder = org.mockito.Mockito.inOrder(si)
      inOrder.verify(si).writeStringToFile(anyString(),
                                           ArgumentMatchers.contains("sweep-marker"),
                                           any[UploadableString],
      )
      inOrder.verify(si).listFileMetaRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  test("sweep skips TP when seekedOffsets returns None") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    // Only open tp, so seekedOffsets only has tp (topic2/0 would have None)
    org.mockito.Mockito.doReturn(Right(None)).when(si).listFileMetaRecursive(anyString(), any[Option[String]])

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()

      // Should only list for tp, not tp2 (which has no seekedOffset)
      verify(si, times(1)).listFileMetaRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  test("sweep treats transient marker read error as not-yet-due") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    // Marker read returns a transient error (not FileNotFoundError)
    when(
      si.getBlobAsObject[IndexManagerV2.SweepMarker](anyString(), ArgumentMatchers.contains("sweep-marker"))(
        any[Decoder[IndexManagerV2.SweepMarker]],
      ),
    )
      .thenReturn(Left(GeneralFileLoadError(new Exception("transient"), "sweep-marker")))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()

      // Should NOT have listed files or written marker (transient error = skip sweep)
      verify(si, never).listFileMetaRecursive(anyString(), any[Option[String]])
      verify(si, never).writeStringToFile(anyString(), anyString(), any[UploadableString])
    } finally im.close()
  }

  test("sweep is disabled when gcSweepEnabled = false") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = createSweepTestManager(si, gcSweepEnabled = false)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()

      // Nothing should happen -- no marker reads, no listing, no writes
      verify(si, never).getBlobAsObject[IndexManagerV2.SweepMarker](anyString(),
                                                                    ArgumentMatchers.contains("sweep-marker"),
      )(any[Decoder[IndexManagerV2.SweepMarker]])
      verify(si, never).listFileMetaRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  test("sweep enqueues orphaned lock file with PendingState and committedOffset below master") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime = Instant.now().minusSeconds(7200)
    val pendingPath =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/pending-key.lock"
    val pendingMeta  = TestFileMetadata(pendingPath, oldTime)
    val listResponse = ListOfMetadataResponse("bucket", Some("prefix"), Seq(pendingMeta), pendingMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )

    val pendingState =
      PendingState(Offset(51), NonEmptyList.one(DeleteOperation("bucket", "some/temp/path", "old-etag")))
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("pending-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), Some(pendingState)), "pending-etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      verify(si, times(1)).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test("sweep writes per-TP markers to each partition's bucket") {
    val tp1 = Topic("topic1").withPartition(0)
    val tp2 = Topic("topic2").withPartition(0)
    val si  = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(ArgumentMatchers.eq(tp1)))
      .thenReturn(Right(CloudLocation("bucket-a", "prefix".some)))
    when(bucketAndPrefixFn(ArgumentMatchers.eq(tp2)))
      .thenReturn(Right(CloudLocation("bucket-b", "prefix".some)))
    when(si.pathExists(anyString(), anyString()))
      .thenReturn(Right(false))
    when(
      si.getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.endsWith(".lock"),
      )(ArgumentMatchers.eq(indexFileDecoder)),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))
    when(
      si.getBlobAsObject[IndexManagerV2.SweepMarker](
        anyString(),
        ArgumentMatchers.contains("sweep-marker"),
      )(any[Decoder[IndexManagerV2.SweepMarker]]),
    ).thenReturn(Left(FileNotFoundError(new Exception("Not found"), "sweep-marker")))
    when(si.writeStringToFile(anyString(), anyString(), any[UploadableString]))
      .thenReturn(Right(()))
    org.mockito.Mockito.doReturn(Right(None))
      .when(si).listFileMetaRecursive(anyString(), any[Option[String]])

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp1, tp2))
      im.sweepOrphanedLocks()

      val bucketCaptor = ArgumentCaptor.forClass(classOf[String])
      val pathCaptor   = ArgumentCaptor.forClass(classOf[String])
      verify(si, times(2)).writeStringToFile(
        bucketCaptor.capture(),
        pathCaptor.capture(),
        any[UploadableString],
      )
      val buckets = bucketCaptor.getAllValues
      val paths   = pathCaptor.getAllValues
      val writes  = (0 until buckets.size()).map(i => (buckets.get(i), paths.get(i))).toSet
      writes should contain(
        ("bucket-a",
         s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp1.topic}/${tp1.partition}/sweep-marker.json",
        ),
      )
      writes should contain(
        ("bucket-b",
         s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp2.topic}/${tp2.partition}/sweep-marker.json",
        ),
      )
    } finally im.close()
  }

  test("sweep is suppressed only for TP whose marker is non-expired") {
    val tp1 = Topic("topic1").withPartition(0)
    val tp2 = Topic("topic2").withPartition(0)
    val si  = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(ArgumentMatchers.eq(tp1)))
      .thenReturn(Right(CloudLocation("bucket-a", "prefix".some)))
    when(bucketAndPrefixFn(ArgumentMatchers.eq(tp2)))
      .thenReturn(Right(CloudLocation("bucket-b", "prefix".some)))
    when(si.pathExists(anyString(), anyString()))
      .thenReturn(Right(false))
    when(
      si.getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.endsWith(".lock"),
      )(ArgumentMatchers.eq(indexFileDecoder)),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val futureMarker = IndexManagerV2.SweepMarker(
      System.currentTimeMillis(),
      System.currentTimeMillis() + 86400000L,
    )
    // tp1: no marker (first run) — should be swept
    val tp1MarkerPath =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp1.topic}/${tp1.partition}/sweep-marker.json"
    when(
      si.getBlobAsObject[IndexManagerV2.SweepMarker](
        ArgumentMatchers.eq("bucket-a"),
        ArgumentMatchers.eq(tp1MarkerPath),
      )(any[Decoder[IndexManagerV2.SweepMarker]]),
    ).thenReturn(Left(FileNotFoundError(new Exception("Not found"), "sweep-marker")))
    // tp2: non-expired marker — should be skipped
    val tp2MarkerPath =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp2.topic}/${tp2.partition}/sweep-marker.json"
    when(
      si.getBlobAsObject[IndexManagerV2.SweepMarker](
        ArgumentMatchers.eq("bucket-b"),
        ArgumentMatchers.eq(tp2MarkerPath),
      )(any[Decoder[IndexManagerV2.SweepMarker]]),
    ).thenReturn(Right(ObjectWithETag(futureMarker, "marker-etag")))

    when(si.writeStringToFile(anyString(), anyString(), any[UploadableString]))
      .thenReturn(Right(()))
    org.mockito.Mockito.doReturn(Right(None))
      .when(si).listFileMetaRecursive(anyString(), any[Option[String]])

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp1, tp2))
      im.sweepOrphanedLocks()

      // tp1 should be swept (marker written + files listed)
      verify(si, times(1)).writeStringToFile(
        ArgumentMatchers.eq("bucket-a"),
        ArgumentMatchers.eq(tp1MarkerPath),
        any[UploadableString],
      )
      verify(si, times(1)).listFileMetaRecursive(anyString(), any[Option[String]])

      // tp2's marker should NOT be written (skipped due to non-expired marker)
      verify(si, never).writeStringToFile(
        ArgumentMatchers.eq("bucket-b"),
        ArgumentMatchers.eq(tp2MarkerPath),
        any[UploadableString],
      )
    } finally im.close()
  }
}
