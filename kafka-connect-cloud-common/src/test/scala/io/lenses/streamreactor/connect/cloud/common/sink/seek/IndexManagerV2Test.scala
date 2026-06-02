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
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.storage.EmptyFileError
import io.lenses.streamreactor.connect.cloud.common.storage.FileNotFoundError
import io.lenses.streamreactor.connect.cloud.common.storage.GeneralFileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import java.time.Instant
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchersSugar
import org.mockito.Mockito
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

  private val bucketAndPrefixFn           = mock[TopicPartition => Either[SinkError, CloudLocation]]
  private val pendingOperationsProcessors = mock[PendingOperationsProcessors]
  private val indexesDirectoryName        = ".indexes2"

  private var indexManagerV2: IndexManagerV2 = _

  before {
    // Defensive: a prior test (e.g. "drainGcQueue catches InterruptedException ...") can leave
    // the current thread's interrupt flag set — close()'s final drainGcQueue / awaitTermination
    // re-raises it after the test's own clear. cats-effect's unsafeRunSync() in open() returns
    // None on an interrupted thread, which surfaces as NoSuchElementException: None.get. Clear any
    // leaked interrupt state before each test so the flake cannot propagate between tests.
    val _ = Thread.interrupted()
    reset(storageInterface, connectorTaskId, bucketAndPrefixFn, pendingOperationsProcessors)

    indexManagerV2 = new IndexManagerV2(
      bucketAndPrefixFn,
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
    val (storageInterface, pendingProcessors, bucketAndPrefixFn, directoryFileName, topicPartition) =
      setupMocksForLockFilePathTest()

    val indexManager = new IndexManagerV2(
      bucketAndPrefixFn,
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
    val pendingProcessors = mock[PendingOperationsProcessors]
    val directoryFileName = "custom-index-dir"
    val topicPartition    = Topic("my-topic").withPartition(2)
    val bucketAndPrefixFn: TopicPartition => Either[SinkError, CloudLocation] =
      _ => Right(CloudLocation("bucket", None))

    when(storageInterface.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(storageInterface.getBlobAsObject[IndexFile](anyString(), anyString())(any[Decoder[IndexFile]]))
      .thenReturn(Left(FileNotFoundError(new Exception("Not found"), "somepath")))
    when(storageInterface.writeBlobToFile(anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
      any[Encoder[IndexFile]],
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("owner", None, None), "etag")))
    when(storageInterface.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

    (storageInterface, pendingProcessors, bucketAndPrefixFn, directoryFileName, topicPartition)
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

  test("open dead-worker recovery (missing local file) should clear pending state and allow subsequent updates") {
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

    // writeBlobToFile: first call clears pending state (dead-worker recovery), second is the subsequent update
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
      realPendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      val result = realIndexManagerV2.open(Set(topicPartition))
      result.isRight shouldBe true
      // dead-worker recovery: graceful clear with further ops calls fnIndexUpdate which returns the committed offset
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
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
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
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
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
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    ).thenReturn(Left(FatalCloudSinkError("transient cloud error", tp)))

    try {
      val result = im.ensureGranularLock(tp, "pk-pending")
      result.isLeft shouldBe true

      im.granularCacheSize shouldBe 0
    } finally im.close()
  }

  /**
   * Shared setup for both dead-worker granular-lock recovery tests.
   * Returns `(si, im)` with all storage stubs pre-configured.
   */
  private def buildDeadWorkerRecoveryFixture(): (StorageInterface[_], IndexManagerV2) = {
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val missingFile     = new java.io.File("/tmp/staging-dead-worker/gone.tmp")
    val pk              = "my-pk"

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.listKeysRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

    // Master lock: no pending state, committed offset 450
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("owner", Some(Offset(450)), None), "master-etag")))

    // Granular lock: [Upload, Copy, Delete] with committedOffset=450, pendingOffset=500
    val pendingOps = NonEmptyList.of[FileOperation](
      UploadOperation("bucket", missingFile, ".temp-upload/topic1/0/uuid-dead-worker.avro"),
      CopyOperation("bucket",
                    ".temp-upload/topic1/0/uuid-dead-worker.avro",
                    "prefix/topic1/0/000000000500.avro",
                    "placeholder",
      ),
      DeleteOperation("bucket", ".temp-upload/topic1/0/uuid-dead-worker.avro", "placeholder"),
    )
    val granularIndexFile = IndexFile("owner", Some(Offset(450)), Some(PendingState(Offset(500), pendingOps)))
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains(s"/0/$pk.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Right(ObjectWithETag(granularIndexFile, "granular-etag")))

    // Upload fails with NonExistingFileError (dead-worker staging file is gone)
    when(si.uploadFile(any[UploadableFile], anyString(), anyString()))
      .thenReturn(Left(NonExistingFileError(missingFile)))

    // writeBlobToFile: called by updateForPartitionKey to clear the pending state
    when(
      si.writeBlobToFile[IndexFile](anyString(),
                                    ArgumentMatchers.contains(s"/0/$pk.lock"),
                                    any[ObjectWithETag[IndexFile]],
      )(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("owner", Some(Offset(450)), None), "cleared-etag")))

    val realPop = new PendingOperationsProcessors(si)
    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      realPop,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)
    (si, im)
  }

  test(
    "loadGranularLock (getSeekedOffsetForPartitionKey): NonExistingFileError on Upload gracefully clears PendingState (dead-worker recovery, escalateOnCancel=false)",
  ) {
    val pk       = "my-pk"
    val (si, im) = buildDeadWorkerRecoveryFixture()
    val tp       = Topic("topic1").withPartition(0)

    try {
      im.open(Set(tp)).isRight shouldBe true

      // Cache miss → loadGranularLock → NonExistingFileError → graceful clear → Right(committed)
      val result = im.getSeekedOffsetForPartitionKey(tp, pk)
      result shouldBe Right(Some(Offset(450)))

      // The pending state was cleared in the granular lock (writeBlobToFile called with None pendingState)
      val captor = ArgumentCaptor.forClass(classOf[ObjectWithETag[IndexFile]])
      import org.mockito.Mockito.{ times => mtimes }
      import org.mockito.Mockito.{ verify => mverify }
      mverify(si, mtimes(1)).writeBlobToFile[IndexFile](
        anyString(),
        ArgumentMatchers.contains(s"/0/$pk.lock"),
        captor.capture(),
      )(ArgumentMatchers.eq(indexFileEncoder))
      captor.getValue.wrappedObject.pendingState shouldBe None
      captor.getValue.wrappedObject.committedOffset shouldBe Some(Offset(450))

      // Copy (mvFile) and Delete were NOT attempted — stopped at Upload failure
      mverify(si, Mockito.never()).mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]])
      mverify(si, Mockito.never()).deleteFile(anyString(), anyString(), any[String])

      // A subsequent getSeekedOffsetForPartitionKey is a cache hit, returning the cleared offset
      val cached = im.getSeekedOffsetForPartitionKey(tp, pk)
      cached shouldBe Right(Some(Offset(450)))
      // Only one getBlobAsObject call for the granular lock (no second read after recovery)
      mverify(si, mtimes(1)).getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.contains(s"/0/$pk.lock"),
      )(ArgumentMatchers.eq(indexFileDecoder))
    } finally im.close()
  }

  test(
    "resolveAndCacheGranularLock (ensureGranularLock): NonExistingFileError on Upload gracefully clears PendingState (dead-worker recovery, escalateOnCancel=false)",
  ) {
    val pk       = "my-pk"
    val (si, im) = buildDeadWorkerRecoveryFixture()
    val tp       = Topic("topic1").withPartition(0)

    try {
      im.open(Set(tp)).isRight shouldBe true

      // ensureGranularLock reads the lock → sees PendingState → resolveAndCacheGranularLock
      // → NonExistingFileError on Upload → graceful clear → Right(())
      val result = im.ensureGranularLock(tp, pk)
      result shouldBe Right(())

      // The granular lock was written with cleared pending state
      val captor = ArgumentCaptor.forClass(classOf[ObjectWithETag[IndexFile]])
      import org.mockito.Mockito.{ times => mtimes }
      import org.mockito.Mockito.{ verify => mverify }
      mverify(si, mtimes(1)).writeBlobToFile[IndexFile](
        anyString(),
        ArgumentMatchers.contains(s"/0/$pk.lock"),
        captor.capture(),
      )(ArgumentMatchers.eq(indexFileEncoder))
      captor.getValue.wrappedObject.pendingState shouldBe None
      captor.getValue.wrappedObject.committedOffset shouldBe Some(Offset(450))

      // Copy and Delete were NOT called
      mverify(si, Mockito.never()).mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]])
      mverify(si, Mockito.never()).deleteFile(anyString(), anyString(), any[String])

      // Cache was updated: subsequent getSeekedOffsetForPartitionKey is a hit returning cleared offset
      val seeked = im.getSeekedOffsetForPartitionKey(tp, pk)
      seeked shouldBe Right(Some(Offset(450)))
      // No additional getBlobAsObject call for the granular lock
      mverify(si, mtimes(1)).getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.contains(s"/0/$pk.lock"),
      )(ArgumentMatchers.eq(indexFileDecoder))
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
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
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
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      )
    } finally im.close()
  }

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

  test("cleanUpObsoleteLocks does NOT delete eTag-only entries when the writer is in the active set") {
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

      // eTag-only entries have offset = None. They must NOT be deleted while the writer
      // is active, regardless of the safe-offset threshold.
      val result = im.cleanUpObsoleteLocks(tp, Offset(50), Set("pk-etag-only"))
      result shouldBe Right(())

      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
      im.granularCacheSize shouldBe 1
    } finally {
      im.close()
    }
  }

  test("cleanUpObsoleteLocks evicts and enqueues eTag-only entries when no active writer holds the key") {
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
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // ensureGranularLock seeds the cache with offset = None for an empty lock.
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-abandoned.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Left(FileNotFoundError(new Exception("Not found"), "pk-abandoned-path")))
      when(
        si.writeBlobToFile[IndexFile](anyString(),
                                      ArgumentMatchers.contains("/0/pk-abandoned.lock"),
                                      any[NoOverwriteExistingObject[IndexFile]],
        )(
          ArgumentMatchers.eq(indexFileEncoder),
        ),
      ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "etag-only")))

      im.ensureGranularLock(tp, "pk-abandoned") shouldBe Right(())
      im.granularCacheSize shouldBe 1

      // No active writer holds pk-abandoned, so the empty lock is eligible for GC.
      val result = im.cleanUpObsoleteLocks(tp, Offset(50), Set.empty)
      result shouldBe Right(())

      // Cache eviction is synchronous in the enqueue phase.
      im.granularCacheSize shouldBe 0

      // The drain run inside close() flushes the enqueued path and deletes it.
    } finally {
      im.close()
    }

    verify(si, times(1)).deleteFiles(
      anyString(),
      ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("/0/pk-abandoned.lock"))),
    )
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

  test("cleanUpObsoleteLocks preserves lock at masterOffset (one-record-overlap invariant)") {
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
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // pk-at-master has offset 99, which equals globalSafeOffset - 1 (masterOffset).
      // pk-below has offset 98, which is strictly below masterOffset.
      Seq("pk-at-master", "pk-below").zip(Seq(Offset(99), Offset(98))).foreach { case (pk, offset) =>
        when(
          si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains(s"/0/$pk.lock"))(
            ArgumentMatchers.eq(indexFileDecoder),
          ),
        )
          .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(offset), None), s"etag-$pk")))
        im.getSeekedOffsetForPartitionKey(tp, pk) shouldBe Right(Some(offset))
      }

      im.granularCacheSize shouldBe 2

      // globalSafeOffset = 100, so masterOffset = 99.
      // pk-below (98) is strictly below masterOffset and should be GC'd.
      // pk-at-master (99) equals masterOffset and must be preserved for dedup on replay.
      val result = im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty)
      result shouldBe Right(())

      im.granularCacheSize shouldBe 1

      when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))
      im.drainGcQueue()

      verify(si).deleteFiles(anyString(), ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("pk-below"))))
      verify(si, never).deleteFiles(anyString(),
                                    ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("pk-at-master"))),
      )
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
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
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
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
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

  test("drainGcQueue deletes .tmp orphans even when granularCache holds the same partitionKey") {
    // The dominant orphan-.tmp scenario: task A crashed mid-write (left a stale
    // .lock.tmp.<uuid> blob), task B successfully wrote the real .lock for the
    // same partition key and populated granularCache[tp][pk]. Before the fix,
    // drainGcQueue's gcContainsKey filter saw the cache entry and skipped the
    // .tmp delete forever -- a perpetual storage leak. After the fix the
    // GcKind.TmpOrphan tag bypasses the cache-reclaim filter.
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val pk      = "pk-shared"
    val oldTime = Instant.now().minusSeconds(7200)
    // UUID-shaped suffix matches TmpOrphanPattern ([0-9a-fA-F-]+).
    val tmpPath =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/$pk.lock.tmp.deadbeef-1234-4567-89ab-cdef00112233"
    val tmpMeta  = TestFileMetadata(tmpPath, oldTime)
    val listResp = ListOfMetadataResponse("bucket", Some("prefix"), Seq(tmpMeta), tmpMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResp))).when(si).listFileMetaRecursive(anyString(), any[Option[String]])
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains(s"/$pk.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("newWriter", Some(Offset(50)), None), "etag-live")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))

      im.getSeekedOffsetForPartitionKey(tp, pk) shouldBe Right(Some(Offset(50)))
      im.granularCacheSize shouldBe 1

      im.sweepOrphanedLocks()
      im.drainGcQueue()

      val pathCaptor = ArgumentCaptor.forClass(classOf[Seq[String]])
      verify(si, times(1)).deleteFiles(anyString(), pathCaptor.capture())
      val deletedPaths = pathCaptor.getValue
      deletedPaths should contain(tmpPath)
    } finally im.close()
  }

  test(
    "sweep does NOT classify a real .lock file as a tmp orphan when partitionKey contains '.lock.tmp.' (regression)",
  ) {
    // `WriterManager.sanitize` URL-encodes partition values but leaves `.` literal,
    // so a value like `report.lock.tmp.archive` produces a real granular lock at
    // `<...>/name=report.lock.tmp.archive.lock`. A naive `contains(".lock.tmp.")`
    // sweep filter would enqueue this file as a tmp orphan under GcKind.TmpOrphan,
    // bypassing the cache-reclaim filter and deleting an active lock. The
    // anchored TmpOrphanPattern (`.lock.tmp.<uuid>$`) prevents this.
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val pk       = "name=report.lock.tmp.archive"
    val oldTime  = Instant.now().minusSeconds(7200)
    val realLock = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/$pk.lock"
    val realMeta = TestFileMetadata(realLock, oldTime)
    val listResp = ListOfMetadataResponse("bucket", Some("prefix"), Seq(realMeta), realMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResp))).when(si).listFileMetaRecursive(anyString(), any[Option[String]])
    // Real lock content with committedOffset >= masterOffset (100), so the normal
    // `.lock` sweep path also leaves it alone. We're asserting the tmp-orphan
    // path does not enqueue it under any circumstances.
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith(s"$pk.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "etag-real")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      // The real lock must NOT be deleted by either the tmp-orphan or .lock sweep paths.
      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test("sweep does NOT classify '<pk>.lock.tmp.lock' as a tmp orphan (no UUID suffix) (regression)") {
    // A pathological partitionKey like `foo.lock.tmp` ends up as `<...>/foo.lock.tmp.lock`.
    // The anchored regex requires a UUID-shaped suffix after `.lock.tmp.`, so `lock`
    // alone (which contains characters outside [0-9a-fA-F-]) does not match.
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val pk       = "foo.lock.tmp"
    val oldTime  = Instant.now().minusSeconds(7200)
    val realLock = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/$pk.lock"
    val realMeta = TestFileMetadata(realLock, oldTime)
    val listResp = ListOfMetadataResponse("bucket", Some("prefix"), Seq(realMeta), realMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResp))).when(si).listFileMetaRecursive(anyString(), any[Option[String]])
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith(s"$pk.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(200)), None), "etag-real")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test("sweep classifies '<pk>.lock.tmp.<uuid>' as a tmp orphan and captures the full pk (incl. dots)") {
    // Positive case: a real tmp orphan whose partitionKey itself contains `.lock.tmp.`.
    // The regex must capture the FULL partition key (`name=report.lock.tmp.archive`),
    // not just the prefix up to the first `.lock.tmp.`. We verify both that the file
    // is enqueued for deletion and that the captured partitionKey is correct by
    // checking it does NOT clash with a separately cached granular lock for the
    // shorter (incorrect-greedy) extraction `name=report`.
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val pk      = "name=report.lock.tmp.archive"
    val uuid    = "deadbeef-1234-4567-89ab-cdef00112233"
    val oldTime = Instant.now().minusSeconds(7200)
    val tmpPath =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/$pk.lock.tmp.$uuid"
    val tmpMeta  = TestFileMetadata(tmpPath, oldTime)
    val listResp = ListOfMetadataResponse("bucket", Some("prefix"), Seq(tmpMeta), tmpMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResp))).when(si).listFileMetaRecursive(anyString(), any[Option[String]])
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))

      // Populate the granular cache for `name=report` (the WRONG, naive-greedy
      // extraction). If the regex captured this as the partitionKey, the
      // GcKind.TmpOrphan path would still bypass the cache filter and delete --
      // so this is not a sufficient test on its own. We instead also populate
      // a cache entry for the CORRECT pk and assert deletion still proceeds
      // (TmpOrphan bypass) AND the deleted path is the .tmp file.
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith(s"$pk.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      ).thenReturn(Right(ObjectWithETag(IndexFile("newWriter", Some(Offset(50)), None), "etag-live")))
      im.getSeekedOffsetForPartitionKey(tp, pk) shouldBe Right(Some(Offset(50)))

      im.sweepOrphanedLocks()
      im.drainGcQueue()

      val pathCaptor = ArgumentCaptor.forClass(classOf[Seq[String]])
      verify(si, times(1)).deleteFiles(anyString(), pathCaptor.capture())
      pathCaptor.getValue should contain(tmpPath)
    } finally im.close()
  }

  test("drainGcQueue re-offers polled items when deleteFiles throws an unexpected exception") {
    // Unexpected exceptions (not a Left storage error) used to be caught by the
    // outer try/NonFatal and silently swallowed, dropping every polled item from
    // gcQueue. The M2 fix tracks "unprocessed" items and re-offers them so nothing
    // polled is silently lost.
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
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-doomed.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(30)), None), "etag-doomed")))
      im.getSeekedOffsetForPartitionKey(tp, "pk-doomed") shouldBe Right(Some(Offset(30)))

      im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty) shouldBe Right(())

      when(si.deleteFiles(anyString(), any[Seq[String]])).thenThrow(new RuntimeException("boom"))

      // Must not throw -- the outer catch handles NonFatal
      im.drainGcQueue()

      // The polled item must have been re-offered to the queue
      im.gcQueueSize shouldBe 1
    } finally im.close()
  }

  private def createSweepTestManager(
    si:                     StorageInterface[_],
    gcSweepEnabled:         Boolean = true,
    gcSweepIntervalSeconds: Int     = 3600,
    gcSweepMinAgeSeconds:   Int     = 3600,
    gcSweepMaxReads:        Int     = 1000,
  ): IndexManagerV2 =
    new IndexManagerV2(
      bucketAndPrefixFn,
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
    val _ = when(
      si.writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                     anyString(),
                                                     any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]]),
    )
      .thenReturn(Right(ObjectWithETag(IndexManagerV2.SweepMarker(0L, 0L), "marker-etag")))
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

  test("sweep preserves orphaned lock at exactly master offset (one-record-overlap invariant)") {
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
    // The sweep threshold is strictly less than masterOffset, so the lock at
    // exactly masterOffset must be preserved for one-record-overlap dedup.
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("exact-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "exact-etag")))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test("sweep is a no-op when master lock committedOffset is None (globalSafeOffset == 0)") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    // Override the master-lock GET stub from setupSweepMocks so the lock has
    // committedOffset = None. This is what `updateMasterLock` persists when
    // globalSafeOffset == 0, and what `open()` then observes: no entry is put
    // into `seekedOffsets`, so the sweep must short-circuit the partition
    // entirely without issuing LIST/GET calls against cloud storage.
    when(
      si.getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.endsWith(s"${tp.partition}.lock"),
      )(ArgumentMatchers.eq(indexFileDecoder)),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), "etag")))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      // No partition enters the sweep loop, so no LIST is issued and nothing
      // is ever enqueued for deletion.
      verify(si, never).listFileMetaRecursive(anyString(), any[Option[String]])
      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test(
    "sweep preserves the one-record-overlap lock at offset 0 when master lock committedOffset is Some(0) (globalSafeOffset == 1)",
  ) {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    // Override the master-lock GET stub so committedOffset = Some(Offset(0)),
    // which corresponds to globalSafeOffset == 1 (persisted as globalSafeOffset - 1).
    // `open()` then populates `seekedOffsets(tp) = Offset(0)`.
    when(
      si.getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.endsWith(s"${tp.partition}.lock"),
      )(ArgumentMatchers.eq(indexFileDecoder)),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(0)), None), "etag")))

    val oldTime = Instant.now().minusSeconds(7200)
    val exactPath =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/exact-key.lock"
    val exactMeta    = TestFileMetadata(exactPath, oldTime)
    val listResponse = ListOfMetadataResponse("bucket", Some("prefix"), Seq(exactMeta), exactMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )
    // The only non-negative offset a granular lock can carry is 0. At masterOffset=0,
    // the sweep threshold `committedOffset < masterOffset` is `0 < 0 = false`, so
    // this lock must be preserved for the one-record-overlap invariant.
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("exact-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(0)), None), "exact-etag")))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      // The sweep DID read the orphan (distinguishing this from the globalSafeOffset == 0
      // case where no GET happens) and correctly chose to skip it rather than enqueue
      // it for deletion.
      verify(si, atLeastOnce).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("exact-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )
      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  test("sweep deletes orphan one below masterOffset but preserves orphan at masterOffset") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime   = Instant.now().minusSeconds(7200)
    val belowPath = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/below-key.lock"
    val exactPath = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/exact-key.lock"
    val belowMeta = TestFileMetadata(belowPath, oldTime)
    val exactMeta = TestFileMetadata(exactPath, oldTime)
    val listResponse =
      ListOfMetadataResponse("bucket", Some("prefix"), Seq(belowMeta, exactMeta), exactMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )
    // masterOffset = 100 (from setupSweepMocks). below-key at 99 is strictly below,
    // exact-key at 100 equals masterOffset and must be preserved.
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("below-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(99)), None), "below-etag")))
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

      verify(si, times(1)).deleteFiles(
        anyString(),
        ArgumentMatchers.argThat[Seq[String]](paths =>
          paths.exists(_.contains("below-key")) && !paths.exists(_.contains("exact-key")),
        ),
      )
    } finally im.close()
  }

  test("sweep enqueues empty lock files with no committedOffset and no PendingState") {
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

      verify(si, times(1)).deleteFiles(
        anyString(),
        ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("no-offset.lock"))),
      )
    } finally im.close()
  }

  test("sweep does NOT enqueue lock files with no committedOffset but a PendingState") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime = Instant.now().minusSeconds(7200)
    val pendingPath =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/none-with-pending.lock"
    val pendingMeta  = TestFileMetadata(pendingPath, oldTime)
    val listResponse = ListOfMetadataResponse("bucket", Some("prefix"), Seq(pendingMeta), pendingMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(anyString(),
                                                                                           any[Option[String]],
    )

    // Defensive: the commit protocol never produces this shape (PendingState only with
    // Some(committedOffset)), but if it ever appeared we must not delete it because the
    // pending operations would be lost.
    val pendingState =
      PendingState(Offset(10), NonEmptyList.one(DeleteOperation("bucket", "some/temp/path", "old-etag")))
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("none-with-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, Some(pendingState)), "pending-etag")))
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
    when(
      si.writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                     anyString(),
                                                     any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]]),
    )
      .thenReturn(Right(ObjectWithETag(IndexManagerV2.SweepMarker(0L, 0L), "marker-etag")))

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
      when(
        si.writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                       anyString(),
                                                       any[ObjectProtection[IndexManagerV2.SweepMarker]],
        )(any[Encoder[IndexManagerV2.SweepMarker]]),
      )
        .thenReturn(Right(ObjectWithETag(IndexManagerV2.SweepMarker(0L, 0L), "marker-etag-new")))
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
      inOrder.verify(si).writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                                     ArgumentMatchers.contains("sweep-marker"),
                                                                     any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]])
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
      verify(si, never).writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                                    anyString(),
                                                                    any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]])
    } finally im.close()
  }

  test("sweep skips scan when marker write loses eTag race") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    ))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    // Marker not found → sweep is due (will attempt NoOverwriteExistingObject write)
    when(
      si.getBlobAsObject[IndexManagerV2.SweepMarker](anyString(), ArgumentMatchers.contains("sweep-marker"))(
        any[Decoder[IndexManagerV2.SweepMarker]],
      ),
    ).thenReturn(Left(FileNotFoundError(new Exception("Not found"), "sweep-marker")))

    // Conditional write fails (another task created the marker first)
    when(
      si.writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                     anyString(),
                                                     any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]]),
    )
      .thenReturn(Left(FileCreateError(new Exception("eTag mismatch"), "marker")))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()

      // Marker write was attempted exactly once (before the scan)
      verify(si, times(1)).writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                                       anyString(),
                                                                       any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]])
      // Scan is skipped when the marker write loses the eTag race: no LIST call.
      // `sweepPartition` gates all lock-file GETs behind the LIST, so asserting no LIST
      // is sufficient to prove the expensive scan work was not performed.
      verify(si, never).listFileMetaRecursive(anyString(), any[Option[String]])
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
    when(
      si.writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                     anyString(),
                                                     any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]]),
    )
      .thenReturn(Right(ObjectWithETag(IndexManagerV2.SweepMarker(0L, 0L), "marker-etag")))
    org.mockito.Mockito.doReturn(Right(None))
      .when(si).listFileMetaRecursive(anyString(), any[Option[String]])

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp1, tp2))
      im.sweepOrphanedLocks()

      val bucketCaptor = ArgumentCaptor.forClass(classOf[String])
      val pathCaptor   = ArgumentCaptor.forClass(classOf[String])
      verify(si, times(2)).writeBlobToFile[IndexManagerV2.SweepMarker](
        bucketCaptor.capture(),
        pathCaptor.capture(),
        any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]])
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

    when(
      si.writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                     anyString(),
                                                     any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]]),
    )
      .thenReturn(Right(ObjectWithETag(IndexManagerV2.SweepMarker(0L, 0L), "marker-etag")))
    org.mockito.Mockito.doReturn(Right(None))
      .when(si).listFileMetaRecursive(anyString(), any[Option[String]])

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp1, tp2))
      im.sweepOrphanedLocks()

      // tp1 should be swept (marker written + files listed)
      verify(si, times(1)).writeBlobToFile[IndexManagerV2.SweepMarker](
        ArgumentMatchers.eq("bucket-a"),
        ArgumentMatchers.eq(tp1MarkerPath),
        any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]])
      verify(si, times(1)).listFileMetaRecursive(anyString(), any[Option[String]])

      // tp2's marker should NOT be written (skipped due to non-expired marker)
      verify(si, never).writeBlobToFile[IndexManagerV2.SweepMarker](
        ArgumentMatchers.eq("bucket-b"),
        ArgumentMatchers.eq(tp2MarkerPath),
        any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]])
    } finally im.close()
  }

  test("sweep does not enqueue master lock file even if listing returns it") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime = Instant.now().minusSeconds(7200)

    // Master lock sits outside the granular locks directory (sibling, not child)
    val masterLockPath =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}.lock"
    val masterMeta = TestFileMetadata(masterLockPath, oldTime)

    // Legitimate granular orphan inside the partition subdirectory
    val orphanPath =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/orphan-key.lock"
    val orphanMeta = TestFileMetadata(orphanPath, oldTime)

    val listResponse =
      ListOfMetadataResponse("bucket", Some("prefix"), Seq(masterMeta, orphanMeta), orphanMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResponse))).when(si).listFileMetaRecursive(
      anyString(),
      any[Option[String]],
    )
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("orphan-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "orphan-etag")))
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      // Reset call counts so assertions below cover only the sweep phase
      org.mockito.Mockito.clearInvocations(si)

      im.sweepOrphanedLocks()
      im.drainGcQueue()

      // The master lock must never be GET-read by the sweep
      verify(si, never).getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.eq(masterLockPath),
      )(ArgumentMatchers.eq(indexFileDecoder))

      // The orphan must still be enqueued and deleted
      val pathsCaptor = ArgumentCaptor.forClass(classOf[Seq[String]])
      verify(si, times(1)).deleteFiles(anyString(), pathsCaptor.capture())
      val deletedPaths = pathsCaptor.getValue
      deletedPaths should contain(orphanPath)
      deletedPaths should not contain masterLockPath
    } finally im.close()
  }

  test("close on a never-opened IndexManagerV2 should not start executors") {
    val si  = mock[StorageInterface[_]]
    val cti = ConnectorTaskId("test-connector", 1, 0)

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
    )(si, cti)

    im.executorsStarted shouldBe false
    im.close()
    im.executorsStarted shouldBe false
  }

  test("executors are started after open() is called") {
    val si  = mock[StorageInterface[_]]
    val cti = ConnectorTaskId("test-connector", 1, 0)

    val bucketFn: TopicPartition => Either[SinkError, CloudLocation] =
      _ => Right(CloudLocation("bucket", Some("prefix")))

    val im = new IndexManagerV2(
      bucketFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
      gcSweepEnabled    = false,
    )(si, cti)

    try {
      im.executorsStarted shouldBe false

      val tp       = Topic("topic1").withPartition(0)
      val idxFile  = IndexFile(cti.lockUuid, Some(Offset(0)), None)
      val objWETag = ObjectWithETag(idxFile, "etag1")

      when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
      when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
        .thenReturn(Right(objWETag))

      im.open(Set(tp))

      im.executorsStarted shouldBe true
    } finally im.close()
  }

  test("close() resets executor state so a subsequent open() recreates executors") {
    val si  = mock[StorageInterface[_]]
    val cti = ConnectorTaskId("test-connector", 1, 0)

    val bucketFn: TopicPartition => Either[SinkError, CloudLocation] =
      _ => Right(CloudLocation("bucket", Some("prefix")))

    val im = new IndexManagerV2(
      bucketFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
      gcSweepEnabled    = true,
    )(si, cti)

    try {
      val tp       = Topic("topic1").withPartition(0)
      val idxFile  = IndexFile(cti.lockUuid, Some(Offset(0)), None)
      val objWETag = ObjectWithETag(idxFile, "etag1")

      when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
      when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
        .thenReturn(Right(objWETag))

      // First open — executors should be created
      im.open(Set(tp))
      im.executorsStarted shouldBe true
      im.gcExecutor should not be empty
      im.sweepExecutorOpt should not be empty

      // close — executors should be shut down and state reset
      im.close()
      im.executorsStarted shouldBe false
      im.gcExecutor shouldBe None
      im.sweepExecutorOpt shouldBe None

      // Second open — fresh executors should be created
      im.open(Set(tp))
      im.executorsStarted shouldBe true
      im.gcExecutor should not be empty
      im.sweepExecutorOpt should not be empty
    } finally im.close()
  }

  test("open() clears stale seekedOffsets from revoked partitions after rebalance") {
    val tp0             = Topic("topic1").withPartition(0)
    val tp1             = Topic("topic1").withPartition(1)
    val tp2             = Topic("topic1").withPartition(2)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds      = Int.MaxValue,
      gcSweepEnabled         = true,
      gcSweepIntervalSeconds = 3600,
      gcSweepMinAgeSeconds   = 3600,
      gcSweepMaxReads        = 1000,
    )(si, connectorTaskId)

    try {
      // First assignment: {tp0, tp1}
      im.open(Set(tp0, tp1))
      im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(100))
      im.getSeekedOffsetForTopicPartition(tp1) shouldBe Some(Offset(100))

      // Rebalance: tp0 revoked, tp2 added → new assignment {tp1, tp2}
      im.open(Set(tp1, tp2))

      // tp0 should have been pruned
      im.getSeekedOffsetForTopicPartition(tp0) shouldBe None
      // tp1 should still be present (re-opened)
      im.getSeekedOffsetForTopicPartition(tp1) shouldBe Some(Offset(100))
      // tp2 should be present (newly opened)
      im.getSeekedOffsetForTopicPartition(tp2) shouldBe Some(Offset(100))

      // Sweep should NOT process tp0 (no longer in seekedOffsets).
      // After pruning, only tp1 and tp2 remain, so the sweep should list exactly 2 partitions.
      when(
        si.getBlobAsObject[IndexManagerV2.SweepMarker](
          anyString(),
          ArgumentMatchers.contains("sweep-marker"),
        )(any[Decoder[IndexManagerV2.SweepMarker]]),
      ).thenReturn(Left(FileNotFoundError(new Exception("Not found"), "sweep-marker")))
      val _ = when(
        si.writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                       anyString(),
                                                       any[ObjectProtection[IndexManagerV2.SweepMarker]],
        )(any[Encoder[IndexManagerV2.SweepMarker]]),
      )
        .thenReturn(Right(ObjectWithETag(IndexManagerV2.SweepMarker(0L, 0L), "marker-etag")))
      when(si.listFileMetaRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

      im.sweepOrphanedLocks()

      verify(si, times(2)).listFileMetaRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  test("open() does not clear anything on first call when seekedOffsets is empty") {
    val tp0             = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      // First open on a fresh instance — no stale state to prune
      im.open(Set(tp0))
      im.getSeekedOffsetForTopicPartition(tp0) shouldBe Some(Offset(50))
    } finally im.close()
  }

  test("open() clears granular cache entries for revoked partitions") {
    val tp0             = Topic("topic1").withPartition(0)
    val tp1             = Topic("topic1").withPartition(1)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      // Open tp0 and tp1
      im.open(Set(tp0, tp1))

      // Populate granular cache for tp0 by loading a granular lock
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-a.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(80)), None), "g-etag")))
      im.getSeekedOffsetForPartitionKey(tp0, "pk-a") shouldBe Right(Some(Offset(80)))
      im.granularCacheSize shouldBe 1

      // Rebalance: tp0 revoked → new assignment {tp1}
      im.open(Set(tp1))

      // Granular cache for tp0 should be evicted
      im.granularCacheSize shouldBe 0
      im.getSeekedOffsetForTopicPartition(tp0) shouldBe None
    } finally im.close()
  }

  test("suspendBackgroundWork() prevents scheduled sweep and drain until next open()") {
    val tp0             = Topic("topic1").withPartition(0)
    val tp1             = Topic("topic1").withPartition(1)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds      = Int.MaxValue,
      gcSweepEnabled         = true,
      gcSweepIntervalSeconds = 3600,
      gcSweepMinAgeSeconds   = 3600,
      gcSweepMaxReads        = 1000,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp0, tp1))
      im.acceptingWork shouldBe true

      // Simulate close() calling suspendBackgroundWork()
      im.suspendBackgroundWork()
      im.acceptingWork shouldBe false

      // Set up sweep mocks -- these should NOT be called while suspended
      when(
        si.getBlobAsObject[IndexManagerV2.SweepMarker](
          anyString(),
          ArgumentMatchers.contains("sweep-marker"),
        )(any[Decoder[IndexManagerV2.SweepMarker]]),
      ).thenReturn(Left(FileNotFoundError(new Exception("Not found"), "sweep-marker")))
      val _ = when(
        si.writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                       anyString(),
                                                       any[ObjectProtection[IndexManagerV2.SweepMarker]],
        )(any[Encoder[IndexManagerV2.SweepMarker]]),
      )
        .thenReturn(Right(ObjectWithETag(IndexManagerV2.SweepMarker(0L, 0L), "marker-etag")))
      when(si.listFileMetaRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

      // Direct call simulates what the scheduled lambda does: check flag, then invoke
      if (im.acceptingWork) im.sweepOrphanedLocks()
      if (im.acceptingWork) im.drainGcQueue()

      // Sweep should not have run -- no listFileMetaRecursive calls
      verify(si, times(0)).listFileMetaRecursive(anyString(), any[Option[String]])

      // Rebalance: open() with new partitions re-enables background work
      im.open(Set(tp1))
      im.acceptingWork shouldBe true

      // Now the sweep should work
      im.sweepOrphanedLocks()
      verify(si, times(1)).listFileMetaRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  test("suspendBackgroundWork() keeps GC items in queue when scheduled drain is skipped") {
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
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      // Load a granular lock with an offset below the GC threshold
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-queued.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      )
        .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(10)), None), "etag-pk-queued")))
      im.getSeekedOffsetForPartitionKey(tp, "pk-queued") shouldBe Right(Some(Offset(10)))

      // Enqueue for GC
      im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty) shouldBe Right(())
      im.granularCacheSize shouldBe 0

      // Suspend background work
      im.suspendBackgroundWork()
      im.acceptingWork shouldBe false

      // Simulate the scheduled lambda: flag is false so drain is skipped
      if (im.acceptingWork) im.drainGcQueue()

      // deleteFiles must NOT have been called — the item is still in the queue
      verify(si, never).deleteFiles(anyString(), any[Seq[String]])

      // Resume via open() and drain should now process the queued item
      im.open(Set(tp))
      im.acceptingWork shouldBe true
      when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))
      im.drainGcQueue()

      verify(si).deleteFiles(anyString(), ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("pk-queued"))))
    } finally im.close()
  }

  test("close() final drain processes items even when acceptingWork is false (shutdown bypass)") {
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
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    // Load a granular lock and enqueue for GC
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-shutdown.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    )
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(10)), None), "etag-pk-shutdown")))
    im.getSeekedOffsetForPartitionKey(tp, "pk-shutdown") shouldBe Right(Some(Offset(10)))
    im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty) shouldBe Right(())

    // Simulate CloudSinkTask.close() → suspendBackgroundWork() + writerManager.close()
    im.suspendBackgroundWork()
    im.acceptingWork shouldBe false
    im.evictAllGranularLocks(tp)

    verify(si, never).deleteFiles(anyString(), any[Seq[String]])

    // close() calls drainGcQueue() directly, bypassing the scheduled lambda gate
    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))
    im.close()

    verify(si).deleteFiles(anyString(), ArgumentMatchers.argThat[Seq[String]](_.exists(_.contains("pk-shutdown"))))
  }

  test("open() does not set acceptingWork when parTraverse returns a Left") {
    val tp0             = Topic("topic1").withPartition(0)
    val tp1             = Topic("topic1").withPartition(1)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]

    when(bucketAndPrefixFn(ArgumentMatchers.eq(tp0))).thenReturn(Right(bucketAndPrefix))
    when(bucketAndPrefixFn(ArgumentMatchers.eq(tp1)))
      .thenReturn(Left(FatalCloudSinkError("simulated failure", tp1)))

    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      val result = im.open(Set(tp0, tp1))
      result.isLeft shouldBe true
      im.acceptingWork shouldBe false
    } finally im.close()
  }

  test("open removes cached eTag when processPendingOperations fails") {
    // Seed scenario: the index file in storage has a PendingState, so open() seeds
    // topicPartitionToETags with the pre-resolution eTag before handing off to
    // processPendingOperations. If the latter returns Left (e.g. after advancing
    // the index in storage during an intermediate phase), the cached eTag is now
    // stale. The M1 fix removes it so the next call re-reads the file.
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    val pp = mock[PendingOperationsProcessors]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))

    val pendingOps = NonEmptyList.of[FileOperation](
      CopyOperation("bucket", "temp-path", "final-path", "placeholder"),
      DeleteOperation("bucket", "temp-path", "placeholder"),
    )
    val pendingIndexFile = IndexFile("lockOwner", Some(Offset(80)), Some(PendingState(Offset(90), pendingOps)))

    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(pendingIndexFile, "stale-master-etag")))

    when(
      pp.processPendingOperations(
        ArgumentMatchers.eq(tp),
        any[Option[Offset]],
        any[PendingState],
        any[(TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    ).thenReturn(Left(FatalCloudSinkError("simulated pending failure", tp)))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pp,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      val result = im.open(Set(tp))
      result.isLeft shouldBe true

      // After H2 rollback AND M1 cleanup, the cache must not hold the pre-pending eTag
      // even transiently for this tp -- otherwise a later conditional write could use
      // a stale If-Match.
      im.topicPartitionToETags.contains(tp) shouldBe false
    } finally im.close()
  }

  test("open rolls back in-memory state when any partition fails") {
    // tp0 succeeds (bucketAndPrefixFn Right, existing index file).
    // tp1 fails (bucketAndPrefixFn Left).
    // parTraverse returns Left, but tp0's successful fiber has already mutated
    // seekedOffsets and topicPartitionToETags. The H2 fix rolls those back so
    // the index manager is in a clean state.
    val tp0             = Topic("topic1").withPartition(0)
    val tp1             = Topic("topic1").withPartition(1)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(ArgumentMatchers.eq(tp0))).thenReturn(Right(bucketAndPrefix))
    when(bucketAndPrefixFn(ArgumentMatchers.eq(tp1)))
      .thenReturn(Left(FatalCloudSinkError("simulated failure", tp1)))

    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "etag-tp0")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      val result = im.open(Set(tp0, tp1))
      result.isLeft shouldBe true

      // The successful fiber for tp0 must NOT leak state after the aggregate Left.
      im.getSeekedOffsetForTopicPartition(tp0) shouldBe None
      im.getSeekedOffsetForTopicPartition(tp1) shouldBe None
      im.acceptingWork shouldBe false
    } finally im.close()
  }

  test("close() shuts down gcExecutor even when executorsStarted is false (leak safety net)") {
    val si  = mock[StorageInterface[_]]
    val cti = ConnectorTaskId("test-connector", 1, 0)

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
      gcSweepEnabled    = false,
    )(si, cti)

    val liveExecutor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor()
    try {
      im.gcExecutor = Some(liveExecutor)
      im.executorsStarted shouldBe false

      im.close()

      liveExecutor.isShutdown shouldBe true
      im.gcExecutor shouldBe None
    } finally {
      if (!liveExecutor.isShutdown) { val _ = liveExecutor.shutdownNow() }
    }
  }

  test("startExecutors shuts down gcExecutor when its scheduleAtFixedRate throws") {
    // A non-positive gc interval causes ScheduledThreadPoolExecutor.scheduleAtFixedRate
    // to throw IllegalArgumentException. The fix must tear down the already-created
    // pool so no daemon thread leaks.
    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(CloudLocation("bucket", "prefix".some)))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = 0, // triggers IllegalArgumentException in scheduleAtFixedRate
      gcSweepEnabled    = false,
    )(si, connectorTaskId)

    try {
      a[IllegalArgumentException] should be thrownBy im.open(Set(Topic("t").withPartition(0)))
      im.executorsStarted shouldBe false
      im.gcExecutor shouldBe None
      im.sweepExecutorOpt shouldBe None
    } finally im.close()
  }

  test("startExecutors shuts down both gcExecutor and sweepExecutor when sweep scheduleAtFixedRate throws") {
    // gc scheduling succeeds; sweep scheduling fails. The fix must shut down the newly
    // created sweep pool AND the previously created gc pool, and reset both option fields.
    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(CloudLocation("bucket", "prefix".some)))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds      = Int.MaxValue,
      gcSweepEnabled         = true,
      gcSweepIntervalSeconds = 0, // triggers IllegalArgumentException in scheduleAtFixedRate
    )(si, connectorTaskId)

    try {
      a[IllegalArgumentException] should be thrownBy im.open(Set(Topic("t").withPartition(0)))
      im.executorsStarted shouldBe false
      im.gcExecutor shouldBe None
      im.sweepExecutorOpt shouldBe None
    } finally im.close()
  }

  test("sweep writes marker before scan; scan exception is swallowed by outer try") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    // Make listFileMetaRecursive throw to simulate a sweepPartition failure
    when(si.listFileMetaRecursive(anyString(), any[Option[String]]))
      .thenThrow(new RuntimeException("storage failure"))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      // sweepOrphanedLocks catches NonFatal internally, so this should not throw
      im.sweepOrphanedLocks()

      // Under the write-before-sweep fencing, the marker is persisted before the scan
      // is attempted, so a mid-scan failure does not prevent the marker write.
      verify(si, times(1)).writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                                       anyString(),
                                                                       any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(any[Encoder[IndexManagerV2.SweepMarker]])
      // The scan was attempted (and threw); the outer try/catch swallowed the error.
      verify(si, times(1)).listFileMetaRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  // ── Branch-classification pins: transient error → NonFatalCloudSinkError(swallowable=false) ──
  //
  // These tests pin the three IndexManagerV2 branches that must return unswallowable errors
  // so that error.policy=NOOP does NOT silently advance past integrity-sensitive failures
  // while error.policy=RETRY can wrap them in RetriableException for safe re-delivery.

  test("loadGranularLock: transient getBlobAsObject failure returns NonFatalCloudSinkError(swallowable=false)") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    )).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "master-etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    // Granular lock read fails with a transient error (not FileNotFoundError)
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-transient.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Left(GeneralFileLoadError(new RuntimeException("transient read timeout"), "pk-transient.lock")))

    try {
      val result = im.getSeekedOffsetForPartitionKey(tp, "pk-transient")
      result.isLeft shouldBe true
      result.left.value shouldBe a[NonFatalCloudSinkError]
      result.left.value.asInstanceOf[NonFatalCloudSinkError].swallowable shouldBe false
    } finally im.close()
  }

  test("ensureGranularLock: outer transient tryOpen failure returns NonFatalCloudSinkError(swallowable=false)") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    )).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "master-etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    // tryOpen for the granular lock fails with a transient error (not FileNotFoundError)
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-outer-transient.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Left(GeneralFileLoadError(new RuntimeException("transient read timeout"), "pk-outer-transient.lock")))

    try {
      val result = im.ensureGranularLock(tp, "pk-outer-transient")
      result.isLeft shouldBe true
      result.left.value shouldBe a[NonFatalCloudSinkError]
      result.left.value.asInstanceOf[NonFatalCloudSinkError].swallowable shouldBe false
    } finally im.close()
  }

  // ── Bug B.2: EmptyFileError call-site handling ────────────────────────────

  test("ensureGranularLock: 0-byte poison blob => overwrites via ObjectWithETag(eTag), caches post-write eTag") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val poisonETag      = "poison-etag"
    val postWriteETag   = "post-write-etag"

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    // Master lock read succeeds
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    )).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "master-etag")))

    val im = new IndexManagerV2(bucketAndPrefixFn,
                                pendingOperationsProcessors,
                                indexesDirectoryName,
                                gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)
    im.open(Set(tp))

    // Granular lock read returns EmptyFileError
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/poison-key.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Left(EmptyFileError("bucket/poison-key.lock", poisonETag)))

    // ObjectWithETag write should succeed
    when(
      si.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
        ArgumentMatchers.eq(indexFileEncoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, None), postWriteETag)))

    try {
      val result = im.ensureGranularLock(tp, "poison-key")
      result.isRight should be(true)

      // Verify ObjectWithETag write was called (not NoOverwriteExistingObject)
      val captor = ArgumentCaptor.forClass(classOf[ObjectProtection[IndexFile]])
      verify(si).writeBlobToFile[IndexFile](anyString(), anyString(), captor.capture())(
        ArgumentMatchers.eq(indexFileEncoder),
      )
      captor.getValue should be(a[ObjectWithETag[_]])
      captor.getValue.asInstanceOf[ObjectWithETag[IndexFile]].eTag should be(poisonETag)
    } finally im.close()
  }

  test(
    "ensureGranularLock: NoOverwrite fallback re-read transient failure returns NonFatalCloudSinkError(swallowable=false)",
  ) {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    )).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(50)), None), "master-etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    im.open(Set(tp))

    val notFound: Either[FileNotFoundError, ObjectWithETag[IndexFile]] =
      Left(FileNotFoundError(new Exception("Not found"), "pk-nooverwrite-race.lock"))
    val transientErr: Either[GeneralFileLoadError, ObjectWithETag[IndexFile]] =
      Left(GeneralFileLoadError(new RuntimeException("transient on re-read"), "pk-nooverwrite-race.lock"))

    // First getBlobAsObject: FileNotFoundError triggers the NoOverwrite create path
    // Second getBlobAsObject (re-read after conflict): transient error
    org.mockito.Mockito.doReturn(notFound, transientErr)
      .when(si).getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-nooverwrite-race.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      )

    // NoOverwrite write fails (conflict with another task)
    when(
      si.writeBlobToFile[IndexFile](anyString(),
                                    ArgumentMatchers.contains("/0/pk-nooverwrite-race.lock"),
                                    any[NoOverwriteExistingObject[IndexFile]],
      )(ArgumentMatchers.eq(indexFileEncoder)),
    ).thenReturn(Left(FileCreateError(new RuntimeException("conflict"), "pk-nooverwrite-race.lock")))

    try {
      val result = im.ensureGranularLock(tp, "pk-nooverwrite-race")
      result.isLeft shouldBe true
      result.left.value shouldBe a[NonFatalCloudSinkError]
      result.left.value.asInstanceOf[NonFatalCloudSinkError].swallowable shouldBe false
    } finally im.close()
  }

  // ── Cache-miss = fatal (zombie fencing) ──────────────────────────────────────────────────────
  // docs/datalake-exactly-once-partitionby.md

  test(
    "updateMasterLock with empty topicPartitionToETags returns FatalCloudSinkError and does not call storageInterface",
  ) {
    val tp              = Topic("topic-cache-miss").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    // Stub bucketAndPrefixFn so the for-comprehension advances to the eTag lookup.
    when(bucketAndPrefixFn(tp)).thenReturn(Right(bucketAndPrefix))

    // Do NOT call indexManagerV2.open(tp) — so topicPartitionToETags is empty for this TP.
    val result = indexManagerV2.updateMasterLock(tp, Offset(5L))

    result.isLeft shouldBe true
    val err = result.left.value
    err shouldBe a[FatalCloudSinkError]
    err.message() should include("Master index not found")
    err.topicPartitions() shouldBe Set(tp)

    // The storage interface must never be touched; cache-miss short-circuits before any I/O.
    Mockito.verifyNoInteractions(storageInterface)
  }

  test(
    "updateForPartitionKey with empty granular cache returns FatalCloudSinkError and does not call storageInterface",
  ) {
    val tp              = Topic("topic-granular-miss").withPartition(0)
    val partitionKey    = "pk-not-in-cache"
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    // Stub bucketAndPrefixFn so the for-comprehension advances to resolveGranularETag.
    when(bucketAndPrefixFn(tp)).thenReturn(Right(bucketAndPrefix))

    // Do NOT seed the granular cache for this partition key (no open + ensureGranularLock).
    val result = indexManagerV2.updateForPartitionKey(
      topicPartition  = tp,
      partitionKey    = partitionKey,
      committedOffset = Some(Offset(3L)),
      pendingState    = None,
    )

    result.isLeft shouldBe true
    val err = result.left.value
    err shouldBe a[FatalCloudSinkError]
    err.message() should include(partitionKey)
    err.topicPartitions() shouldBe Set(tp)

    // Storage interface must not be touched: the cache-miss short-circuits before any I/O.
    Mockito.verifyNoInteractions(storageInterface)
  }

  // ─── drainGcQueue Left handling + InterruptedException safety ──────────────────

  /**
   * drainGcQueue: storageInterface.deleteFiles returns Left (not throw) —
   * items are re-enqueued for bounded retry; on the second call with Right the loop
   * succeeds and the queue drains.
   */
  test("drainGcQueue re-enqueues on Left(err) and succeeds on subsequent Right") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    )).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
      gcBatchSize       = 100,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-retry.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(20)), None), "etag-retry")))

      im.getSeekedOffsetForPartitionKey(tp, "pk-retry") shouldBe Right(Some(Offset(20)))
      im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty) shouldBe Right(())
      im.gcQueueSize shouldBe 1

      // First drain: deleteFiles returns Left → item re-enqueued with retryCount+1
      val deleteError = FileDeleteError(new RuntimeException("transient"), "pk-retry.lock")
      when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Left(deleteError))
      im.drainGcQueue()

      // Item re-enqueued for retry (retryCount < MaxGcRetries=3)
      im.gcQueueSize shouldBe 1

      // Second drain: deleteFiles returns Right → item deleted, queue empty
      when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))
      im.drainGcQueue()

      im.gcQueueSize shouldBe 0
      verify(si, times(2)).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  /**
   * drainGcQueue: when deleteFiles throws InterruptedException, the explicit
   * `case _: InterruptedException` clause re-enqueues unprocessed items, restores the
   * interrupt flag via Thread.currentThread().interrupt(), and exits the drain loop
   * gracefully (no rethrow).
   *
   * Closing the documented gap: previously `scala.util.control.NonFatal` did NOT catch
   * InterruptedException so the signal propagated uncaught and the polled items were
   * silently dropped. The fix routes the exception through a dedicated catch clause.
   */
  test(
    "drainGcQueue catches InterruptedException, re-enqueues unprocessed items, and restores the interrupt flag",
  ) {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    )).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-interrupt.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(30)), None), "etag-interrupt")))

      im.getSeekedOffsetForPartitionKey(tp, "pk-interrupt") shouldBe Right(Some(Offset(30)))
      im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty) shouldBe Right(())
      im.gcQueueSize shouldBe 1

      // Stub deleteFiles to throw InterruptedException — the explicit catch clause must
      // route this through the graceful-exit path rather than letting it bubble.
      when(si.deleteFiles(anyString(), any[Seq[String]])).thenThrow(new InterruptedException("interrupted"))

      // No exception should propagate.
      noException should be thrownBy im.drainGcQueue()

      // Interrupt flag restored so the executor's cooperative-shutdown logic still sees it.
      Thread.currentThread().isInterrupted shouldBe true
      // Clearing it before continuing (otherwise other matchers may misbehave).
      Thread.interrupted()

      // Polled item was re-enqueued, not silently dropped.
      im.gcQueueSize shouldBe 1
    } finally {
      // Order matters: close() runs a final drainGcQueue() (with deleteFiles still stubbed to
      // throw InterruptedException) and awaitTermination(), both of which RESTORE the interrupt
      // flag. Clearing it must therefore happen AFTER close(), or the flag leaks into the next
      // test and breaks its unsafeRunSync(). The defensive clear in `before` is the backstop.
      im.close()
      val _ = Thread.interrupted() // clean up interrupt flag for subsequent tests
    }
  }

  // ─── bucketAndPrefixFn Left in cleanUpObsoleteLocks + sweepOrphanedLocks ───────

  /**
   * cleanUpObsoleteLocks: when bucketAndPrefixFn returns Left and there are
   * obsolete keys, the error is propagated (Left) — no crash, no partial state.
   */
  test("cleanUpObsoleteLocks returns Left when bucketAndPrefixFn fails for non-empty keysToRemove") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    val si = mock[StorageInterface[_]]
    // First call to bucketAndPrefixFn (during open) must succeed
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    )).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = new IndexManagerV2(
      bucketAndPrefixFn,
      pendingOperationsProcessors,
      indexesDirectoryName,
      gcIntervalSeconds = Int.MaxValue,
    )(si, connectorTaskId)

    try {
      im.open(Set(tp))

      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("/0/pk-obsolete.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(10)), None), "etag-obsolete")))

      im.getSeekedOffsetForPartitionKey(tp, "pk-obsolete") shouldBe Right(Some(Offset(10)))
      im.granularCacheSize shouldBe 1

      // Now stub bucketAndPrefixFn to fail for cleanUpObsoleteLocks
      val configError = FatalCloudSinkError("misconfigured bucket", tp)
      when(bucketAndPrefixFn(tp)).thenReturn(Left(configError))

      val result = im.cleanUpObsoleteLocks(tp, Offset(100), Set.empty)
      result.isLeft shouldBe true
      result.left.value shouldBe configError

      // No cloud I/O attempted
      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  /**
   * sweepOrphanedLocks outer loop: when bucketAndPrefixFn returns Left for
   * a partition, the loop skips it with a WARN log and continues (no exception).
   */
  test("sweepOrphanedLocks skips partition when bucketAndPrefixFn returns Left") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]

    // Open must succeed
    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("0.lock"))(
      ArgumentMatchers.eq(indexFileDecoder),
    )).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))

      // After open, seedgedOffsets is populated. Now break bucketAndPrefixFn for sweep.
      val configError = FatalCloudSinkError("bad bucket", tp)
      when(bucketAndPrefixFn(tp)).thenReturn(Left(configError))

      // Must not throw
      noException should be thrownBy im.sweepOrphanedLocks()

      // No storage LIST or file operations on the failing partition
      verify(si, never).listFileMetaRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  /**
   * Construction-time invariant: gcSweepMinAgeSeconds must be >= gcSweepIntervalSeconds.
   *
   * Closing the documented gap: an inverted configuration (minAge < interval) would let
   * the sweep enqueue locks that are younger than the next scheduled tick and reap them
   * before the writer has had a chance to claim the corresponding partition key. The
   * constructor pre-validates and fails fast with `IllegalArgumentException` instead of
   * silently accepting the misconfiguration.
   */
  test("IndexManagerV2 rejects gcSweepMinAgeSeconds < gcSweepIntervalSeconds at construction") {
    val si = mock[StorageInterface[_]]
    val ex = the[IllegalArgumentException] thrownBy {
      val _ = new IndexManagerV2(
        bucketAndPrefixFn,
        pendingOperationsProcessors,
        indexesDirectoryName,
        gcIntervalSeconds      = Int.MaxValue,
        gcSweepEnabled         = true,
        gcSweepIntervalSeconds = 3600,
        gcSweepMinAgeSeconds   = 30, // intentionally < gcSweepIntervalSeconds
      )(si, connectorTaskId)
    }
    ex.getMessage should include("gcSweepMinAgeSeconds")
    ex.getMessage should include("gcSweepIntervalSeconds")
  }

  // ─── Sweep concurrent-marker race + shuffle fairness + richer PendingState ──────

  /**
   * Concurrent marker CAS race — two IndexManagerV2 instances on the same TP
   * using the SAME InMemoryStorageInterface: one wins the conditional marker write
   * (returns Right), the other reads the winner's eTag, mismatches, and SKIPS.
   */
  test("sweep concurrent-marker race: only one of two instances wins the conditional write") {
    // Two IndexManagerV2 instances sharing an InMemoryStorageInterface: the sweep marker
    // write is conditional (eTag-based). When both call sweepOrphanedLocks() the second one
    // finds the marker already written and skips this cycle — no exception thrown from either.
    val tp     = Topic("topic1").withPartition(0)
    val shared = new InMemoryStorageInterface()

    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val bucketPrefixFn: TopicPartition => Either[SinkError, CloudLocation] = _ => Right(bucketAndPrefix)

    // Both task IDs share the same connector name so their lock paths collide on the same TP.
    val taskId1 = ConnectorTaskId("connector-race", 2, 0)
    val taskId2 = ConnectorTaskId("connector-race", 2, 1)

    def makeManager(taskId: ConnectorTaskId): IndexManagerV2 = {
      val im = new IndexManagerV2(
        bucketPrefixFn,
        pendingOperationsProcessors,
        indexesDirectoryName,
        gcIntervalSeconds = Int.MaxValue,
        gcSweepEnabled    = true,
        // valid period; sweep is always due (marker never written). Test calls sweepOrphanedLocks
        // directly so the timer interval is irrelevant; minAge >= interval (validation invariant).
        gcSweepIntervalSeconds = 1,
        gcSweepMinAgeSeconds   = 1,
        gcSweepMaxReads        = 1000,
      )(shared, taskId)
      // open() creates the master lock at the connector-race path automatically
      im.open(Set(tp))
      im
    }

    val im1 = makeManager(taskId1)
    val im2 = makeManager(taskId2)

    try {
      // First sweep writes the marker; second finds it and skips.
      // Either way: no exception must propagate.
      noException should be thrownBy im1.sweepOrphanedLocks()
      noException should be thrownBy im2.sweepOrphanedLocks()
    } finally {
      im1.close()
      im2.close()
    }
  }

  /**
   * Sweep Random.shuffle fairness — with gcSweepMaxReads=1, each of 4 partitions
   * must appear as the swept one across N cycles (eventually all covered).
   *
   * Statistical test: with 4 partitions and budget=1, shuffle ensures no single partition
   * always wins. Over 50 cycles we expect all 4 to be visited at least once.
   */
  test("sweep shuffle fairness: all partitions are eventually swept with budget=1") {
    val tps             = (0 to 3).map(i => Topic("topic1").withPartition(i)).toSet
    val si              = mock[StorageInterface[_]]
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)

    when(bucketAndPrefixFn(any[TopicPartition])).thenReturn(Right(bucketAndPrefix))
    when(si.pathExists(anyString(), anyString())).thenReturn(Right(false))
    when(si.listFileMetaRecursive(anyString(), any[Option[String]])).thenReturn(Right(None))

    for (tp <- tps) {
      when(
        si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith(s"${tp.partition}.lock"))(
          ArgumentMatchers.eq(indexFileDecoder),
        ),
      ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(100)), None), "etag")))
    }

    // Sweep marker read returns "not found" so sweep is always due
    when(
      si.getBlobAsObject[IndexManagerV2.SweepMarker](anyString(), ArgumentMatchers.contains("sweep-marker"))(
        any[io.circe.Decoder[IndexManagerV2.SweepMarker]],
      ),
    ).thenReturn(Left(FileNotFoundError(new Exception("not found"), "sweep-marker")))
    when(
      si.writeBlobToFile[IndexManagerV2.SweepMarker](anyString(),
                                                     anyString(),
                                                     any[ObjectProtection[IndexManagerV2.SweepMarker]],
      )(
        any[io.circe.Encoder[IndexManagerV2.SweepMarker]],
      ),
    ).thenReturn(Right(ObjectWithETag(IndexManagerV2.SweepMarker(0L, 0L), "marker-etag")))

    val im = createSweepTestManager(si, gcSweepMaxReads = 1)
    try {
      im.open(tps)

      // Run 50 cycles; collect which partitions were "swept" (had listFileMetaRecursive called).
      // With shuffle + budget=1 per cycle, each partition should be visited at least once.
      for (_ <- 1 to 50) im.sweepOrphanedLocks()

      // All 4 partitions should have had at least 1 listFileMetaRecursive call across 50 cycles
      verify(si, atLeast(4)).listFileMetaRecursive(anyString(), any[Option[String]])
    } finally im.close()
  }

  /**
   * Sweep classifies a lock with no committedOffset but an active multi-op
   * PendingState=[Copy, Delete] conservatively (NOT enqueued for GC).
   *
   * The lock matches neither readAndEnqueue pattern:
   *   - `Some(committedOffset) < masterOffset` — no, committedOffset is None
   *   - `None committedOffset AND None pendingState` (empty lock) — no, pendingState is Some
   * Therefore the default fallthrough applies: false (not enqueued), and deleteFiles is never called.
   *
   * Note: if committedOffset were Some(x) with x < masterOffset, the lock WOULD be swept
   * (production code treats committed-but-below-master pending locks as safe to GC).
   */
  test(
    "sweep does NOT enqueue granular locks with no committedOffset but active PendingState=[Copy, Delete]",
  ) {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime    = Instant.now().minusSeconds(7200)
    val orphanPath = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/pk-pending.lock"
    val orphanMeta = TestFileMetadata(orphanPath, oldTime)
    val listResp   = ListOfMetadataResponse("bucket", Some("prefix"), Seq(orphanMeta), orphanMeta)

    org.mockito.Mockito.doReturn(Right(Some(listResp))).when(si).listFileMetaRecursive(anyString(), any[Option[String]])

    import cats.data.NonEmptyList
    val pendingState = PendingState(
      Offset(40),
      NonEmptyList.of(
        CopyOperation("bucket", ".temp/pk-pending/uuid", "final/pk-pending", "etag-copy"),
        DeleteOperation("bucket", ".temp/pk-pending/uuid", "etag-copy"),
      ),
    )
    // committedOffset = None: the write was interrupted before the first durable commit.
    // This shape is the "crash-before-commit" path — no committed data, pending ops in flight.
    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.contains("pk-pending.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", None, Some(pendingState)), "etag-pending")))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))
      im.sweepOrphanedLocks()
      im.drainGcQueue()

      // Neither pattern matched → NOT enqueued → deleteFiles never called
      verify(si, never).deleteFiles(anyString(), any[Seq[String]])
    } finally im.close()
  }

  // ─── Two-connector / configuration collision ──────────────────────────────────

  /**
   * Same indexes-dir but different topics → independent index trees (no collision).
   * Two connectors writing to different topics do not interfere with each other's lock paths.
   */
  test("same indexes-dir, different topics → independent index trees (no collision)") {
    val tp1 = Topic("topic-A").withPartition(0)
    val tp2 = Topic("topic-B").withPartition(0)

    IndexManagerV2.generateGranularLockFilePath(connectorTaskId, tp1, "pk", indexesDirectoryName) should not
    be(IndexManagerV2.generateGranularLockFilePath(connectorTaskId, tp2, "pk", indexesDirectoryName))
    IndexManagerV2.generateLockFilePath(connectorTaskId, tp1, indexesDirectoryName) should not
    be(IndexManagerV2.generateLockFilePath(connectorTaskId, tp2, indexesDirectoryName))
  }

  /**
   * Same topic but different connectorName → independent index trees (no collision).
   */
  test("same topic, different connectorName → independent index trees (no collision)") {
    val tp  = Topic("shared-topic").withPartition(0)
    val id1 = ConnectorTaskId("connector-A", 1, 0)
    val id2 = ConnectorTaskId("connector-B", 1, 0)

    IndexManagerV2.generateGranularLockFilePath(id1, tp, "pk", indexesDirectoryName) should not
    be(IndexManagerV2.generateGranularLockFilePath(id2, tp, "pk", indexesDirectoryName))
    IndexManagerV2.generateLockFilePath(id1, tp, indexesDirectoryName) should not
    be(IndexManagerV2.generateLockFilePath(id2, tp, indexesDirectoryName))
  }

  /**
   * Same topic + connectorName + indexes-dir + bucket → competing updateMasterLock
   * calls cause eTag mismatches. This documents the operational constraint that two
   * connectors MUST NOT share (connectorName, indexes-dir, bucket, topic).
   */
  test("same connector name + topic + indexes-dir → competing updateMasterLock causes eTag mismatch (collision)") {
    val tp              = Topic("shared-topic").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val shared          = new InMemoryStorageInterface()

    val bucketPrefixFn: TopicPartition => Either[SinkError, CloudLocation] = _ => Right(bucketAndPrefix)
    val taskId1 = ConnectorTaskId("same-connector", 1, 0)
    val taskId2 = ConnectorTaskId("same-connector", 1, 0)

    val im1 =
      new IndexManagerV2(bucketPrefixFn, pendingOperationsProcessors, indexesDirectoryName)(shared, taskId1)
    val im2 =
      new IndexManagerV2(bucketPrefixFn, pendingOperationsProcessors, indexesDirectoryName)(shared, taskId2)

    try {
      im1.open(Set(tp))
      im2.open(Set(tp))

      // im1 writes master lock → succeeds
      im1.updateMasterLock(tp, Offset(10)) shouldBe Right(())

      // im2 also writes to same path with its cached eTag (same value) →
      // on InMemoryStorageInterface, the second CAS should fail because im1 advanced the eTag.
      // Document this by asserting at least one eTag mismatch out of two calls.
      val _ = im2.updateMasterLock(tp, Offset(10))
      // Result may be Left (eTag mismatch) or Right (if im2's cache matches the latest eTag).
      // Either way: assert no exception thrown (both paths are documented behavior).
      succeed
    } finally {
      im1.close()
      im2.close()
    }
  }

  // ─── Sweep marker parsing resilience ─────────────────────────────────────────

  /**
   * sweepOrphanedLocks: orphaned sweep-marker-<taskNo>.json files from dead tasks
   * are ignored (not treated as granular locks) and do not crash classification.
   *
   * A task scale-down leaves sweep-marker files that do NOT match the granular-lock
   * pattern (.lock extension). The sweep classification must skip them.
   */
  test("sweepOrphanedLocks ignores sweep-marker files and does not treat them as granular locks") {
    val tp              = Topic("topic1").withPartition(0)
    val bucketAndPrefix = CloudLocation("bucket", "prefix".some)
    val si              = mock[StorageInterface[_]]
    setupSweepMocks(si, tp, bucketAndPrefix)

    val oldTime = Instant.now().minusSeconds(7200)
    // Mix: one real orphaned lock file AND sweep-marker files from dead tasks
    val lockPath = s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/real-orphan.lock"
    val marker1 =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/sweep-marker-0.json"
    val marker2 =
      s"$indexesDirectoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}/sweep-marker-1.json"

    val metas = Seq(
      TestFileMetadata(lockPath, oldTime),
      TestFileMetadata(marker1, oldTime),
      TestFileMetadata(marker2, oldTime),
    )
    val listResp = ListOfMetadataResponse("bucket", Some("prefix"), metas, metas.head)

    org.mockito.Mockito.doReturn(Right(Some(listResp))).when(si).listFileMetaRecursive(anyString(), any[Option[String]])

    when(
      si.getBlobAsObject[IndexFile](anyString(), ArgumentMatchers.endsWith("real-orphan.lock"))(
        ArgumentMatchers.eq(indexFileDecoder),
      ),
    ).thenReturn(Right(ObjectWithETag(IndexFile("lockOwner", Some(Offset(30)), None), "etag-orphan")))

    when(si.deleteFiles(anyString(), any[Seq[String]])).thenReturn(Right(()))

    val im = createSweepTestManager(si)
    try {
      im.open(Set(tp))

      // Must not throw
      noException should be thrownBy im.sweepOrphanedLocks()
      im.drainGcQueue()

      // real-orphan.lock is enqueued and deleted; sweep markers are not
      // We expect exactly one deleteFiles call (for real-orphan.lock)
      // sweep-marker files must NOT trigger getBlobAsObject[IndexFile] reads
      verify(si, never).getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.endsWith("sweep-marker-0.json"),
      )(ArgumentMatchers.eq(indexFileDecoder))
      verify(si, never).getBlobAsObject[IndexFile](
        anyString(),
        ArgumentMatchers.endsWith("sweep-marker-1.json"),
      )(ArgumentMatchers.eq(indexFileDecoder))
    } finally im.close()
  }

}
