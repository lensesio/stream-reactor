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
import io.lenses.streamreactor.connect.cloud.common.storage.FileNotFoundError
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
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

  test("open should successfully process pending operations without race condition") {
    // This test verifies that the race condition bug has been fixed. Previously, pending operations
    // were processed before the eTag was stored in topicPartitionToETags map, causing the update()
    // method to fail with "Index not found" error. Now it should work correctly.
    
    val topicPartition = Topic("my-topic").withPartition(0)
    val bucketAndPrefix = CloudLocation("my-bucket", "prefix".some)
    
    // Create pending operations similar to the bug report
    val pendingOperations = NonEmptyList.of(
      CopyOperation(
        bucket = "my-bucket",
        source = ".temp-upload/Topic(my-topic)/0/b25f433f-d790-4ba1-b6d6-871f07c7a211kafka/topics/497de2d9f0370cd3/my-topic/0/000000000773_1756721073301_1756810548919.avro",
        destination = "kafka/topics/497de2d9f0370cd3/my-topic/0/000000000773_1756721073301_1756810548919.avro",
        eTag = "\"004ef63902cbd9da30709e77c0132bf6\""
      ),
      DeleteOperation(
        bucket = "my-bucket",
        source = ".temp-upload/Topic(my-topic)/0/b25f433f-d790-4ba1-b6d6-871f07c7a211kafka/topics/497de2d9f0370cd3/my-topic/0/000000000773_1756721073301_1756810548919.avro",
        eTag = "\"004ef63902cbd9da30709e77c0132bf6\""
      )
    )
    
    val pendingState = PendingState(
      pendingOffset = Offset(773),
      pendingOperations = pendingOperations
    )
    
    val indexFile = IndexFile(
      owner = "bbee8a19-639b-4a5d-9f6f-b344f9bb00c0",
      committedOffset = Some(Offset(733)),
      pendingState = Some(pendingState)
    )
    
    val objectWithETag = ObjectWithETag(indexFile, "original-etag")
    
    // Setup mocks
    when(bucketAndPrefixFn(topicPartition)).thenReturn(Right(bucketAndPrefix))
    when(storageInterface.getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder)))
      .thenReturn(Right(objectWithETag))
    
    // Mock the storage interface methods that will be called during pending operations processing
    when(storageInterface.mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]]))
      .thenReturn(Right(()))
    when(storageInterface.deleteFile(anyString(), anyString(), anyString()))
      .thenReturn(Right(()))
    
    // Mock writeBlobToFile to simulate successful index file updates during pending operations processing
    when(storageInterface.writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
      ArgumentMatchers.eq(indexFileEncoder)
    )).thenReturn(Right(ObjectWithETag(indexFile.copy(pendingState = None, committedOffset = Some(Offset(773))), "updated-etag")))
    
    // Create a real PendingOperationsProcessors instance to test the actual flow
    val realPendingOperationsProcessors = new PendingOperationsProcessors(storageInterface)
    
    // Create a new IndexManagerV2 with the real pending operations processor
    val realIndexManagerV2 = new IndexManagerV2(
      bucketAndPrefixFn,
      oldIndexManager,
      realPendingOperationsProcessors,
      indexesDirectoryName,
    )(storageInterface, connectorTaskId)
    
    // The open method should now succeed and process pending operations correctly
    // The fix ensures that:
    // 1. Index file is loaded with pending operations
    // 2. eTag is stored in topicPartitionToETags map before processing pending operations
    // 3. processPendingOperations is called which calls update() 
    // 4. update() can successfully get eTag from topicPartitionToETags map
    // 5. Pending operations are processed successfully
    val result = realIndexManagerV2.open(Set(topicPartition))
    
    // Verify the operation succeeds
    result.isRight shouldBe true
    result.value shouldBe Map(topicPartition -> Some(Offset(773)))
    
    // Verify that the storage interface was called to load the index file
    verify(storageInterface).getBlobAsObject[IndexFile](anyString(), anyString())(ArgumentMatchers.eq(indexFileDecoder))
    
    // Verify that pending operations were processed (copy and delete operations)
    verify(storageInterface).mvFile(anyString(), anyString(), anyString(), anyString(), any[Option[String]])
    verify(storageInterface).deleteFile(anyString(), anyString(), anyString())
    
    // Verify that the index file was updated after processing pending operations
    // Note: writeBlobToFile may be called multiple times during pending operations processing
    verify(storageInterface, times(2)).writeBlobToFile[IndexFile](anyString(), anyString(), any[ObjectWithETag[IndexFile]])(
      ArgumentMatchers.eq(indexFileEncoder)
    )
  }

}
