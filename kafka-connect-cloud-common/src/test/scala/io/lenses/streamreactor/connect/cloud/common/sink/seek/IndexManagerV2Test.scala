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
    verifyLockFilePathUsed(storageInterface, directoryFileName, topicPartition)
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
  ): Assertion = {
    val pathCaptor = ArgumentCaptor.forClass(classOf[String])
    verify(storageInterface).getBlobAsObject[IndexFile](anyString(), pathCaptor.capture())(any[Decoder[IndexFile]])
    val usedPath = pathCaptor.getValue
    usedPath should be(s"$directoryFileName/.locks/${topicPartition.topic}/${topicPartition.partition}.lock")
  }

  test("generateLockFilePath uses the provided directoryFileName") {
    val connectorTaskId   = ConnectorTaskId("my-connector", 1, 0)
    val topicPartition    = Topic("my-topic").withPartition(5)
    val directoryFileName = "my-index-dir"

    val lockFilePath = IndexManagerV2.generateLockFilePath(connectorTaskId, topicPartition, directoryFileName)

    lockFilePath should be(s"$directoryFileName/.locks/Topic(my-topic)/5.lock")
  }

}
