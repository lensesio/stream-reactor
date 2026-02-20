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
package io.lenses.streamreactor.connect.cloud.common.sink.writer

import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext

class WriterCommitManagerTest
    extends AnyFunSuiteLike
    with BeforeAndAfterEach
    with Matchers
    with MockitoSugar
    with EitherValues {

  private implicit val connectorTaskId: ConnectorTaskId = mock[ConnectorTaskId]
  private val topicPartition = Topic("topic").withPartition(0)
  private val partitionField = PartitionField(Seq("_value")).value

  override protected def beforeEach(): Unit = reset(mock[ConnectorTaskId])

  private def createManager(writers: Map[MapKey, Writer[FileMetadata]]) =
    new WriterCommitManager(() => writers, ExecutionContext.global, Duration.Inf)

  private def createMockWriter(
    hasPendingUpload: Boolean                           = false,
    shouldFlush:      Boolean                           = false,
    commitResult:     Either[FatalCloudSinkError, Unit] = ().asRight,
  ): Writer[FileMetadata] = {
    val mockWriter = mock[Writer[FileMetadata]]
    when(mockWriter.hasPendingUpload).thenReturn(hasPendingUpload)
    when(mockWriter.shouldFlush).thenReturn(shouldFlush)
    when(mockWriter.commit).thenReturn(commitResult)
    mockWriter
  }

  // Tests for commitPending
  test("commitPending should commit writers with pending uploads") {
    val mockWriter = createMockWriter(hasPendingUpload = true)
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitPending().value shouldBe ()
    verify(mockWriter).commit
  }

  test("commitPending should return an error if any writer fails to commit") {
    val mockWriter = createMockWriter(hasPendingUpload = true,
                                      commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft,
    )
    val manager = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitPending().left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError("commit failed",
                                                                                            topicPartition,
    )))
  }

  test("commitPending should commit all writers for the given topic partition if one has pending uploads") {
    val mockWriter1 = createMockWriter(hasPendingUpload = true)
    val mockWriter2 = createMockWriter()
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitPending().value shouldBe ()
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }

  test("commitPending should return an error if any writer fails to commit for the given topic partition") {
    val mockWriter1 = createMockWriter(hasPendingUpload = true)
    val mockWriter2 = createMockWriter(commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft)
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitPending().left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError("commit failed",
                                                                                            topicPartition,
    )))
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }

  // Tests for commitForTopicPartition
  test("commitForTopicPartition should commit all writers for the given topic partition if one is committed") {
    val mockWriter1 = createMockWriter()
    val mockWriter2 = createMockWriter()
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }

  test("commitForTopicPartition should return an error if any writer fails to commit for the given topic partition") {
    val mockWriter1 = createMockWriter()
    val mockWriter2 = createMockWriter(commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft)
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitForTopicPartition(topicPartition).left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError(
      "commit failed",
      topicPartition,
    )))
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }

  test("commitForTopicPartition should commit writers for the given topic partition") {
    val mockWriter = createMockWriter()
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter).commit
  }

  test("commitForTopicPartition should return an error if any writer fails to commit") {
    val mockWriter = createMockWriter(commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft)
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitForTopicPartition(topicPartition).left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError(
      "commit failed",
      topicPartition,
    )))
  }

  // Tests for commitFlushableWriters
  test("commitFlushableWriters should commit writers that require flush") {
    val mockWriter = createMockWriter(shouldFlush = true)
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitFlushableWriters().value shouldBe ()
    verify(mockWriter).commit
  }

  test("commitFlushableWriters should return an error if any writer fails to commit") {
    val mockWriter =
      createMockWriter(shouldFlush = true, commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft)
    val manager = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitFlushableWriters().left.value shouldBe BatchCloudSinkError(Set(FatalCloudSinkError("commit failed",
                                                                                                     topicPartition,
    )))
  }

  // Tests for commitFlushableWritersForTopicPartition
  test("commitFlushableWritersForTopicPartition should commit flushable writers for the given topic partition") {
    val mockWriter = createMockWriter(shouldFlush = true)
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitFlushableWritersForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter).commit
  }

  test("commitFlushableWritersForTopicPartition should continue if no writers require flushing") {
    val mockWriter = createMockWriter()
    val manager    = createManager(Map(MapKey(topicPartition, Map.empty) -> mockWriter))

    manager.commitFlushableWritersForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter, times(0)).commit
  }

  test(
    "commitFlushableWritersForTopicPartition should commit all writers for the given topic partition if one is flushable",
  ) {
    val mockWriter1 = createMockWriter(shouldFlush = true)
    val mockWriter2 = createMockWriter()
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitFlushableWritersForTopicPartition(topicPartition).value shouldBe ()
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }

  test(
    "commitFlushableWritersForTopicPartition should return an error if any writer fails to commit for the given topic partition",
  ) {
    val mockWriter1 = createMockWriter(shouldFlush = true)
    val mockWriter2 = createMockWriter(commitResult = FatalCloudSinkError("commit failed", topicPartition).asLeft)
    val manager = createManager(Map(
      MapKey(topicPartition, Map.empty) -> mockWriter1,
      MapKey(topicPartition, Map(partitionField -> "value")) -> mockWriter2,
    ))

    manager.commitFlushableWritersForTopicPartition(topicPartition).left.value shouldBe BatchCloudSinkError(
      Set(FatalCloudSinkError("commit failed", topicPartition)),
    )
    verify(mockWriter1).commit
    verify(mockWriter2).commit
  }
}
