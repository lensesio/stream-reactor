/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File

class WriterTest extends AnyFunSuiteLike with Matchers with MockitoSugar {

  private implicit val connectorTaskId:  ConnectorTaskId                         = ConnectorTaskId("test-connector", 1, 1)
  private implicit val storageInterface: StorageInterface[FileMetadata]          = mock[StorageInterface[FileMetadata]]
  private val commitPolicy:              CommitPolicy                            = mock[CommitPolicy]
  private val writerIndexer:             WriterIndexer[FileMetadata]             = mock[WriterIndexer[FileMetadata]]
  private val objectKeyBuilder:          ObjectKeyBuilder                        = mock[ObjectKeyBuilder]
  private val formatWriter:              FormatWriter                            = mock[FormatWriter]
  private val stagingFilenameFn:         () => Either[SinkError, File]           = () => Right(new File("test-file"))
  private val formatWriterFn:            File => Either[SinkError, FormatWriter] = _ => Right(formatWriter)
  private val topicPartition:            TopicPartition                          = Topic("test-topic").withPartition(0)

  test("shouldSkip should return false when indexing is disabled") {
    when(writerIndexer.indexingEnabled()).thenReturn(false)
    when(writerIndexer.getSeekedOffsetForTopicPartition(topicPartition)).thenReturn(None)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
    )
    writer.shouldSkip(Offset(100)) shouldBe false
  }

  test(
    "shouldSkip should return true when current offset is less than or equal to committed offset in NoWriter state",
  ) {
    when(writerIndexer.indexingEnabled()).thenReturn(true)
    when(writerIndexer.getSeekedOffsetForTopicPartition(topicPartition)).thenReturn(Some(Offset(50)))
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
    )
    writer.writeState = NoWriter(CommitState(topicPartition, Some(Offset(100))))
    writer.shouldSkip(Offset(100)) shouldBe true
    writer.shouldSkip(Offset(99)) shouldBe true
  }

  test("shouldSkip should return false when current offset is greater than committed offset in NoWriter state") {
    when(writerIndexer.indexingEnabled()).thenReturn(true)
    when(writerIndexer.getSeekedOffsetForTopicPartition(topicPartition)).thenReturn(Some(Offset(50)))
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
    )
    writer.writeState = NoWriter(CommitState(topicPartition, Some(Offset(100))))
    writer.shouldSkip(Offset(101)) shouldBe false
  }

  test("shouldSkip should return true when current offset is less than or equal to largest offset in Uploading state") {
    when(writerIndexer.indexingEnabled()).thenReturn(true)
    when(writerIndexer.getSeekedOffsetForTopicPartition(topicPartition)).thenReturn(Some(Offset(50)))
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
    )
    writer.writeState = Uploading(
      CommitState(topicPartition, Some(Offset(100))),
      new File("test-file"),
      Offset(150),
      earliestRecordTimestamp = 1L,
      latestRecordTimestamp   = 1L,
    )
    writer.shouldSkip(Offset(150)) shouldBe true
    writer.shouldSkip(Offset(149)) shouldBe true
  }

  test("shouldSkip should return false when current offset is greater than largest offset in Uploading state") {
    when(writerIndexer.indexingEnabled()).thenReturn(true)
    when(writerIndexer.getSeekedOffsetForTopicPartition(topicPartition)).thenReturn(Some(Offset(50)))
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
    )
    writer.writeState =
      Uploading(CommitState(topicPartition, Some(Offset(100))), new File("test-file"), Offset(150), 1L, 1L)
    writer.shouldSkip(Offset(151)) shouldBe false
  }

  test("shouldSkip should return true when current offset is less than or equal to largest offset in Writing state") {
    when(writerIndexer.indexingEnabled()).thenReturn(true)
    when(writerIndexer.getSeekedOffsetForTopicPartition(topicPartition)).thenReturn(Some(Offset(50)))
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
    )
    writer.writeState =
      Writing(CommitState(topicPartition, Some(Offset(100))), formatWriter, new File("test-file"), Offset(150), 1L, 1L)
    writer.shouldSkip(Offset(150)) shouldBe true
    writer.shouldSkip(Offset(149)) shouldBe true
  }

  test("shouldSkip should return false when current offset is greater than largest offset in Writing state") {
    when(writerIndexer.indexingEnabled()).thenReturn(true)
    when(writerIndexer.getSeekedOffsetForTopicPartition(topicPartition)).thenReturn(Some(Offset(50)))
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
    )
    writer.writeState =
      Writing(CommitState(topicPartition, Some(Offset(100))), formatWriter, new File("test-file"), Offset(150), 1L, 1L)
    writer.shouldSkip(Offset(151)) shouldBe false
  }
}
