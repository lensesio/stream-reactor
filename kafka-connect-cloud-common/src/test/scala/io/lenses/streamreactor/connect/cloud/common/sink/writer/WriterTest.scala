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

import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.apache.kafka.connect.data.Schema
import org.mockito.Answers
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File

class WriterTest extends AnyFunSuiteLike with Matchers with MockitoSugar with ArgumentMatchersSugar {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("test-connector", 1, 1)

  private val commitPolicy:                CommitPolicy                            = mock[CommitPolicy]
  private val writerIndexer:               IndexManager                            = mock[IndexManager]
  private val objectKeyBuilder:            ObjectKeyBuilder                        = mock[ObjectKeyBuilder]
  private val formatWriter:                FormatWriter                            = mock[FormatWriter]
  private val stagingFilenameFn:           () => Either[SinkError, File]           = () => Right(new File("test-file"))
  private val formatWriterFn:              File => Either[SinkError, FormatWriter] = _ => Right(formatWriter)
  private val topicPartition:              TopicPartition                          = Topic("test-topic").withPartition(0)
  private val schemaChangeDetector:        SchemaChangeDetector                    = mock[SchemaChangeDetector]
  private val pendingOperationsProcessors: PendingOperationsProcessors             = mock[PendingOperationsProcessors]

  test("shouldSkip should return false when indexing is disabled") {
    when(writerIndexer.indexingEnabled).thenReturn(false)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.shouldSkip(Offset(100)) shouldBe false
  }

  test(
    "shouldSkip should return true when current offset is less than or equal to committed offset in NoWriter state",
  ) {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(100)))))
    writer.shouldSkip(Offset(100)) shouldBe true
    writer.shouldSkip(Offset(99)) shouldBe true
  }

  test("shouldSkip should return false when current offset is greater than committed offset in NoWriter state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(100)))))
    writer.shouldSkip(Offset(101)) shouldBe false
  }

  test("shouldSkip should return true when current offset is less than or equal to largest offset in Uploading state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(Offset(100))),
        new File("test-file"),
        firstBufferedOffset = Offset(100),
        Offset(150),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 1L,
      ),
    )
    writer.shouldSkip(Offset(150)) shouldBe true
    writer.shouldSkip(Offset(149)) shouldBe true
  }

  test("shouldSkip should return false when current offset is greater than largest offset in Uploading state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Uploading(CommitState(topicPartition, Some(Offset(100))), new File("test-file"), Offset(100), Offset(150), 1L, 1L),
    )
    writer.shouldSkip(Offset(151)) shouldBe false
  }

  test("shouldSkip should return true when current offset is less than or equal to largest offset in Writing state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Writing(CommitState(topicPartition, Some(Offset(100))),
              formatWriter,
              new File("test-file"),
              Offset(100),
              Offset(150),
              1L,
              1L,
      ),
    )
    writer.shouldSkip(Offset(150)) shouldBe true
    writer.shouldSkip(Offset(149)) shouldBe true
  }

  test("shouldSkip should return false when current offset is greater than largest offset in Writing state") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Writing(CommitState(topicPartition, Some(Offset(100))),
              formatWriter,
              new File("test-file"),
              Offset(100),
              Offset(150),
              1L,
              1L,
      ),
    )
    writer.shouldSkip(Offset(151)) shouldBe false
  }

  test("shouldRollover returns true when rollover is enabled and schema has changed") {
    val schema = mock[Schema]
    val writer = spy(
      new Writer[FileMetadata](topicPartition,
                               commitPolicy,
                               writerIndexer,
                               stagingFilenameFn,
                               objectKeyBuilder,
                               formatWriterFn,
                               schemaChangeDetector,
                               pendingOperationsProcessors,
      ),
    )
    when(writer.schemaHasChanged(schema)).thenReturn(true)
    when(writer.rolloverOnSchemaChange).thenReturn(true)
    writer.shouldRollover(schema) shouldBe true
  }

  test("schemaHasChanged should return true when schema has changed in Writing state") {
    val schema       = mock[Schema]
    val lastSchema   = mock[Schema]
    val formatWriter = mock[FormatWriter](Answers.RETURNS_DEEP_STUBS)
    val commitState = CommitState(topicPartition, Some(Offset(100)))
      .copy(lastKnownSchema = Some(lastSchema))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    when(schemaChangeDetector.detectSchemaChange(lastSchema, schema)).thenReturn(true)

    writer.schemaHasChanged(schema) shouldBe true
  }

  test("schemaHasChanged should return false when schema has not changed in Writing state") {
    val schema       = mock[Schema]
    val lastSchema   = mock[Schema]
    val formatWriter = mock[FormatWriter](Answers.RETURNS_DEEP_STUBS)
    val commitState = CommitState(topicPartition, Some(Offset(100)))
      .copy(lastKnownSchema = Some(lastSchema))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    when(schemaChangeDetector.detectSchemaChange(lastSchema, schema)).thenReturn(false)

    writer.schemaHasChanged(schema) shouldBe false
  }

  test("schemaHasChanged should return false when not in Writing state") {
    val schema = mock[Schema]
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(100)))))

    writer.schemaHasChanged(schema) shouldBe false
  }

  test("schemaHasChanged should return false when lastKnownSchema is None in Writing state") {
    val schema       = mock[Schema]
    val formatWriter = mock[FormatWriter]
    val commitState  = CommitState(topicPartition, Some(Offset(100)))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    writer.schemaHasChanged(schema) shouldBe false
  }
  test("schemaHasChanged should return false when lastKnownSchema is the same as the new schema in Writing state") {
    val schema       = mock[Schema]
    val formatWriter = mock[FormatWriter]
    val commitState = CommitState(topicPartition, Some(Offset(100)))
      .copy(lastKnownSchema = Some(schema))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    writer.schemaHasChanged(schema) shouldBe false
  }

  test(
    "schemaHasChanged should return false when lastKnownSchema is None and schemaHasChanged returns false in Writing state",
  ) {
    val schema       = mock[Schema]
    val formatWriter = mock[FormatWriter](Answers.RETURNS_DEEP_STUBS)
    val commitState  = CommitState(topicPartition, Some(Offset(100)))
    val writingState = Writing(commitState, formatWriter, new File("test-file"), Offset(100), Offset(150), 1L, 1L)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(writingState)

    when(schemaChangeDetector.detectSchemaChange(null, schema)).thenReturn(false)

    writer.schemaHasChanged(schema) shouldBe false
  }

  test("close should delete staging file when in Writing state") {
    val tmpFile = File.createTempFile("writer-test-writing-", ".tmp")
    tmpFile.exists() shouldBe true

    val formatWriter = mock[FormatWriter]
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Writing(CommitState(topicPartition, Some(Offset(100))), formatWriter, tmpFile, Offset(100), Offset(150), 1L, 1L),
    )

    writer.close()

    tmpFile.exists() shouldBe false
    writer.currentWriteState shouldBe a[NoWriter]
  }

  test("close should delete staging file when in Uploading state") {
    val tmpFile = File.createTempFile("writer-test-uploading-", ".tmp")
    tmpFile.exists() shouldBe true

    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(
      Uploading(CommitState(topicPartition, Some(Offset(100))), tmpFile, Offset(100), Offset(150), 1L, 1L),
    )

    writer.close()

    tmpFile.exists() shouldBe false
    writer.currentWriteState shouldBe a[NoWriter]
  }

  test("close should be a no-op when in NoWriter state") {
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(100)))))

    writer.close()

    writer.currentWriteState shouldBe a[NoWriter]
  }

  // Phase 1a: WriteState tests - firstBufferedOffset

  test("toWriting should set firstBufferedOffset to the first record's offset") {
    val noWriter = NoWriter(CommitState(topicPartition, None))
    val fw       = mock[FormatWriter]
    val result   = noWriter.toWriting(fw, new File("test"), Offset(42), 1000L)
    result.firstBufferedOffset shouldBe Offset(42)
  }

  test("Writing.update should preserve firstBufferedOffset when uncommittedOffset advances") {
    val fw = mock[FormatWriter]
    when(fw.getPointer).thenReturn(100L)
    val writing = Writing(CommitState(topicPartition, None), fw, new File("test"), Offset(42), Offset(42), 1L, 1L)
    val updated = writing.update(Offset(99), 2L, None).asInstanceOf[Writing]
    updated.firstBufferedOffset shouldBe Offset(42)
    updated.uncommittedOffset shouldBe Offset(99)
  }

  test("toUploading should carry firstBufferedOffset from Writing") {
    val fw        = mock[FormatWriter]
    val writing   = Writing(CommitState(topicPartition, None), fw, new File("test"), Offset(42), Offset(99), 1L, 2L)
    val uploading = writing.toUploading
    uploading.firstBufferedOffset shouldBe Offset(42)
  }

  test("getFirstBufferedOffset returns Some in Writing state") {
    val fw = mock[FormatWriter]
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(Writing(CommitState(topicPartition, None),
                                   fw,
                                   new File("test"),
                                   Offset(42),
                                   Offset(50),
                                   1L,
                                   1L,
    ))
    writer.getFirstBufferedOffset shouldBe Some(Offset(42))
  }

  test("getFirstBufferedOffset returns Some in Uploading state") {
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.forceWriteState(Uploading(CommitState(topicPartition, None),
                                     new File("test"),
                                     Offset(42),
                                     Offset(50),
                                     1L,
                                     1L,
    ))
    writer.getFirstBufferedOffset shouldBe Some(Offset(42))
  }

  test("getFirstBufferedOffset returns None in NoWriter state") {
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
    )
    writer.getFirstBufferedOffset shouldBe None
  }

  // Phase 1b: Writer tests - per-writer seeked offset and granular lock routing

  test("Writer with partitionKey should use granular lock seeked offset") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = Some("date=12_00"),
                                          lastSeekedOffset = Some(Offset(200)),
    )
    writer.shouldSkip(Offset(200)) shouldBe true
  }

  test("Writer with partitionKey=None should fallback to master lock seeked offset") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = None,
                                          lastSeekedOffset = Some(Offset(100)),
    )
    writer.shouldSkip(Offset(100)) shouldBe true
  }

  test("Writer with partitionKey should not skip offsets above granular lock") {
    when(writerIndexer.indexingEnabled).thenReturn(true)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = Some("date=12_00"),
                                          lastSeekedOffset = Some(Offset(200)),
    )
    writer.shouldSkip(Offset(201)) shouldBe false
  }

  test("close should not evict granular lock even when partitionKey is defined") {
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          writerIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = Some("date=12_00"),
                                          lastSeekedOffset = Some(Offset(200)),
    )
    writer.close()
    verify(writerIndexer, never).evictGranularLock(any[TopicPartition], any[String])
  }

  test("close should not evict granular lock when partitionKey is None") {
    val freshIndexer = mock[IndexManager]
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          freshIndexer,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey = None,
    )
    writer.close()
    verify(freshIndexer, never).evictGranularLock(any[TopicPartition], any[String])
  }
}
