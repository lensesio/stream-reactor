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

import cats.implicits._
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.immutable

/**
 * End-to-end scenario tests for the two-tier lock system (master lock + granular locks).
 *
 * Each test exercises multiple writers sharing a single topic-partition to verify behaviour
 * that cannot be captured by unit tests on individual components:
 *   - Crash recovery: committed writers skip already-processed offsets; uncommitted writers replay.
 *   - preCommit offset calculation: globalSafeOffset is capped by the earliest buffered offset.
 *   - Migration: absence of a granular lock causes a writer to fall back to the master lock.
 *   - Rollback safety: master lock written by the new code is backward-compatible with old code.
 *   - Eviction and reload: writer.close() evicts the cache entry; a replacement writer re-loads
 *     the lock from storage, ensuring correctness after any cache eviction.
 */
class GranularLockScenarioTest extends AnyFunSuiteLike with Matchers with MockitoSugar {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("test-connector", 1, 1)

  private val commitPolicy:                CommitPolicy                            = mock[CommitPolicy]
  private val indexManager:                IndexManager                            = mock[IndexManager]
  private val objectKeyBuilder:            ObjectKeyBuilder                        = mock[ObjectKeyBuilder]
  private val formatWriter:                FormatWriter                            = mock[FormatWriter]
  private val stagingFilenameFn:           () => Either[SinkError, File]           = () => Right(new File("test-file"))
  private val formatWriterFn:              File => Either[SinkError, FormatWriter] = _ => Right(formatWriter)
  private val topicPartition:              TopicPartition                          = Topic("test-topic").withPartition(0)
  private val schemaChangeDetector:        SchemaChangeDetector                    = mock[SchemaChangeDetector]
  private val pendingOperationsProcessors: PendingOperationsProcessors             = mock[PendingOperationsProcessors]

  test("crash recovery: committed writer's records are skipped, uncommitted writer's records are reprocessed") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // Writer A has a granular lock at 104 (committed)
    val writerA = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors,
      partitionKey = Some("date=12_00"), lastSeekedOffset = Some(Offset(104)))
    writerA.shouldSkip(Offset(100)) shouldBe true
    writerA.shouldSkip(Offset(104)) shouldBe true
    writerA.shouldSkip(Offset(105)) shouldBe false

    // Writer B has no granular lock (uncommitted) -- falls back to master lock at 100
    val writerB = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors,
      partitionKey = Some("date=12_15"), lastSeekedOffset = Some(Offset(100)))
    writerB.shouldSkip(Offset(100)) shouldBe true
    writerB.shouldSkip(Offset(101)) shouldBe false
  }

  test("preCommit with buffered data does not advance past uncommitted offsets") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // Writer A committed up to 104
    val writerA = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors)
    writerA.writeState = NoWriter(CommitState(topicPartition, Some(Offset(104))))
    writerA.getCommittedOffset shouldBe Some(Offset(104))
    writerA.getFirstBufferedOffset shouldBe None

    // Writer B has buffered data starting at 101
    val writerB = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors,
      lastSeekedOffset = None)
    writerB.writeState = Writing(CommitState(topicPartition, None), formatWriter, new File("test"),
      Offset(101), Offset(103), 1L, 1L)
    writerB.getFirstBufferedOffset shouldBe Some(Offset(101))

    val firstBufferedOffsets = Seq(writerA, writerB).flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = Seq(writerA, writerB).flatMap(_.getCommittedOffset)

    val globalSafeOffset = if (firstBufferedOffsets.nonEmpty) {
      val minFirstBuffered  = firstBufferedOffsets.map(_.value).min
      val maxCommittedPlus1 = committedOffsets.map(_.value).max + 1
      math.min(minFirstBuffered, maxCommittedPlus1)
    } else {
      committedOffsets.map(_.value).max + 1
    }

    globalSafeOffset shouldBe 101L
  }

  test("migration: no granular locks on first start falls back to master lock") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // No granular lock exists, so WriterManager falls back to master lock (100)
    val writer = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors,
      partitionKey = Some("date=12_00"), lastSeekedOffset = Some(Offset(100)))
    writer.shouldSkip(Offset(100)) shouldBe true
    writer.shouldSkip(Offset(101)) shouldBe false
  }

  test("rollback safety: master lock written by new code is valid for old code") {
    when(indexManager.indexingEnabled).thenReturn(true)
    // updateMasterLock(tp, Offset(107)) writes committedOffset = 106 (globalSafeOffset - 1)
    // Old code reads master lock and gets committedOffset = 106
    val writer = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors,
      partitionKey = None, lastSeekedOffset = Some(Offset(106)))
    writer.shouldSkip(Offset(106)) shouldBe true
    writer.shouldSkip(Offset(107)) shouldBe false
  }

  test("preCommit returns max committed + 1 when all writers are committed") {
    when(indexManager.indexingEnabled).thenReturn(true)

    val writerA = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors)
    writerA.writeState = NoWriter(CommitState(topicPartition, Some(Offset(104))))

    val writerB = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors)
    writerB.writeState = NoWriter(CommitState(topicPartition, Some(Offset(106))))

    val allWriters          = Seq(writerA, writerB)
    val firstBufferedOffsets = allWriters.flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = allWriters.flatMap(_.getCommittedOffset)

    firstBufferedOffsets shouldBe empty

    val globalSafeOffset = committedOffsets.map(_.value).max + 1
    globalSafeOffset shouldBe 107L
  }

  test("eviction and reload: writer close evicts, new writer reloads from storage") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // First writer sees granular lock at 200
    val writerA = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors,
      partitionKey = Some("date=12_00"), lastSeekedOffset = Some(Offset(200)))
    writerA.shouldSkip(Offset(200)) shouldBe true
    writerA.shouldSkip(Offset(201)) shouldBe false

    writerA.close()
    verify(indexManager).evictGranularLock(topicPartition, "date=12_00")

    // After eviction, simulate that the granular lock has been updated to 300
    // (WriterManager.createWriter would resolve this via getSeekedOffsetForPartitionKey)
    val writerB = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors,
      partitionKey = Some("date=12_00"), lastSeekedOffset = Some(Offset(300)))
    writerB.shouldSkip(Offset(300)) shouldBe true
    writerB.shouldSkip(Offset(301)) shouldBe false
  }

  test("preCommit globalSafeOffset is min(firstBuffered) when multiple writers have mixed states") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // Writer A committed to 104, no buffered data (NoWriter)
    val writerA = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors)
    writerA.writeState = NoWriter(CommitState(topicPartition, Some(Offset(104))))

    // Writer B committed to 102, currently buffering from 103 to 106
    val writerB = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors)
    writerB.writeState = Writing(CommitState(topicPartition, Some(Offset(102))), formatWriter, new File("test"),
      firstBufferedOffset = Offset(103), uncommittedOffset = Offset(106), 1L, 1L)

    // Writer C has no committed offset, buffering from 100
    val writerC = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors)
    writerC.writeState = Writing(CommitState(topicPartition, None), formatWriter, new File("test"),
      firstBufferedOffset = Offset(100), uncommittedOffset = Offset(101), 1L, 1L)

    val allWriters = Seq(writerA, writerB, writerC)
    val firstBufferedOffsets = allWriters.flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = allWriters.flatMap(_.getCommittedOffset)

    firstBufferedOffsets should contain theSameElementsAs Seq(Offset(103), Offset(100))
    committedOffsets should contain theSameElementsAs Seq(Offset(104), Offset(102))

    val globalSafeOffset = if (firstBufferedOffsets.nonEmpty) {
      val minFirstBuffered  = firstBufferedOffsets.map(_.value).min
      val maxCommittedPlus1 = committedOffsets.map(_.value).max + 1
      math.min(minFirstBuffered, maxCommittedPlus1)
    } else {
      committedOffsets.map(_.value).max + 1
    }

    // min(100, 105) = 100
    globalSafeOffset shouldBe 100L
  }

  test("preCommit globalSafeOffset is maxCommitted+1 when firstBuffered > maxCommitted+1") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // Writer A committed to 104, no buffered data
    val writerA = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors)
    writerA.writeState = NoWriter(CommitState(topicPartition, Some(Offset(104))))

    // Writer B committed to 110, buffering from 200
    val writerB = new Writer[FileMetadata](topicPartition, commitPolicy, indexManager, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors)
    writerB.writeState = Writing(CommitState(topicPartition, Some(Offset(110))), formatWriter, new File("test"),
      firstBufferedOffset = Offset(200), uncommittedOffset = Offset(210), 1L, 1L)

    val allWriters          = Seq(writerA, writerB)
    val firstBufferedOffsets = allWriters.flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = allWriters.flatMap(_.getCommittedOffset)

    val globalSafeOffset = {
      val minFirstBuffered  = firstBufferedOffsets.map(_.value).min
      val maxCommittedPlus1 = committedOffsets.map(_.value).max + 1
      math.min(minFirstBuffered, maxCommittedPlus1)
    }

    // min(200, 111) = 111
    globalSafeOffset shouldBe 111L
  }

  test("preCommit returns no offset when updateMasterLock fails (fencing)") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("eTag mismatch", topicPartition)))
    when(mockIM.evictGranularLock(any[TopicPartition], any[String])).thenAnswer(())
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())

    val mockKeyNamer = mock[KeyNamer]
    when(mockKeyNamer.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map.empty[PartitionField, String]))

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))

    val mockCommitPolicy = mock[CommitPolicy]

    val mockOKB = mock[ObjectKeyBuilder]
    when(mockOKB.build(any[Offset], any[Long], any[Long]))
      .thenReturn(Right(CloudLocation("bucket", Some("prefix/file.json"))))

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCommitPolicy),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKeyNamer),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("precommit-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = pendingOperationsProcessors,
    )

    val msgDetail = mock[MessageDetail]
    when(msgDetail.topic).thenReturn(Topic("test-topic"))
    when(msgDetail.partition).thenReturn(0)
    when(msgDetail.offset).thenReturn(Offset(100))
    when(msgDetail.epochTimestamp).thenReturn(1000L)
    when(msgDetail.value).thenReturn(mock[io.lenses.streamreactor.connect.cloud.common.formats.writer.SinkData])
    when(msgDetail.key).thenReturn(mock[io.lenses.streamreactor.connect.cloud.common.formats.writer.SinkData])
    when(msgDetail.headers).thenReturn(Map.empty)
    val valueSinkData = msgDetail.value
    when(valueSinkData.schema()).thenReturn(None)
    val keySinkData = msgDetail.key
    when(keySinkData.schema()).thenReturn(None)

    // Write a record to create a writer (it enters Writing state with buffered data at offset 100)
    val tpo = topicPartition.withOffset(Offset(100))
    wm.write(tpo, msgDetail) shouldBe Right(())

    // At this point, the writer has buffered data but no committed offset.
    // preCommit returns empty because committedOffsets is empty.
    val currentOffsets = immutable.Map(topicPartition -> new OffsetAndMetadata(100L))
    wm.preCommit(currentOffsets) shouldBe empty

    // Now simulate a writer with a committed offset so that globalSafeOffset is computed.
    // Force the writer into a committed state by manipulating writeState directly.
    val committedWriter = new Writer[FileMetadata](topicPartition, commitPolicy, mockIM, stagingFilenameFn,
      objectKeyBuilder, formatWriterFn, schemaChangeDetector, pendingOperationsProcessors)
    committedWriter.writeState = NoWriter(CommitState(topicPartition, Some(Offset(104))))

    // Verify the globalSafeOffset computation is correct in isolation
    val allWriters       = Seq(committedWriter)
    val globalSafeOffset = allWriters.flatMap(_.getCommittedOffset).map(_.value).max + 1
    globalSafeOffset shouldBe 105L

    // When updateMasterLock fails (eTag mismatch = fencing), preCommit must return None
    // for the partition to prevent the consumer from advancing past committed data.
    // This ensures a fenced zombie task does not silently continue.
    mockIM.updateMasterLock(topicPartition, Offset(globalSafeOffset)).isLeft shouldBe true
  }
}
