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
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
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
 *   - Eviction and reload: writer.close() does NOT evict the cache entry (left for GC);
 *     a replacement writer re-loads the lock from storage or uses the cached entry.
 */
class GranularLockScenarioTest extends AnyFunSuiteLike with Matchers with MockitoSugar with EitherValues {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("test-connector", 1, 1)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => cats.data.Validated.fromEither(Right(location))

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
    val writerA = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
                                           partitionKey     = Some("date=12_00"),
                                           lastSeekedOffset = Some(Offset(104)),
    )
    writerA.shouldSkip(Offset(100)) shouldBe true
    writerA.shouldSkip(Offset(104)) shouldBe true
    writerA.shouldSkip(Offset(105)) shouldBe false

    // Writer B has no granular lock (uncommitted) -- falls back to master lock at 100
    val writerB = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
                                           partitionKey     = Some("date=12_15"),
                                           lastSeekedOffset = Some(Offset(100)),
    )
    writerB.shouldSkip(Offset(100)) shouldBe true
    writerB.shouldSkip(Offset(101)) shouldBe false
  }

  test("preCommit with buffered data does not advance past uncommitted offsets") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // Writer A committed up to 104
    val writerA = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerA.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(104)))))
    writerA.getCommittedOffset shouldBe Some(Offset(104))
    writerA.getFirstBufferedOffset shouldBe None

    // Writer B has buffered data starting at 101
    val writerB = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
                                           lastSeekedOffset = None,
    )
    writerB.forceWriteState(
      Writing(CommitState(topicPartition, None), formatWriter, new File("test"), Offset(101), Offset(103), 1L, 1L),
    )
    writerB.getFirstBufferedOffset shouldBe Some(Offset(101))

    val firstBufferedOffsets = Seq(writerA, writerB).flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = Seq(writerA, writerB).flatMap(_.getCommittedOffset)

    val globalSafeOffset =
      if (firstBufferedOffsets.nonEmpty) {
        firstBufferedOffsets.map(_.value).min
      } else {
        committedOffsets.map(_.value).max + 1
      }

    globalSafeOffset shouldBe 101L
  }

  test("migration: no granular locks on first start falls back to master lock") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // No granular lock exists, so WriterManager falls back to master lock (100)
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          indexManager,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = Some("date=12_00"),
                                          lastSeekedOffset = Some(Offset(100)),
    )
    writer.shouldSkip(Offset(100)) shouldBe true
    writer.shouldSkip(Offset(101)) shouldBe false
  }

  test("rollback safety: master lock written by new code is valid for old code") {
    when(indexManager.indexingEnabled).thenReturn(true)
    // updateMasterLock(tp, Offset(107)) writes committedOffset = 106 (globalSafeOffset - 1)
    // Old code reads master lock and gets committedOffset = 106
    val writer = new Writer[FileMetadata](topicPartition,
                                          commitPolicy,
                                          indexManager,
                                          stagingFilenameFn,
                                          objectKeyBuilder,
                                          formatWriterFn,
                                          schemaChangeDetector,
                                          pendingOperationsProcessors,
                                          partitionKey     = None,
                                          lastSeekedOffset = Some(Offset(106)),
    )
    writer.shouldSkip(Offset(106)) shouldBe true
    writer.shouldSkip(Offset(107)) shouldBe false
  }

  test("preCommit returns max committed + 1 when all writers are committed") {
    when(indexManager.indexingEnabled).thenReturn(true)

    val writerA = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerA.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(104)))))

    val writerB = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerB.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(106)))))

    val allWriters           = Seq(writerA, writerB)
    val firstBufferedOffsets = allWriters.flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = allWriters.flatMap(_.getCommittedOffset)

    firstBufferedOffsets shouldBe empty

    val globalSafeOffset = committedOffsets.map(_.value).max + 1
    globalSafeOffset shouldBe 107L
  }

  test("eviction and reload: writer close does not evict cache, new writer reloads from storage") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // First writer sees granular lock at 200
    val writerA = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
                                           partitionKey     = Some("date=12_00"),
                                           lastSeekedOffset = Some(Offset(200)),
    )
    writerA.shouldSkip(Offset(200)) shouldBe true
    writerA.shouldSkip(Offset(201)) shouldBe false

    writerA.close()
    verify(indexManager, never).evictGranularLock(topicPartition, "date=12_00")

    // After close, simulate that the granular lock has been updated to 300
    // (WriterManager.createWriter would resolve this via getSeekedOffsetForPartitionKey)
    val writerB = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
                                           partitionKey     = Some("date=12_00"),
                                           lastSeekedOffset = Some(Offset(300)),
    )
    writerB.shouldSkip(Offset(300)) shouldBe true
    writerB.shouldSkip(Offset(301)) shouldBe false
  }

  test("preCommit globalSafeOffset is min(firstBuffered) when multiple writers have mixed states") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // Writer A committed to 104, no buffered data (NoWriter)
    val writerA = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerA.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(104)))))

    // Writer B committed to 102, currently buffering from 103 to 106
    val writerB = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerB.forceWriteState(
      Writing(
        CommitState(topicPartition, Some(Offset(102))),
        formatWriter,
        new File("test"),
        firstBufferedOffset = Offset(103),
        uncommittedOffset   = Offset(106),
        1L,
        1L,
      ),
    )

    // Writer C has no committed offset, buffering from 100
    val writerC = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerC.forceWriteState(
      Writing(
        CommitState(topicPartition, None),
        formatWriter,
        new File("test"),
        firstBufferedOffset = Offset(100),
        uncommittedOffset   = Offset(101),
        1L,
        1L,
      ),
    )

    val allWriters           = Seq(writerA, writerB, writerC)
    val firstBufferedOffsets = allWriters.flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = allWriters.flatMap(_.getCommittedOffset)

    firstBufferedOffsets should contain theSameElementsAs Seq(Offset(103), Offset(100))
    committedOffsets should contain theSameElementsAs Seq(Offset(104), Offset(102))

    val globalSafeOffset =
      if (firstBufferedOffsets.nonEmpty) {
        firstBufferedOffsets.map(_.value).min
      } else {
        committedOffsets.map(_.value).max + 1
      }

    // minFirstBuffered = 100
    globalSafeOffset shouldBe 100L
  }

  test("preCommit globalSafeOffset accounts for writer in Uploading state (buffered data must not be skipped)") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // Writer A committed to 104, no buffered data (NoWriter)
    val writerA = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerA.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(104)))))

    // Writer B is in Uploading state: committed to 90, buffered from 91 to 95
    val writerB = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerB.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(Offset(90))),
        new File("test"),
        firstBufferedOffset = Offset(91),
        uncommittedOffset   = Offset(95),
        1L,
        1L,
        1L,
      ),
    )

    val allWriters           = Seq(writerA, writerB)
    val firstBufferedOffsets = allWriters.flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = allWriters.flatMap(_.getCommittedOffset)

    firstBufferedOffsets should contain theSameElementsAs Seq(Offset(91))
    committedOffsets should contain theSameElementsAs Seq(Offset(104), Offset(90))

    val globalSafeOffset =
      if (firstBufferedOffsets.nonEmpty) {
        firstBufferedOffsets.map(_.value).min
      } else {
        committedOffsets.map(_.value).max + 1
      }

    // minFirstBuffered = 91 -- must not advance past the Uploading writer's buffered data
    globalSafeOffset shouldBe 91L
  }

  test("preCommit globalSafeOffset is minFirstBuffered even when it exceeds maxCommitted+1") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // Writer A committed to 104, no buffered data
    val writerA = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerA.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(104)))))

    // Writer B committed to 110, buffering from 200
    val writerB = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
    )
    writerB.forceWriteState(
      Writing(
        CommitState(topicPartition, Some(Offset(110))),
        formatWriter,
        new File("test"),
        firstBufferedOffset = Offset(200),
        uncommittedOffset   = Offset(210),
        1L,
        1L,
      ),
    )

    val allWriters           = Seq(writerA, writerB)
    val firstBufferedOffsets = allWriters.flatMap(_.getFirstBufferedOffset)

    val globalSafeOffset = firstBufferedOffsets.map(_.value).min

    // minFirstBuffered = 200
    globalSafeOffset shouldBe 200L
  }

  test("preCommit returns no offset when updateMasterLock fails (fencing)") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())

    // Stub updateMasterLock with a specific Offset to avoid AnyVal/null issue with Mockito
    when(mockIM.updateMasterLock(topicPartition, Offset(105)))
      .thenReturn(Left(FatalCloudSinkError("eTag mismatch", topicPartition)))

    val mockKeyNamer = mock[KeyNamer]
    when(mockKeyNamer.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map.empty[PartitionField, String]))

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))

    val mockCommitPolicy = mock[CommitPolicy]

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCommitPolicy),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKeyNamer),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("precommit-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => objectKeyBuilder,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = pendingOperationsProcessors,
    )

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    val msgDetail = mock[MessageDetail]
    when(msgDetail.topic).thenReturn(Topic("test-topic"))
    when(msgDetail.partition).thenReturn(0)
    when(msgDetail.offset).thenReturn(Offset(100))
    when(msgDetail.epochTimestamp).thenReturn(1000L)
    when(msgDetail.value).thenReturn(mockValue)
    when(msgDetail.key).thenReturn(mockKeySD)
    when(msgDetail.headers).thenReturn(Map.empty)

    val tpo = topicPartition.withOffset(Offset(100))
    wm.write(tpo, msgDetail) shouldBe Right(())

    val currentOffsets = immutable.Map(topicPartition -> new OffsetAndMetadata(100L))
    wm.preCommit(currentOffsets) shouldBe empty

    val committedWriter = new Writer[FileMetadata](topicPartition,
                                                   commitPolicy,
                                                   mockIM,
                                                   stagingFilenameFn,
                                                   objectKeyBuilder,
                                                   formatWriterFn,
                                                   schemaChangeDetector,
                                                   pendingOperationsProcessors,
    )
    committedWriter.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(104)))))

    val allWriters       = Seq(committedWriter)
    val globalSafeOffset = allWriters.flatMap(_.getCommittedOffset).map(_.value).max + 1
    globalSafeOffset shouldBe 105L

    mockIM.updateMasterLock(topicPartition, Offset(globalSafeOffset)).isLeft shouldBe true
  }

  test("isIdle returns true for NoWriter, false for Writing and Uploading") {
    when(indexManager.indexingEnabled).thenReturn(true)

    val writerIdle = new Writer[FileMetadata](topicPartition,
                                              commitPolicy,
                                              indexManager,
                                              stagingFilenameFn,
                                              objectKeyBuilder,
                                              formatWriterFn,
                                              schemaChangeDetector,
                                              pendingOperationsProcessors,
                                              partitionKey     = Some("date=12_00"),
                                              lastSeekedOffset = Some(Offset(104)),
    )
    writerIdle.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(104)))))
    writerIdle.isIdle shouldBe true

    val writerWriting = new Writer[FileMetadata](topicPartition,
                                                 commitPolicy,
                                                 indexManager,
                                                 stagingFilenameFn,
                                                 objectKeyBuilder,
                                                 formatWriterFn,
                                                 schemaChangeDetector,
                                                 pendingOperationsProcessors,
                                                 partitionKey     = Some("date=12_15"),
                                                 lastSeekedOffset = Some(Offset(100)),
    )
    writerWriting.forceWriteState(
      Writing(
        CommitState(topicPartition, Some(Offset(100))),
        formatWriter,
        new File("test"),
        firstBufferedOffset = Offset(101),
        uncommittedOffset   = Offset(103),
        1L,
        1L,
      ),
    )
    writerWriting.isIdle shouldBe false

    val writerUploading = new Writer[FileMetadata](topicPartition,
                                                   commitPolicy,
                                                   indexManager,
                                                   stagingFilenameFn,
                                                   objectKeyBuilder,
                                                   formatWriterFn,
                                                   schemaChangeDetector,
                                                   pendingOperationsProcessors,
                                                   partitionKey     = Some("date=12_30"),
                                                   lastSeekedOffset = Some(Offset(100)),
    )
    writerUploading.forceWriteState(
      Uploading(
        CommitState(topicPartition, Some(Offset(100))),
        new File("test"),
        firstBufferedOffset = Offset(101),
        uncommittedOffset   = Offset(103),
        1L,
        1L,
        1L,
      ),
    )
    writerUploading.isIdle shouldBe false
  }

  test("activePartitionKeys includes all writers in the map (idle and active) to protect lock files from GC") {
    val pf = PartitionField(Seq("_value")).value

    val writerIdle = new Writer[FileMetadata](topicPartition,
                                              commitPolicy,
                                              indexManager,
                                              stagingFilenameFn,
                                              objectKeyBuilder,
                                              formatWriterFn,
                                              schemaChangeDetector,
                                              pendingOperationsProcessors,
    )
    writerIdle.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(104)))))

    val writerActive = new Writer[FileMetadata](topicPartition,
                                                commitPolicy,
                                                indexManager,
                                                stagingFilenameFn,
                                                objectKeyBuilder,
                                                formatWriterFn,
                                                schemaChangeDetector,
                                                pendingOperationsProcessors,
    )
    writerActive.forceWriteState(
      Writing(
        CommitState(topicPartition, Some(Offset(100))),
        formatWriter,
        new File("test"),
        firstBufferedOffset = Offset(101),
        uncommittedOffset   = Offset(103),
        1L,
        1L,
      ),
    )

    val writers: Map[MapKey, Writer[FileMetadata]] = Map(
      MapKey(topicPartition, immutable.Map(pf -> "idle_val")) -> writerIdle,
      MapKey(topicPartition, immutable.Map(pf -> "active_val")) -> writerActive,
    )

    val activePartitionKeys: Set[String] = writers
      .filter { case (key, _) => key.topicPartition == topicPartition }
      .keys
      .flatMap(key => WriterManager.derivePartitionKey(key.partitionValues))
      .toSet

    val idleKey   = WriterManager.derivePartitionKey(immutable.Map(pf -> "idle_val")).get
    val activeKey = WriterManager.derivePartitionKey(immutable.Map(pf -> "active_val")).get

    activePartitionKeys should contain(idleKey)
    activePartitionKeys should contain(activeKey)
  }

  test("globalSafeOffset does not regress after idle writer with highest committed offset is evicted") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())

    // Stub updateMasterLock for both expected offset values
    when(mockIM.updateMasterLock(topicPartition, Offset(501))).thenReturn(Right(()))
    when(mockIM.cleanUpObsoleteLocks(topicPartition, Offset(501), Set.empty[String])).thenReturn(Right(()))

    val mockKN = mock[KeyNamer]
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map.empty[PartitionField, String]))

    val mockFW2 = mock[FormatWriter]
    when(mockFW2.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW2.getPointer).thenReturn(100L)
    when(mockFW2.rolloverFileOnSchemaChange()).thenReturn(false)

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    def makeMockMsg(offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(commitPolicy),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("monotonic-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => objectKeyBuilder,
      formatWriterFn              = (_, _) => Right(mockFW2),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = pendingOperationsProcessors,
    )

    // Write a record so that a writer is created (enters Writing state at offset 100)
    wm.write(topicPartition.withOffset(Offset(100)), makeMockMsg(100L)) shouldBe Right(())
    wm.writerCount shouldBe 1

    // Demonstrate the high watermark arithmetic protects against regression:
    // Before eviction: two idle writers at offsets 500 and 200
    val writerHigh = new Writer[FileMetadata](topicPartition,
                                              commitPolicy,
                                              mockIM,
                                              stagingFilenameFn,
                                              objectKeyBuilder,
                                              formatWriterFn,
                                              schemaChangeDetector,
                                              pendingOperationsProcessors,
    )
    writerHigh.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(500)))))

    val writerLow = new Writer[FileMetadata](topicPartition,
                                             commitPolicy,
                                             mockIM,
                                             stagingFilenameFn,
                                             objectKeyBuilder,
                                             formatWriterFn,
                                             schemaChangeDetector,
                                             pendingOperationsProcessors,
    )
    writerLow.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(200)))))

    // Before eviction: globalSafeOffset = max(committed) + 1 = 501
    val beforeEviction = Seq(writerHigh, writerLow).flatMap(_.getCommittedOffset).map(_.value).max + 1
    beforeEviction shouldBe 501L

    // After eviction of writerHigh, naively recalculated offset would be 201
    val afterEviction = Seq(writerLow).flatMap(_.getCommittedOffset).map(_.value).max + 1
    afterEviction shouldBe 201L

    // The high watermark guard ensures: max(201, 501) = 501 (no regression)
    val highWatermark   = beforeEviction
    val correctedOffset = math.max(afterEviction, highWatermark)
    correctedOffset shouldBe 501L
  }

  test("preCommit globalSafeOffset does not regress after idle writer eviction (WriterManager integration)") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())
    when(
      mockIM.cleanUpObsoleteLocks(any[TopicPartition], Offset(ArgumentMatchers.anyLong()), any[Set[String]]),
    ).thenReturn(Right(()))
    when(mockIM.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, _: String, co: Option[Offset], _: Option[PendingState]) => Right(co))

    var masterLockOffsets = List.empty[Long]
    when(mockIM.updateMasterLock(any[TopicPartition], Offset(ArgumentMatchers.anyLong()))).thenAnswer {
      (_: TopicPartition, offset: Offset) =>
        masterLockOffsets = masterLockOffsets :+ offset.value
        Right(())
    }

    val pf = PartitionField(Seq("_value")).value

    val mockKN    = mock[KeyNamer]
    var callCount = 0
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenAnswer { (_: MessageDetail, _: TopicPartition) =>
        callCount += 1
        Right(immutable.Map(pf -> s"key_$callCount"))
      }

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))
    when(mockFW.close()).thenAnswer(())

    val mockCP = mock[CommitPolicy]
    when(mockCP.shouldFlush(any[CommitContext])).thenReturn(true)

    val mockOKB = mock[ObjectKeyBuilder]
    when(
      mockOKB.build(
        Offset(ArgumentMatchers.anyLong()),
        Offset(ArgumentMatchers.anyLong()),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
      ),
    )
      .thenReturn(Right(CloudLocation("bucket", Some("prefix"), Some("prefix/file.parquet"))))

    val mockPOP = mock[PendingOperationsProcessors]
    type IndexUpdateFn = (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]
    when(
      mockPOP.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[IndexUpdateFn],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    )
      .thenAnswer {
        (
          tp: TopicPartition,
          _:  Option[Offset],
          ps: PendingState,
          fn: IndexUpdateFn,
          _:  Boolean,
          _:  Option[String],
          _:  Option[java.io.File],
        ) =>
          fn(tp, Some(ps.pendingOffset), None)
      }

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    def makeMockMsg(offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCP),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("hwm-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = mockPOP,
    )

    // Each write creates a new writer (unique partition key due to incrementing callCount),
    // and shouldFlush=true causes an immediate commit, so each writer transitions to NoWriter.
    // Idle writers are eagerly evicted when the next one is created.
    wm.write(topicPartition.withOffset(Offset(500)), makeMockMsg(500L)) shouldBe Right(())

    // preCommit: globalSafeOffset = 501, master lock = 501
    val offsets1 = wm.preCommit(immutable.Map(topicPartition -> new OffsetAndMetadata(500L)))
    offsets1(topicPartition).offset() shouldBe 501L
    masterLockOffsets.last shouldBe 501L

    // Second write: new partition key, triggers eviction of the writer with committed=500
    wm.write(topicPartition.withOffset(Offset(200)), makeMockMsg(200L)) shouldBe Right(())

    // preCommit: remaining writer has committed=200, so calculated = 201.
    // But the high-watermark guard must keep it at 501.
    val offsets2 = wm.preCommit(immutable.Map(topicPartition -> new OffsetAndMetadata(501L)))
    offsets2(topicPartition).offset() shouldBe 501L
    masterLockOffsets.last shouldBe 501L
  }

  test("high-watermark survives cleanUp via lastReturnedSafeOffset to prevent post-rollback regression") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())
    when(
      mockIM.cleanUpObsoleteLocks(any[TopicPartition], Offset(ArgumentMatchers.anyLong()), any[Set[String]]),
    ).thenReturn(Right(()))
    when(mockIM.updateMasterLock(any[TopicPartition], Offset(ArgumentMatchers.anyLong()))).thenReturn(Right(()))
    when(mockIM.update(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, co: Option[Offset], _: Option[PendingState]) => Right(co))

    val mockKN = mock[KeyNamer]
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map.empty[PartitionField, String]))

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))
    when(mockFW.close()).thenAnswer(())

    val mockCP = mock[CommitPolicy]
    when(mockCP.shouldFlush(any[CommitContext])).thenReturn(true)

    val mockOKB = mock[ObjectKeyBuilder]
    when(
      mockOKB.build(
        Offset(ArgumentMatchers.anyLong()),
        Offset(ArgumentMatchers.anyLong()),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
      ),
    )
      .thenReturn(Right(CloudLocation("bucket", Some("prefix"), Some("prefix/file.parquet"))))

    type IndexUpdateFn = (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]
    val mockPOP = mock[PendingOperationsProcessors]
    when(
      mockPOP.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[IndexUpdateFn],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    )
      .thenAnswer {
        (
          tp: TopicPartition,
          _:  Option[Offset],
          ps: PendingState,
          fn: IndexUpdateFn,
          _:  Boolean,
          _:  Option[String],
          _:  Option[java.io.File],
        ) =>
          fn(tp, Some(ps.pendingOffset), None)
      }

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    def makeMockMsg(offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCP),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("cleanup-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = mockPOP,
    )

    // Establish a high watermark at 501
    wm.write(topicPartition.withOffset(Offset(500)), makeMockMsg(500L)) shouldBe Right(())
    val offsets1 = wm.preCommit(immutable.Map(topicPartition -> new OffsetAndMetadata(500L)))
    offsets1(topicPartition).offset() shouldBe 501L

    // In-place rollback (NOT rebalance — rebalance goes through close()/open()).
    // cleanUp clears HWM and `lastWrittenMasterSafeOffset(tp)` but PRESERVES
    // `lastReturnedSafeOffset(tp)` so the next preCommit cannot return less than the 501
    // already reported to Kafka — cleanUp must preserve lastReturnedSafeOffset.
    wm.cleanUp(topicPartition)

    // Same task instance retries with a fresh writer at a much lower committed offset.
    // Without the lastReturnedSafeOffset preservation, this would return 51 and (after the
    // forced write at that lower value) drive cleanUpObsoleteLocks past still-needed
    // granular locks — silently regressing monotonicity and breaking dedup.
    wm.write(topicPartition.withOffset(Offset(50)), makeMockMsg(50L)) shouldBe Right(())
    val offsets2 = wm.preCommit(immutable.Map(topicPartition -> new OffsetAndMetadata(50L)))

    // Floor held at 501; WriteForced(PostCleanUp) catches the durable master lock up to
    // the previously returned value.
    offsets2(topicPartition).offset() shouldBe 501L
  }

  test("evictIdleWritersIfNeeded does not evict active (non-NoWriter) writers") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())

    val mockKN = mock[KeyNamer]
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map.empty[PartitionField, String]))

    val mockFW2 = mock[FormatWriter]
    when(mockFW2.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW2.getPointer).thenReturn(100L)
    when(mockFW2.rolloverFileOnSchemaChange()).thenReturn(false)

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    def makeMockMsg(partition: Int, offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(partition)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(commitPolicy),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("eviction-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => objectKeyBuilder,
      formatWriterFn              = (_, _) => Right(mockFW2),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = pendingOperationsProcessors,
    )

    val tp2 = Topic("test-topic").withPartition(1)

    // Write a record to partition 0 — creates writer in Writing state
    wm.write(topicPartition.withOffset(Offset(100)), makeMockMsg(0, 100L)) shouldBe Right(())
    wm.writerCount shouldBe 1

    // Write a record to partition 1 — creates a second writer, also in Writing state
    wm.write(tp2.withOffset(Offset(200)), makeMockMsg(1, 200L)) shouldBe Right(())

    // Both writers are in Writing state — active writers are never evicted
    wm.writerCount shouldBe 2
  }

  test("preCommit protects idle writers still in map from GC (activePartitionKeys includes all writers)") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())
    when(mockIM.updateMasterLock(any[TopicPartition], Offset(ArgumentMatchers.anyLong()))).thenReturn(Right(()))
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(mockIM.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, _: String, co: Option[Offset], _: Option[PendingState]) => Right(co))

    var capturedActiveKeys: Set[String] = Set.empty
    when(
      mockIM.cleanUpObsoleteLocks(any[TopicPartition], Offset(ArgumentMatchers.anyLong()), any[Set[String]]),
    ).thenAnswer {
      (_: TopicPartition, _: Offset, activeKeys: Set[String]) =>
        capturedActiveKeys = activeKeys
        Right(())
    }

    val pf = PartitionField(Seq("_value")).value

    val mockKN       = mock[KeyNamer]
    var keyCallCount = 0
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenAnswer { (_: MessageDetail, _: TopicPartition) =>
        keyCallCount += 1
        Right(immutable.Map(pf -> s"val_$keyCallCount"))
      }

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))
    when(mockFW.close()).thenAnswer(())

    val mockCP = mock[CommitPolicy]
    when(mockCP.shouldFlush(any[CommitContext])).thenReturn(true)

    val mockOKB = mock[ObjectKeyBuilder]
    when(
      mockOKB.build(
        Offset(ArgumentMatchers.anyLong()),
        Offset(ArgumentMatchers.anyLong()),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
      ),
    )
      .thenReturn(Right(CloudLocation("bucket", Some("prefix"), Some("prefix/file.parquet"))))

    type IndexUpdateFn = (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]
    val mockPOP = mock[PendingOperationsProcessors]
    when(
      mockPOP.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[IndexUpdateFn],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    )
      .thenAnswer {
        (
          tp: TopicPartition,
          _:  Option[Offset],
          ps: PendingState,
          fn: IndexUpdateFn,
          _:  Boolean,
          _:  Option[String],
          _:  Option[java.io.File],
        ) =>
          fn(tp, Some(ps.pendingOffset), None)
      }

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    def makeMockMsg(offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCP),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("gc-idle-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = mockPOP,
    )

    // Write and commit (shouldFlush=true) a record with partition key val_1 -> transitions to NoWriter (idle)
    wm.write(topicPartition.withOffset(Offset(100)), makeMockMsg(100L)) shouldBe Right(())

    // Writer is now idle (NoWriter state) but still in the writers map
    wm.writerCount shouldBe 1

    // preCommit should pass the idle writer's partition key in activePartitionKeys to cleanUpObsoleteLocks
    wm.preCommit(immutable.Map(topicPartition -> new OffsetAndMetadata(100L)))

    val idleKey = WriterManager.derivePartitionKey(immutable.Map(pf -> "val_1")).get
    capturedActiveKeys should contain(idleKey)
  }

  test("eager eviction keeps only the newest writer after multiple writes with unique partition keys") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())
    when(mockIM.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, _: String, co: Option[Offset], _: Option[PendingState]) => Right(co))

    val mockKN       = mock[KeyNamer]
    var keyCallCount = 0
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenAnswer { (_: MessageDetail, _: TopicPartition) =>
        keyCallCount += 1
        Right(immutable.Map.empty[PartitionField, String].updated(
          PartitionField(Seq("_value")).value,
          s"key_$keyCallCount",
        ))
      }

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))
    when(mockFW.close()).thenAnswer(())

    val mockCP = mock[CommitPolicy]
    when(mockCP.shouldFlush(any[CommitContext])).thenReturn(true)

    val mockOKB = mock[ObjectKeyBuilder]
    when(
      mockOKB.build(
        Offset(ArgumentMatchers.anyLong()),
        Offset(ArgumentMatchers.anyLong()),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
      ),
    )
      .thenReturn(Right(CloudLocation("bucket", Some("prefix"), Some("prefix/file.parquet"))))

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    type IndexUpdateFn = (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]
    val mockPOP = mock[PendingOperationsProcessors]
    when(
      mockPOP.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[IndexUpdateFn],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    )
      .thenAnswer {
        (
          tp: TopicPartition,
          _:  Option[Offset],
          ps: PendingState,
          fn: IndexUpdateFn,
          _:  Boolean,
          _:  Option[String],
          _:  Option[java.io.File],
        ) =>
          fn(tp, Some(ps.pendingOffset), None)
      }

    def makeMockMsg(offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCP),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("bound-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = mockPOP,
    )

    // Write 3 records each with a unique partition key; shouldFlush=true causes immediate commit -> NoWriter.
    // Each new writer creation eagerly evicts all idle writers, so only the newest remains.
    (1 to 3).foreach { i =>
      wm.write(topicPartition.withOffset(Offset(i.toLong)), makeMockMsg(i.toLong)) shouldBe Right(())
    }

    // Only the most recently created writer (key_3) remains; key_1 and key_2 were eagerly evicted.
    wm.writerCount shouldBe 1
  }

  test("HWM initialization from master lock prevents master lock regression across restarts") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(Some(Offset(1000)))
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())
    when(
      mockIM.cleanUpObsoleteLocks(any[TopicPartition], Offset(ArgumentMatchers.anyLong()), any[Set[String]]),
    ).thenReturn(Right(()))
    when(mockIM.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, _: String, co: Option[Offset], _: Option[PendingState]) => Right(co))

    var masterLockOffsets = List.empty[Long]
    when(mockIM.updateMasterLock(any[TopicPartition], Offset(ArgumentMatchers.anyLong()))).thenAnswer {
      (_: TopicPartition, offset: Offset) =>
        masterLockOffsets = masterLockOffsets :+ offset.value
        Right(())
    }

    val pf = PartitionField(Seq("_value")).value

    val mockKN = mock[KeyNamer]
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map(pf -> "new_key")))

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))
    when(mockFW.close()).thenAnswer(())

    val mockCP = mock[CommitPolicy]
    when(mockCP.shouldFlush(any[CommitContext])).thenReturn(true)

    val mockOKB = mock[ObjectKeyBuilder]
    when(
      mockOKB.build(
        Offset(ArgumentMatchers.anyLong()),
        Offset(ArgumentMatchers.anyLong()),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
      ),
    )
      .thenReturn(Right(CloudLocation("bucket", Some("prefix"), Some("prefix/file.parquet"))))

    type IndexUpdateFn = (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]
    val mockPOP = mock[PendingOperationsProcessors]
    when(
      mockPOP.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[IndexUpdateFn],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    )
      .thenAnswer {
        (
          tp: TopicPartition,
          _:  Option[Offset],
          ps: PendingState,
          fn: IndexUpdateFn,
          _:  Boolean,
          _:  Option[String],
          _:  Option[java.io.File],
        ) =>
          fn(tp, Some(ps.pendingOffset), None)
      }

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    def makeMockMsg(offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCP),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("hwm-init-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = mockPOP,
    )

    // Master lock was at offset 1000. A new partition key arrives with committed offset 500.
    // Without HWM init from master lock, globalSafeOffset would regress to 501.
    wm.write(topicPartition.withOffset(Offset(500)), makeMockMsg(500L)) shouldBe Right(())

    val offsets = wm.preCommit(immutable.Map(topicPartition -> new OffsetAndMetadata(500L)))

    // globalSafeOffset must be at least 1001 (master lock committedOffset 1000 + 1).
    // Under the dirty-flag gate, lastWrittenMasterSafeOffset(tp) also lazy-seeds to
    // 1001 on this first cycle, so the cycle is classified `Skip` (no advance to persist).
    // The point of the test — that the safe offset reported to Kafka does NOT regress to
    // 501 — still holds; the master-lock write itself is correctly suppressed because
    // the durable floor is already at the required value.
    offsets(topicPartition).offset() shouldBe 1001L
    masterLockOffsets shouldBe empty
  }

  test("master lock fallback: writer created after granular lock GC'd uses master lock for dedup") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(true)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(Some(Offset(200)))
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())
    when(
      mockIM.cleanUpObsoleteLocks(any[TopicPartition], Offset(ArgumentMatchers.anyLong()), any[Set[String]]),
    ).thenReturn(Right(()))
    when(mockIM.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, _: String, co: Option[Offset], _: Option[PendingState]) => Right(co))
    when(mockIM.updateMasterLock(any[TopicPartition], Offset(ArgumentMatchers.anyLong()))).thenReturn(Right(()))

    val pf = PartitionField(Seq("_value")).value

    val mockKN = mock[KeyNamer]
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map(pf -> "gc_d_key")))

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))
    when(mockFW.close()).thenAnswer(())

    val mockCP = mock[CommitPolicy]
    when(mockCP.shouldFlush(any[CommitContext])).thenReturn(false)

    val mockOKB = mock[ObjectKeyBuilder]
    when(
      mockOKB.build(
        Offset(ArgumentMatchers.anyLong()),
        Offset(ArgumentMatchers.anyLong()),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
      ),
    )
      .thenReturn(Right(CloudLocation("bucket", Some("prefix"), Some("prefix/file.parquet"))))

    type IndexUpdateFn = (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]
    val mockPOP = mock[PendingOperationsProcessors]
    when(
      mockPOP.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[IndexUpdateFn],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    )
      .thenAnswer {
        (
          tp: TopicPartition,
          _:  Option[Offset],
          ps: PendingState,
          fn: IndexUpdateFn,
          _:  Boolean,
          _:  Option[String],
          _:  Option[java.io.File],
        ) =>
          fn(tp, Some(ps.pendingOffset), None)
      }

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    def makeMockMsg(offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCP),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("gc-fallback-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = mockPOP,
    )

    // Granular lock returns None (simulates GC'd lock). Master lock is at 200.
    // The fallback should provide Some(Offset(200)) as lastSeekedOffset.
    // Offset 200 should be skipped (already committed), offset 201 should be written.
    wm.write(topicPartition.withOffset(Offset(200)), makeMockMsg(200L)) shouldBe Right(())
    verify(mockFW, never).write(any[MessageDetail])

    wm.write(topicPartition.withOffset(Offset(201)), makeMockMsg(201L)) shouldBe Right(())
    verify(mockFW, times(1)).write(any[MessageDetail])
  }

  test("master lock fallback: manual consumer rewind with GC'd granular locks skips records <= master offset") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(true)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(Some(Offset(500)))
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())
    when(
      mockIM.cleanUpObsoleteLocks(any[TopicPartition], Offset(ArgumentMatchers.anyLong()), any[Set[String]]),
    ).thenReturn(Right(()))
    when(mockIM.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, _: String, co: Option[Offset], _: Option[PendingState]) => Right(co))
    when(mockIM.updateMasterLock(any[TopicPartition], Offset(ArgumentMatchers.anyLong()))).thenReturn(Right(()))

    val pf = PartitionField(Seq("_value")).value

    val mockKN = mock[KeyNamer]
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map(pf -> "rewind_key")))

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))
    when(mockFW.close()).thenAnswer(())

    val mockCP = mock[CommitPolicy]
    when(mockCP.shouldFlush(any[CommitContext])).thenReturn(false)

    val mockOKB = mock[ObjectKeyBuilder]
    when(
      mockOKB.build(
        Offset(ArgumentMatchers.anyLong()),
        Offset(ArgumentMatchers.anyLong()),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
      ),
    )
      .thenReturn(Right(CloudLocation("bucket", Some("prefix"), Some("prefix/file.parquet"))))

    type IndexUpdateFn = (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]
    val mockPOP = mock[PendingOperationsProcessors]
    when(
      mockPOP.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[IndexUpdateFn],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    )
      .thenAnswer {
        (
          tp: TopicPartition,
          _:  Option[Offset],
          ps: PendingState,
          fn: IndexUpdateFn,
          _:  Boolean,
          _:  Option[String],
          _:  Option[java.io.File],
        ) =>
          fn(tp, Some(ps.pendingOffset), None)
      }

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    def makeMockMsg(offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCP),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("rewind-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = mockPOP,
    )

    // Operator rewound consumer to offset 100. Master lock is at 500 (granular locks GC'd).
    // All offsets 100-500 should be skipped; offset 501 should be the first written.
    (100L to 500L).foreach { i =>
      wm.write(topicPartition.withOffset(Offset(i)), makeMockMsg(i)) shouldBe Right(())
    }
    verify(mockFW, never).write(any[MessageDetail])

    wm.write(topicPartition.withOffset(Offset(501L)), makeMockMsg(501L)) shouldBe Right(())
    verify(mockFW, times(1)).write(any[MessageDetail])
  }

  test("selective commit: sibling buffered records replay correctly after crash; no data loss, no duplication") {
    when(indexManager.indexingEnabled).thenReturn(true)

    // Writer A has flushed offsets up to 105 (committedOffset = 105, granular lock = 105).
    // Under selective commit, A is now NoWriter and would not be re-touched on the next
    // commit cycle. Writer B is still in Writing with firstBufferedOffset = 100,
    // uncommittedOffset = 104 — its flush threshold has not fired.
    val writerA = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
                                           partitionKey     = Some("date=12_00"),
                                           lastSeekedOffset = Some(Offset(105)),
    )
    writerA.forceWriteState(NoWriter(CommitState(topicPartition, Some(Offset(105)))))

    val writerB = new Writer[FileMetadata](topicPartition,
                                           commitPolicy,
                                           indexManager,
                                           stagingFilenameFn,
                                           objectKeyBuilder,
                                           formatWriterFn,
                                           schemaChangeDetector,
                                           pendingOperationsProcessors,
                                           partitionKey     = Some("date=12_15"),
                                           lastSeekedOffset = None,
    )
    writerB.forceWriteState(
      Writing(
        CommitState(topicPartition, None),
        formatWriter,
        new File("test"),
        firstBufferedOffset = Offset(100),
        uncommittedOffset   = Offset(104),
        1L,
        1L,
      ),
    )

    // Pre-crash globalSafeOffset must clamp at writer B's firstBuffered = 100, NOT
    // advance past A's committedOffset = 105. The master lock is therefore written at
    // 100 (committedOffset = 99 in lock-file terms).
    val firstBufferedOffsets = Seq(writerA, writerB).flatMap(_.getFirstBufferedOffset)
    val committedOffsets     = Seq(writerA, writerB).flatMap(_.getCommittedOffset)
    val globalSafeOffset =
      if (firstBufferedOffsets.nonEmpty) firstBufferedOffsets.map(_.value).min
      else committedOffsets.map(_.value).max + 1
    globalSafeOffset shouldBe 100L

    // Simulated crash: the consumer is rewound to the master lock floor (offset 100).
    // Records re-delivered for writer B (offsets 100..104) are NOT skipped by its
    // restart-side `shouldSkip` because B never committed (lastSeekedOffset = None).
    val replayedB = new Writer[FileMetadata](topicPartition,
                                             commitPolicy,
                                             indexManager,
                                             stagingFilenameFn,
                                             objectKeyBuilder,
                                             formatWriterFn,
                                             schemaChangeDetector,
                                             pendingOperationsProcessors,
                                             partitionKey     = Some("date=12_15"),
                                             lastSeekedOffset = None,
    )
    Seq(100L, 101L, 102L, 103L, 104L).foreach { off =>
      replayedB.shouldSkip(Offset(off)) shouldBe false
    }

    // Records re-delivered for writer A (anything at or below 105) ARE skipped via the
    // surviving granular lock — no duplication at A's path even though A's records were
    // re-delivered alongside B's.
    val replayedA = new Writer[FileMetadata](topicPartition,
                                             commitPolicy,
                                             indexManager,
                                             stagingFilenameFn,
                                             objectKeyBuilder,
                                             formatWriterFn,
                                             schemaChangeDetector,
                                             pendingOperationsProcessors,
                                             partitionKey     = Some("date=12_00"),
                                             lastSeekedOffset = Some(Offset(105)),
    )
    Seq(100L, 101L, 102L, 103L, 104L, 105L).foreach { off =>
      replayedA.shouldSkip(Offset(off)) shouldBe true
    }
    replayedA.shouldSkip(Offset(106)) shouldBe false
  }

  test(
    "selective commit: schema rollover fans out to all TP writers via WriterManager.write, not only flushable writers",
  ) {
    // Integration test for WriterManager.rollOverTopicPartitionWriters routing.
    //
    // The critical invariant: when a record with an incompatible schema arrives for pk-a,
    // WriterManager must call commitForTopicPartition (full fan-out), NOT
    // commitFlushableWritersForTopicPartition (selective). The sibling writer for pk-b —
    // whose shouldFlush is false and therefore would never be touched by the selective path
    // — must also be committed so the format boundary stays consistent across every writer
    // on the topic-partition.
    //
    // If commitForTopicPartition were replaced with commitFlushableWritersForTopicPartition
    // in rollOverTopicPartitionWriters, the verify(mockFwB, times(1)).complete() assertion
    // at the end of this test would fail, catching the regression.
    //
    // Sequence:
    //   msg1 (schemaV1, pk-a) → writer A created, lastKnownSchema[A] = schemaV1
    //   msg2 (schemaV1, pk-b) → writer B created, lastKnownSchema[B] = schemaV1
    //   msg3 (schemaV2, pk-a) → writer A detects schema change → commitForTopicPartition
    //                           → both A and B committed (complete() called on both FormatWriters)
    //                           → msg3 written to fresh writer A (mockFwA2)

    // Three FormatWriter mocks dispensed in call order by multiFormatWriterFn:
    //   call 1 (msg1→pk-a)  → mockFwA   (writer A, schema-V1 records)
    //   call 2 (msg2→pk-b)  → mockFwB   (writer B, sibling never triggering rollover)
    //   call 3 (msg3→pk-a)  → mockFwA2  (new writer A created after rollover, schema-V2 records)
    val mockFwA  = mock[FormatWriter]
    val mockFwB  = mock[FormatWriter]
    val mockFwA2 = mock[FormatWriter]
    Seq(mockFwA, mockFwB, mockFwA2).foreach { fw =>
      when(fw.write(any[MessageDetail])).thenReturn(Right(()))
      when(fw.getPointer).thenReturn(100L)
      when(fw.rolloverFileOnSchemaChange()).thenReturn(true) // must be true for shouldRollover to fire
      when(fw.complete()).thenReturn(Right(()))
      when(fw.close()).thenAnswer(())
    }

    val fwSeq = Array(mockFwA, mockFwB, mockFwA2)
    var fwIdx = 0
    val multiFormatWriterFn: (TopicPartition, File) => Either[SinkError, FormatWriter] = (_, _) => {
      val fw = fwSeq(fwIdx); fwIdx += 1; Right(fw)
    }

    // Two distinct schema objects; reference inequality guarantees schemaHasChanged fires.
    val schemaV1 = mock[org.apache.kafka.connect.data.Schema]
    val schemaV2 = mock[org.apache.kafka.connect.data.Schema]

    val mockSCD = mock[SchemaChangeDetector]
    when(mockSCD.detectSchemaChange(schemaV1, schemaV2)).thenReturn(true)

    // shouldFlush always false — so selective commit would never touch writer B.
    val mockCP = mock[CommitPolicy]
    when(mockCP.shouldFlush(any[CommitContext])).thenReturn(false)

    // indexingEnabled = false keeps stubs minimal (single UploadOperation, no temp-copy chain).
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(false)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, _: String, co: Option[Offset], _: Option[PendingState]) => Right(co))

    val mockOKB = mock[ObjectKeyBuilder]
    when(
      mockOKB.build(
        Offset(ArgumentMatchers.anyLong()),
        Offset(ArgumentMatchers.anyLong()),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
      ),
    )
      .thenReturn(Right(CloudLocation("bucket", Some("prefix"), Some("prefix/file.jsonl"))))

    type IndexUpdateFn = (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]
    val mockPOP = mock[PendingOperationsProcessors]
    when(
      mockPOP.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[IndexUpdateFn],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    ).thenAnswer {
      (
        tp: TopicPartition,
        _:  Option[Offset],
        ps: PendingState,
        fn: IndexUpdateFn,
        _:  Boolean,
        _:  Option[String],
        _:  Option[java.io.File],
      ) =>
        fn(tp, Some(ps.pendingOffset), None)
    }

    type SD = io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData
    val valueV1 = mock[SD]
    val valueV2 = mock[SD]
    val mockKey = mock[SD]
    when(valueV1.schema()).thenReturn(Some(schemaV1))
    when(valueV2.schema()).thenReturn(Some(schemaV2))
    when(mockKey.schema()).thenReturn(None)

    def makeMsg(offset: Long, value: SD): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(value)
      when(m.key).thenReturn(mockKey)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    // Messages are created before the KeyNamer mock so that specific-instance argument
    // matching can be used: each message maps to its intended partition key without needing
    // thenAnswer (which has Scala type-inference issues in chained multi-arg form).
    val pf   = PartitionField(Seq("_value")).value
    val msg1 = makeMsg(10L, valueV1) // pk-a
    val msg2 = makeMsg(11L, valueV1) // pk-b
    val msg3 = makeMsg(12L, valueV2) // pk-a (triggers rollover)

    val mockKN = mock[KeyNamer]
    when(mockKN.processPartitionValues(msg1, topicPartition))
      .thenReturn(Right(immutable.Map(pf -> "pk-a")))
    when(mockKN.processPartitionValues(msg2, topicPartition))
      .thenReturn(Right(immutable.Map(pf -> "pk-b")))
    when(mockKN.processPartitionValues(msg3, topicPartition))
      .thenReturn(Right(immutable.Map(pf -> "pk-a")))

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCP),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("rollover-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = multiFormatWriterFn,
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = mockSCD,
      skipNullValues              = false,
      pendingOperationsProcessors = mockPOP,
    )

    // msg1: primes writer A with schemaV1 (lastKnownSchema[A] = schemaV1).
    wm.write(topicPartition.withOffset(Offset(10L)), msg1) shouldBe Right(())

    // msg2: primes writer B with schemaV1 (lastKnownSchema[B] = schemaV1).
    wm.write(topicPartition.withOffset(Offset(11L)), msg2) shouldBe Right(())

    // msg3: schema V2 on pk-a.  Writer A is still in Writing state when
    // rollOverTopicPartitionWriters is called, so shouldRollover fires:
    //   rolloverOnSchemaChange = true  (mockFwA.rolloverFileOnSchemaChange() = true)
    //   schemaHasChanged(schemaV2) = true  (lastKnownSchema=schemaV1, detector returns true)
    // → commitForTopicPartition flushes BOTH writer A and writer B.
    // → writer.write(msg3) then transitions writer A to Writing again with mockFwA2.
    wm.write(topicPartition.withOffset(Offset(12L)), msg3) shouldBe Right(())

    // Writer A committed during rollover (schema boundary before V2 record lands).
    verify(mockFwA, times(1)).complete()
    // Writer B (sibling) must also be committed by the full-fan-out path.
    // This assertion fails if rollOverTopicPartitionWriters is changed to use
    // commitFlushableWritersForTopicPartition, because mockCP.shouldFlush returns false.
    verify(mockFwB, times(1)).complete()
  }

  test("master lock fallback: globalSafeOffset == 0 produces None, no false skip of offset 0") {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(true)
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())
    when(
      mockIM.cleanUpObsoleteLocks(any[TopicPartition], Offset(ArgumentMatchers.anyLong()), any[Set[String]]),
    ).thenReturn(Right(()))
    when(mockIM.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, _: String, co: Option[Offset], _: Option[PendingState]) => Right(co))
    when(mockIM.updateMasterLock(any[TopicPartition], Offset(ArgumentMatchers.anyLong()))).thenReturn(Right(()))

    val pf = PartitionField(Seq("_value")).value

    val mockKN = mock[KeyNamer]
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map(pf -> "fresh_key")))

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(100L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)
    when(mockFW.complete()).thenReturn(Right(()))
    when(mockFW.close()).thenAnswer(())

    val mockCP = mock[CommitPolicy]
    when(mockCP.shouldFlush(any[CommitContext])).thenReturn(false)

    val mockOKB = mock[ObjectKeyBuilder]
    when(
      mockOKB.build(
        Offset(ArgumentMatchers.anyLong()),
        Offset(ArgumentMatchers.anyLong()),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.anyLong(),
      ),
    )
      .thenReturn(Right(CloudLocation("bucket", Some("prefix"), Some("prefix/file.parquet"))))

    type IndexUpdateFn = (TopicPartition, Option[Offset], Option[PendingState]) => Either[SinkError, Option[Offset]]
    val mockPOP = mock[PendingOperationsProcessors]
    when(
      mockPOP.processPendingOperations(
        any[TopicPartition],
        any[Option[Offset]],
        any[PendingState],
        any[IndexUpdateFn],
        any[Boolean],
        any[Option[String]],
        any[Option[java.io.File]],
      ),
    )
      .thenAnswer {
        (
          tp: TopicPartition,
          _:  Option[Offset],
          ps: PendingState,
          fn: IndexUpdateFn,
          _:  Boolean,
          _:  Option[String],
          _:  Option[java.io.File],
        ) =>
          fn(tp, Some(ps.pendingOffset), None)
      }

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    val mockKeySD = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    when(mockKeySD.schema()).thenReturn(None)

    def makeMockMsg(offset: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(Topic("test-topic"))
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(offset))
      when(m.epochTimestamp).thenReturn(1000L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockKeySD)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(mockCP),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("zero-offset-test-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mockOKB,
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = mockPOP,
    )

    // Both granular lock and master lock return None (globalSafeOffset == 0).
    // The fallback produces max(None, None) = None, so offset 0 must NOT be skipped.
    wm.write(topicPartition.withOffset(Offset(0)), makeMockMsg(0L)) shouldBe Right(())
    verify(mockFW, times(1)).write(any[MessageDetail])
  }

  // ── createWriter granular fallback max(granular, master) ─────────────────────────────

  /**
   * Builds a granular-mode WriterManager whose granular lock returns `granular` and whose master
   * lock returns `master`, then delivers `offset`. Returns whether the record was written (true)
   * or deduplicated (false).
   */
  private def granularFallbackWrites(
    granular: Option[Long],
    master:   Option[Long],
    offset:   Long,
  ): Boolean = {
    val mockIM = mock[IndexManager]
    when(mockIM.indexingEnabled).thenReturn(true)
    when(mockIM.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(mockIM.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String]))
      .thenReturn(Right(granular.map(Offset(_))))
    when(mockIM.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(master.map(Offset(_)))
    when(mockIM.evictAllGranularLocks(any[TopicPartition])).thenAnswer(())

    val mockKN = mock[KeyNamer]
    val pf     = PartitionField(Seq("_value")).value
    when(mockKN.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(Right(immutable.Map(pf -> "key_a")))

    val mockFW = mock[FormatWriter]
    when(mockFW.write(any[MessageDetail])).thenReturn(Right(()))
    when(mockFW.getPointer).thenReturn(1L)
    when(mockFW.rolloverFileOnSchemaChange()).thenReturn(false)

    val neverFlush = mock[CommitPolicy]
    when(neverFlush.shouldFlush(any[CommitContext])).thenReturn(false)

    val mockValue = mock[io.lenses.streamreactor.connect.cloud.common.sink.conversion.SinkData]
    when(mockValue.schema()).thenReturn(None)
    def msg(o: Long): MessageDetail = {
      val m = mock[MessageDetail]
      when(m.topic).thenReturn(topicPartition.topic)
      when(m.partition).thenReturn(0)
      when(m.offset).thenReturn(Offset(o))
      when(m.epochTimestamp).thenReturn(1L)
      when(m.value).thenReturn(mockValue)
      when(m.key).thenReturn(mockValue)
      when(m.headers).thenReturn(Map.empty)
      m
    }

    val metrics = new io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics()
    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(neverFlush),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", Some("prefix"))),
      keyNamerFn                  = _ => Right(mockKN),
      stagingFilenameFn           = (_, _) => Right(File.createTempFile("c8-", ".tmp")),
      objKeyBuilderFn             = (_, _) => mock[ObjectKeyBuilder],
      formatWriterFn              = (_, _) => Right(mockFW),
      indexManager                = mockIM,
      transformerF                = m => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = mock[PendingOperationsProcessors],
      metrics                     = metrics,
    )
    wm.write(topicPartition.withOffset(Offset(offset)), msg(offset)).value
    metrics.getRecordsWrittenTotal == 1L
  }

  test("[B] granular fallback with a granular lock at or above the master floor uses the granular offset") {
    // K.lock = 150 >= M = 99: floor 150, so record 150 is skipped and 151 is written.
    granularFallbackWrites(granular = Some(150), master = Some(99), offset = 150) shouldBe false
    granularFallbackWrites(granular = Some(150), master = Some(99), offset = 151) shouldBe true
  }

  test("[ND] granular fallback with a stale granular lock below the master floor uses the master offset") {
    // K.lock = 90 < M = 99: with `orElse` the floor would be 90 and record 95 would be written
    // (a duplicate); `max` raises the floor to 99 so 91..99 are skipped and 100 is written.
    granularFallbackWrites(granular = Some(90), master = Some(99), offset = 95) shouldBe false
    granularFallbackWrites(granular = Some(90), master = Some(99), offset = 99) shouldBe false
    granularFallbackWrites(granular = Some(90), master = Some(99), offset = 100) shouldBe true
  }
}
