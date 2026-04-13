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
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.immutable

class WriterManagerPreCommitTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfter {

  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("test-connector", 1, 1)
  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val tp0 = Topic("topic").withPartition(0)

  private val dateField: PartitionField                        = ValuePartitionField(PartitionNamePath("date"))
  private val dateA:     immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-01")
  private val dateB:     immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-02")

  private val commitPolicy         = mock[CommitPolicy]
  private val formatWriter         = mock[FormatWriter]
  private val objectKeyBuilder     = mock[ObjectKeyBuilder]
  private val schemaChangeDetector = mock[SchemaChangeDetector]
  private val pendingOpsProcessors = mock[PendingOperationsProcessors]

  private def makeWriter(
    tp:              TopicPartition,
    committedOffset: Option[Offset],
    partitionKey:    Option[String] = None,
  ): Writer[FileMetadata] = {
    val indexManager = mock[IndexManager]
    when(indexManager.indexingEnabled).thenReturn(true)
    new Writer[FileMetadata](
      tp,
      commitPolicy,
      indexManager,
      () => Right(new File("test")),
      objectKeyBuilder,
      _ => Right(formatWriter),
      schemaChangeDetector,
      pendingOpsProcessors,
      partitionKey,
      committedOffset,
    )
  }

  private def writerInNoWriterState(
    tp:              TopicPartition,
    committedOffset: Option[Offset],
  ): Writer[FileMetadata] = {
    val w = makeWriter(tp, committedOffset)
    w.writeState = NoWriter(CommitState(tp, committedOffset))
    w
  }

  private def writerInWritingState(
    tp:                  TopicPartition,
    committedOffset:     Option[Offset],
    firstBufferedOffset: Offset,
    uncommittedOffset:   Offset,
  ): Writer[FileMetadata] = {
    val w = makeWriter(tp, committedOffset)
    w.writeState = Writing(
      CommitState(tp, committedOffset),
      formatWriter,
      new File("test"),
      firstBufferedOffset,
      uncommittedOffset,
      System.currentTimeMillis(),
      System.currentTimeMillis(),
    )
    w
  }

  private def buildWriterManager(indexManager: IndexManager, maxWriters: Int = 10000): WriterManager[FileMetadata] =
    new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(commitPolicy),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", None)),
      keyNamerFn                  = _ => Right(mock[KeyNamer]),
      stagingFilenameFn           = (_, _) => Right(new File("test")),
      objKeyBuilderFn             = (_, _) => objectKeyBuilder,
      formatWriterFn              = (_, _) => Right(formatWriter),
      indexManager                = indexManager,
      transformerF                = (m: MessageDetail) => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = pendingOpsProcessors,
      maxWriters                  = maxWriters,
    )

  private def currentOffsets(tp: TopicPartition, offset: Long): immutable.Map[TopicPartition, OffsetAndMetadata] =
    Map(tp -> new OffsetAndMetadata(offset))

  // --- globalSafeOffset computation ---

  test("preCommit returns no offset when no writers exist for a partition") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager)

    val result = wm.preCommit(currentOffsets(tp0, 100))
    result shouldBe empty
  }

  test("preCommit returns no offset when no writer has committed yet") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager)

    val writerA =
      writerInWritingState(tp0, committedOffset = None, firstBufferedOffset = Offset(0), uncommittedOffset = Offset(5))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    val result = wm.preCommit(currentOffsets(tp0, 100))
    result shouldBe empty
  }

  test("preCommit returns max(committed) + 1 when all writers are idle (no buffered data)") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(104)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    val result = wm.preCommit(currentOffsets(tp0, 200))
    result should contain key tp0
    result(tp0).offset() shouldBe 106L
  }

  test("preCommit returns min(firstBufferedOffset) when writers have uncommitted data") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm      = buildWriterManager(indexManager)
    val writerB = writerInNoWriterState(tp0, Some(Offset(105)))
    val writerA = writerInWritingState(tp0,
                                       committedOffset     = None,
                                       firstBufferedOffset = Offset(100),
                                       uncommittedOffset   = Offset(104),
    )
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    val result = wm.preCommit(currentOffsets(tp0, 200))
    result should contain key tp0
    result(tp0).offset() shouldBe 100L
  }

  test("preCommit takes min of firstBuffered and maxCommitted+1 to prevent skipping unseen partition keys") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)
    // Writer A committed to 50, Writer B is writing with firstBuffered=1000
    // Without math.min, globalSafeOffset would be 1000, skipping offsets 51-999
    val writerA = writerInNoWriterState(tp0, Some(Offset(50)))
    val writerB = writerInWritingState(tp0,
                                       committedOffset     = Some(Offset(50)),
                                       firstBufferedOffset = Offset(1000),
                                       uncommittedOffset   = Offset(1005),
    )
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    val result = wm.preCommit(currentOffsets(tp0, 2000))
    result should contain key tp0
    // min(1000, max(50,50)+1) = min(1000, 51) = 51
    result(tp0).offset() shouldBe 51L
  }

  // --- High watermark initialization ---

  test("preCommit initializes high watermark from master lock offset on first call") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(99)))
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(30)))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    val result = wm.preCommit(currentOffsets(tp0, 200))
    result should contain key tp0
    // calculatedSafeOffset = max(30) + 1 = 31
    // previousHighWatermark = 99 + 1 = 100
    // globalSafeOffset = max(31, 100) = 100
    result(tp0).offset() shouldBe 100L
  }

  test("preCommit uses 0 as high watermark when no master lock offset exists") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    val result = wm.preCommit(currentOffsets(tp0, 200))
    result should contain key tp0
    // calculatedSafeOffset = 51, previousHighWatermark = 0
    // globalSafeOffset = max(51, 0) = 51
    result(tp0).offset() shouldBe 51L
  }

  // --- Master lock failure handling ---

  test("preCommit returns no offset when master lock update fails") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("eTag mismatch", tp0)))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    val result = wm.preCommit(currentOffsets(tp0, 200))
    result shouldBe empty

    verify(indexManager, never).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])
  }

  test("preCommit does not advance high watermark on master lock failure") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("eTag mismatch", tp0)))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    wm.preCommit(currentOffsets(tp0, 200)) shouldBe empty

    // Now let the master lock succeed
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val result = wm.preCommit(currentOffsets(tp0, 200))
    result should contain key tp0
    // If high watermark was incorrectly advanced on failure, this would be wrong
    result(tp0).offset() shouldBe 51L
  }

  // --- Monotonicity ---

  test("preCommit never regresses globalSafeOffset even when highest-offset writer is removed") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)

    // First round: two idle writers, committed to 104 and 105
    val writerA = writerInNoWriterState(tp0, Some(Offset(104)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    val result1 = wm.preCommit(currentOffsets(tp0, 200))
    result1(tp0).offset() shouldBe 106L

    // Simulate eviction of Writer B (highest offset)
    wm.cleanUp(tp0)
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)

    // Re-add only Writer A with lower offset
    val writerA2 = writerInNoWriterState(tp0, Some(Offset(30)))
    wm.putWriter(MapKey(tp0, dateA), writerA2)

    val result2 = wm.preCommit(currentOffsets(tp0, 200))
    result2 should contain key tp0
    // After cleanUp, high watermark was cleared. On re-assignment, master lock is authoritative.
    // getSeekedOffsetForTopicPartition returns None, so previousHighWatermark = 0.
    // calculatedSafeOffset = 31. globalSafeOffset = max(31, 0) = 31.
    // This is correct: cleanUp clears the watermark because re-assignment means fresh start.
    result2(tp0).offset() shouldBe 31L
  }

  test("preCommit high watermark prevents regression within same assignment (no cleanUp)") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)

    // First round: committed to 105
    val writerA = writerInNoWriterState(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    val result1 = wm.preCommit(currentOffsets(tp0, 200))
    result1(tp0).offset() shouldBe 106L

    // Replace with a writer that has a lower committed offset (simulates eviction + re-creation
    // without cleanUp -- the writer was evicted from the map but the partition was not revoked)
    val keyA = MapKey(tp0, dateA)
    writerA.close()
    val writerA2 = writerInNoWriterState(tp0, Some(Offset(30)))
    wm.putWriter(keyA, writerA2)

    val result2 = wm.preCommit(currentOffsets(tp0, 200))
    result2 should contain key tp0
    // calculatedSafeOffset = 31, but previousHighWatermark = 106
    // globalSafeOffset = max(31, 106) = 106 -- no regression
    result2(tp0).offset() shouldBe 106L
  }

  // --- GC interaction ---

  test("preCommit passes correct activePartitionKeys to cleanUpObsoleteLocks") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(50)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(60)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    wm.preCommit(currentOffsets(tp0, 200))

    import org.mockito.ArgumentCaptor
    val activeKeysCaptor = ArgumentCaptor.forClass(classOf[Set[String]])
    verify(indexManager).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], activeKeysCaptor.capture())
    val capturedKeys = activeKeysCaptor.getValue
    capturedKeys should have size 2
    capturedKeys should contain(WriterManager.derivePartitionKey(dateA).get)
    capturedKeys should contain(WriterManager.derivePartitionKey(dateB).get)
  }

  test("preCommit skips GC on master lock update failure") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset]))
      .thenReturn(Left(FatalCloudSinkError("write failed", tp0)))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    wm.preCommit(currentOffsets(tp0, 200))

    verify(indexManager, never).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])
  }

  // --- close() and cleanUp() ---

  test("close clears high watermarks and evicts granular locks") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    wm.preCommit(currentOffsets(tp0, 200))
    wm.close()

    verify(indexManager).evictAllGranularLocks(tp0)
    verify(indexManager, never).clearTopicPartitionState(tp0)
    wm.writerCount shouldBe 0
  }

  test("cleanUp clears high watermark so re-assignment starts fresh from master lock") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(105)))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    val result1 = wm.preCommit(currentOffsets(tp0, 200))
    result1(tp0).offset() shouldBe 106L

    wm.cleanUp(tp0)

    verify(indexManager).evictAllGranularLocks(tp0)
    verify(indexManager).clearTopicPartitionState(tp0)

    // After cleanUp, re-add with lower offset
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(20)))
    val writerA2 = writerInNoWriterState(tp0, Some(Offset(20)))
    wm.putWriter(MapKey(tp0, dateA), writerA2)

    val result2 = wm.preCommit(currentOffsets(tp0, 200))
    // previousHighWatermark re-initialized from master lock: 20 + 1 = 21
    // calculatedSafeOffset = 20 + 1 = 21
    // globalSafeOffset = max(21, 21) = 21
    result2(tp0).offset() shouldBe 21L
  }

  // --- non-PARTITIONBY writers have empty partitionValues, producing no partition key ---

  test("preCommit skips updateMasterLock for non-PARTITIONBY writers (empty partitionValues)") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)

    val wm = buildWriterManager(indexManager)
    val emptyPartitionValues: immutable.Map[PartitionField, String] = Map.empty
    val writer = writerInNoWriterState(tp0, Some(Offset(50)))
    wm.putWriter(MapKey(tp0, emptyPartitionValues), writer)

    val result = wm.preCommit(currentOffsets(tp0, 200))
    result should contain key tp0
    result(tp0).offset() shouldBe 51L

    verify(indexManager, never).updateMasterLock(any[TopicPartition], any[Offset])
    verify(indexManager, never).cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])
  }

  test("preCommit maintains high watermark correctly for non-PARTITIONBY writers across cycles") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)

    val wm = buildWriterManager(indexManager)
    val emptyPartitionValues: immutable.Map[PartitionField, String] = Map.empty

    val writer1 = writerInNoWriterState(tp0, Some(Offset(50)))
    wm.putWriter(MapKey(tp0, emptyPartitionValues), writer1)

    val result1 = wm.preCommit(currentOffsets(tp0, 200))
    result1(tp0).offset() shouldBe 51L

    // Replace with a writer that has a lower committed offset (simulates eviction + re-creation
    // without cleanUp). High watermark should prevent regression.
    writer1.close()
    val writer2 = writerInNoWriterState(tp0, Some(Offset(30)))
    wm.putWriter(MapKey(tp0, emptyPartitionValues), writer2)

    val result2 = wm.preCommit(currentOffsets(tp0, 200))
    result2(tp0).offset() shouldBe 51L // high watermark prevents regression

    verify(indexManager, never).updateMasterLock(any[TopicPartition], any[Offset])
  }

  // --- evictIdleWritersIfNeeded ---

  test("eviction does nothing when writer count is below maxWriters") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager, maxWriters = 5)

    val writerA = writerInNoWriterState(tp0, Some(Offset(10)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(20)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    wm.evictIdleWritersNow()

    wm.writerCount shouldBe 2
  }

  test("eviction removes idle writers when count equals maxWriters") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager, maxWriters = 2)

    val writerA = writerInNoWriterState(tp0, Some(Offset(10)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(20)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    wm.evictIdleWritersNow()

    // maxWriters=2, size=2, evicts take(2 - 2 + 1) = 1 idle writer
    wm.writerCount shouldBe 1
  }

  test("eviction does not remove writers that are actively writing") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager, maxWriters = 1)

    val writerA = writerInWritingState(tp0, Some(Offset(10)), Offset(11), Offset(15))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    wm.evictIdleWritersNow()

    wm.writerCount shouldBe 1
  }

  test("eviction only removes idle writers, leaving active ones intact") {
    val indexManager = mock[IndexManager]
    val dateC: immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-03")
    val wm = buildWriterManager(indexManager, maxWriters = 2)

    val writerA = writerInNoWriterState(tp0, Some(Offset(10)))
    val writerB = writerInWritingState(tp0, Some(Offset(20)), Offset(21), Offset(25))
    val writerC = writerInNoWriterState(tp0, Some(Offset(30)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)
    wm.putWriter(MapKey(tp0, dateC), writerC)

    wm.evictIdleWritersNow()

    // 3 writers, maxWriters=2 => needs to evict take(3 - 2 + 1) = 2, but only 2 are idle
    // Both idle writers (A and C) are evicted, active writer B remains
    wm.writerCount shouldBe 1
  }

  test("eviction respects insertion order (LinkedHashMap) for idle writer selection") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager, maxWriters = 2)

    // Writer A (oldest, idle), Writer B (newest, idle)
    val writerA = writerInNoWriterState(tp0, Some(Offset(10)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(20)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    wm.evictIdleWritersNow()

    wm.writerCount shouldBe 1
    // Writer A was first in insertion order, so it gets evicted first.
    // Verify B remains: preCommit should report offset 21 (B's committed + 1)
    val result = wm.preCommit(currentOffsets(tp0, 200))
    result should contain key tp0
    result(tp0).offset() shouldBe 21L
  }
}
