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
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.NullSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
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
    w.forceWriteState(NoWriter(CommitState(tp, committedOffset)))
    w
  }

  private def writerInWritingState(
    tp:                  TopicPartition,
    committedOffset:     Option[Offset],
    firstBufferedOffset: Offset,
    uncommittedOffset:   Offset,
  ): Writer[FileMetadata] = {
    val w = makeWriter(tp, committedOffset)
    w.forceWriteState(
      Writing(
        CommitState(tp, committedOffset),
        formatWriter,
        new File("test"),
        firstBufferedOffset,
        uncommittedOffset,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
      ),
    )
    w
  }

  private def buildWriterManager(
    indexManager: IndexManager,
    metrics:      CloudSinkMetrics = new CloudSinkMetrics(),
    commitMode:   CommitMode       = CommitMode.Granular,
  ): WriterManager[FileMetadata] =
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
      commitMode                  = commitMode,
      metrics                     = metrics,
    )

  /** A writer parked in `Staged`: bytes already at a temp path, waiting for the batch CAS. */
  private def writerInStagedState(
    tp:                  TopicPartition,
    committedOffset:     Option[Offset],
    firstBufferedOffset: Offset,
    uncommittedOffset:   Offset,
  ): Writer[FileMetadata] = {
    val w = makeWriter(tp, committedOffset)
    w.forceWriteState(
      Staged(
        CommitState(tp, committedOffset),
        new File("test"),
        firstBufferedOffset,
        uncommittedOffset,
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 2L,
        recordCount             = 3L,
        bucket                  = "bucket",
        tempPath                = ".temp-upload/test-connector/topic/0/uuid/data/x.json",
        tempETag                = "etag-1",
        finalPath               = "data/x.json",
      ),
    )
    w
  }

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

  test("preCommit uses minFirstBuffered as safe offset even when it exceeds maxCommitted+1") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)
    // Writer A committed to 50, Writer B is writing with firstBuffered=1000.
    // Offsets 51-999 were handled by writers that have since been evicted after committing.
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
    result(tp0).offset() shouldBe 1000L
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

  test("preCommit skips updateMasterLock when globalSafeOffset is 0 (idle PARTITIONBY TP on fresh start)") {
    // Dirty-flag gate: on a fresh ownership episode where nothing has advanced,
    // `globalSafeOffset == 0` and `lastWrittenMasterSafeOffset(tp)` lazy-seeds to 0 from the
    // absent durable floor — so the dirty flag is false and the master-lock write is skipped.
    // The cycle still returns Some(Offset(0)) to Kafka (the consumer cannot advance until
    // any writer commits). This is the headline cost saving on idle partitions: previously
    // the code wrote on every cycle regardless of advance; now it only writes on advancement.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)
    // Writer A committed offset 5, Writer B is buffering from offset 0.
    // committedOffsets = [5] (non-empty, so preCommit doesn't short-circuit).
    // firstBufferedOffsets = [0], so calculatedSafeOffset = min(0) = 0.
    // previousHighWatermark = 0 (no master lock offset). globalSafeOffset = max(0, 0) = 0.
    val writerA = writerInNoWriterState(tp0, Some(Offset(5)))
    val writerB =
      writerInWritingState(tp0,
                           committedOffset     = Some(Offset(5)),
                           firstBufferedOffset = Offset(0),
                           uncommittedOffset   = Offset(4),
      )
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    val result = wm.preCommit(currentOffsets(tp0, 100))
    result should contain key tp0
    result(tp0).offset() shouldBe 0L

    verify(indexManager, never).updateMasterLock(any[TopicPartition], any[Offset])
    metrics.getMasterLockWriteSkipped shouldBe 1L
    metrics.getMasterLockUpdates shouldBe 0L
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

  test("preCommit never regresses globalSafeOffset after cleanUp even when the highest-offset writer is removed") {
    // HWM re-seed after `cleanUp(tp)`: cleanUp clears HWM and `lastWrittenMasterSafeOffset(tp)`
    // but PRESERVES `lastReturnedSafeOffset(tp)` so the post-rollback preCommit re-seeds
    // HWM as `max(durableFloor + 1, lastReturned)`. Without this preservation, a routine-
    // write failure or selective-eviction race could let the same task return a LOWER offset
    // than it had already returned to Kafka in an earlier cycle, breaking the monotonicity
    // invariant on globalSafeOffset and ultimately letting a successful master-lock write at
    // the lower value drive `cleanUpObsoleteLocks` past still-needed granular locks.
    //
    // Without the lastReturned preservation this path would drop to calculatedSafeOffset (31);
    // the preserved value correctly holds the floor at the previously returned 106.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

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
    // After cleanUp, HWM was cleared but lastReturnedSafeOffset(tp) = 106 was preserved.
    // HWM re-seed = max(durableFloor=0, lastReturned=106) = 106.
    // calculatedSafeOffset = 31. globalSafeOffset = max(31, 106) = 106 — no regression.
    // forceWriteAfterCleanUp(tp) fired, so the write was classified as WriteForced(PostCleanUp).
    result2(tp0).offset() shouldBe 106L
    metrics.getMasterLockWriteForcedPostCleanUp shouldBe 1L
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

  test("cleanUp closes writers and resets high watermark but preserves master-lock state") {
    // cleanUp is called from CloudSinkTask.rollback when a put() surfaced a FatalCloudSinkError.
    // The partition is still owned by this task -- only the writer state is invalidated.
    // The master-lock state in storage has not changed, so seekedOffsets / topicPartitionToETags
    // must remain populated; otherwise the next preCommit's updateMasterLock would permanently
    // fail with "Master index not found" until the next rebalance/restart.
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
    verify(indexManager, never).clearTopicPartitionState(tp0)
  }

  test("cleanUp followed by retried put + preCommit does not regress the master-lock high watermark") {
    // Regression for ZD-2451: a transient GCS upload error rolls back the writer for tp0,
    // Kafka Connect retries put(), and the next preCommit must continue to advance the
    // master-lock offset rather than stalling with "Master index not found". This test
    // pins the in-memory contract between WriterManager and IndexManager that makes that
    // recovery possible: cleanUp leaves seekedOffsets intact, so the high watermark is
    // re-initialised from the same master-lock offset on the first preCommit after cleanUp.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(105)))
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm      = buildWriterManager(indexManager)
    val writerA = writerInNoWriterState(tp0, Some(Offset(150)))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    val result1 = wm.preCommit(currentOffsets(tp0, 200))
    result1(tp0).offset() shouldBe 151L

    // Simulate the rollback path: cleanUp(tp0) then the same task retries with a fresh writer
    // for the same partition. No re-open(), no rebalance -- this is the in-place retry that
    // 11.7.0 broke when cleanUp cleared the master eTag cache.
    wm.cleanUp(tp0)

    val writerA2 = writerInNoWriterState(tp0, Some(Offset(160)))
    wm.putWriter(MapKey(tp0, dateA), writerA2)

    val result2 = wm.preCommit(currentOffsets(tp0, 200))
    // previousHighWatermark seed = master(105) + 1 = 106; calculatedSafeOffset = 160 + 1 = 161;
    // globalSafeOffset = max(161, 106) = 161. The point is that this returns a Some offset at
    // all -- with the buggy cleanUp, updateMasterLock would have returned Left and result2
    // would be empty.
    result2 should contain key tp0
    result2(tp0).offset() shouldBe 161L
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

  // --- evictIdleWritersIfNeeded (eager eviction) ---

  test("eviction removes all idle writers regardless of map size") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager)

    val writerA = writerInNoWriterState(tp0, Some(Offset(10)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(20)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    wm.evictIdleWritersNow(tp0)

    wm.writerCount shouldBe 0
  }

  test("eviction eagerly evicts granular cache entries for idle writers") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager)

    val writerA = writerInNoWriterState(tp0, Some(Offset(10)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(20)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    wm.writerCount shouldBe 2

    // Trigger eviction of all idle writers for tp0
    wm.evictIdleWritersNow(tp0)

    wm.writerCount shouldBe 0

    val pkA = WriterManager.derivePartitionKey(dateA).get
    val pkB = WriterManager.derivePartitionKey(dateB).get
    verify(indexManager).evictGranularLock(tp0, pkA)
    verify(indexManager).evictGranularLock(tp0, pkB)
  }

  test("eviction also evicts idle non-PARTITIONBY writers") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager)

    val emptyPartitionValues: immutable.Map[PartitionField, String] = Map.empty
    val writer = writerInNoWriterState(tp0, Some(Offset(10)))
    wm.putWriter(MapKey(tp0, emptyPartitionValues), writer)

    wm.evictIdleWritersNow(tp0)
    wm.writerCount shouldBe 0

    // Non-PARTITIONBY writers have no granular partition key
    // (`derivePartitionKey(Map.empty) == None`), so no granular lock eviction is expected.
    verify(indexManager, never).evictGranularLock(any[TopicPartition], any[String])
  }

  test("eviction does not remove writers that are actively writing") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager)

    val writerA = writerInWritingState(tp0, Some(Offset(10)), Offset(11), Offset(15))
    wm.putWriter(MapKey(tp0, dateA), writerA)

    wm.evictIdleWritersNow(tp0)

    wm.writerCount shouldBe 1
  }

  test("eviction only removes idle writers, leaving active ones intact") {
    val indexManager = mock[IndexManager]
    val dateC: immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-03")
    val dateD: immutable.Map[PartitionField, String] = Map(dateField -> "2024-01-04")
    val wm = buildWriterManager(indexManager)

    val writerA = writerInNoWriterState(tp0, Some(Offset(10)))
    val writerB = writerInWritingState(tp0, Some(Offset(20)), Offset(21), Offset(25))
    val writerC = writerInNoWriterState(tp0, Some(Offset(30)))
    val writerD = writerInNoWriterState(tp0, Some(Offset(40)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)
    wm.putWriter(MapKey(tp0, dateC), writerC)
    wm.putWriter(MapKey(tp0, dateD), writerD)

    wm.evictIdleWritersNow(tp0)

    // All 3 idle writers (A, C, D) evicted; only active writer B remains
    wm.writerCount shouldBe 1
  }

  // --- Defensive globalSafeOffset invariants (require checks in getOffsetAndMeta) ---

  test("preCommit require does not fire on the normal path when calculatedSafeOffset < HWM") {
    // Regression guard for the `require(globalSafeOffset >= previousHighWatermark)` invariant.
    // When the high watermark sits well above the currently calculable safe offset (because a
    // writer with a lower committedOffset has replaced a higher one), math.max keeps the
    // globalSafeOffset on the watermark so the `require` must remain silent.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)

    // Establish HWM at 500 by committing a writer with a high offset
    val highWriter = writerInNoWriterState(tp0, Some(Offset(499)))
    wm.putWriter(MapKey(tp0, dateA), highWriter)
    wm.preCommit(currentOffsets(tp0, 1000))(tp0).offset() shouldBe 500L

    // Replace with a writer whose calculatedSafeOffset would be 100 -- well below the HWM
    highWriter.close()
    val lowWriter = writerInWritingState(tp0,
                                         committedOffset     = Some(Offset(50)),
                                         firstBufferedOffset = Offset(100),
                                         uncommittedOffset   = Offset(110),
    )
    wm.putWriter(MapKey(tp0, dateA), lowWriter)

    // Must not throw from the `require`; must preserve monotonicity
    noException should be thrownBy wm.preCommit(currentOffsets(tp0, 1000))
    wm.preCommit(currentOffsets(tp0, 1000))(tp0).offset() shouldBe 500L
  }

  test("preCommit maintains monotone non-decreasing globalSafeOffset across a long sequence") {
    // Exercises the `require` invariant across a multi-cycle sequence where writers with
    // varying committed offsets come and go. The returned globalSafeOffset must never decrease
    // within a single assignment (no cleanUp call).
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(9)))
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)

    def stepWithIdleWriter(committed: Long): Long = {
      val w = writerInNoWriterState(tp0, Some(Offset(committed)))
      wm.putWriter(MapKey(tp0, dateA), w)
      val out = wm.preCommit(currentOffsets(tp0, 10000))(tp0).offset()
      w.close()
      out
    }

    // Sequence: [50, 120, 80, 200, 150, 30, 300]
    // Master lock seeds HWM to 10 (Offset(9) + 1). Returned offsets must be monotone.
    val observed = List(50L, 120L, 80L, 200L, 150L, 30L, 300L).map(stepWithIdleWriter)

    // Pairwise monotone non-decreasing
    observed.sliding(2).collect { case List(a, b) => (a, b) }.toList.foreach {
      case (a, b) => a should be <= b
    }

    // First step: calc=51, HWM=10 -> 51
    // Second step: calc=121, HWM=51 -> 121
    // Third step: calc=81, HWM=121 -> 121
    // Fourth step: calc=201, HWM=121 -> 201
    // Fifth step: calc=151, HWM=201 -> 201
    // Sixth step: calc=31, HWM=201 -> 201
    // Seventh step: calc=301, HWM=201 -> 301
    observed shouldBe List(51L, 121L, 121L, 201L, 201L, 201L, 301L)
  }

  // --- evictIdleWriters Some(newKey) exclusion via the production write() path ---

  /**
   * Regression guard for `evictIdleWriters(topicPartition, Some(key))` in WriterManager.write().
   *
   * When a new writer is created (NoWriter state), WriterManager immediately calls
   * `evictIdleWriters(tp, Some(newKey))` to reclaim other idle writers on the same
   * topic-partition.  Without the `Some(newKey)` exclusion, the brand-new writer would
   * evict itself (it is also in NoWriter / isIdle=true) before its first `write()` call.
   *
   * This test drives the production `write()` path so the `Some(key)` parameter is actually
   * passed, and verifies the intended invariant:
   *   - An existing idle writer for a different partition key IS evicted.
   *   - The just-created writer is NOT evicted.
   */
  test(
    "write() via production path evicts other idle writers on the same TP but preserves the just-created writer (Some(newKey) exclusion in evictIdleWriters)",
  ) {
    val keyNamer = mock[KeyNamer]
    when(keyNamer.processPartitionValues(any[MessageDetail], any[TopicPartition])).thenReturn(Right(dateB))

    val indexManager = mock[IndexManager]
    when(indexManager.ensureGranularLock(any[TopicPartition], any[String])).thenReturn(Right(()))
    when(indexManager.getSeekedOffsetForPartitionKey(any[TopicPartition], any[String])).thenReturn(Right(None))
    when(indexManager.getSeekedOffsetForTopicPartition(any[TopicPartition])).thenReturn(None)

    when(formatWriter.write(any[MessageDetail])).thenReturn(Right(()))

    val wm = new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(commitPolicy),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", None)),
      keyNamerFn                  = _ => Right(keyNamer),
      stagingFilenameFn           = (_, _) => Right(new File("test")),
      objKeyBuilderFn             = (_, _) => objectKeyBuilder,
      formatWriterFn              = (_, _) => Right(formatWriter),
      indexManager                = indexManager,
      transformerF                = (m: MessageDetail) => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = pendingOpsProcessors,
    )

    // Pre-populate an idle (NoWriter-state) writer for dateA on tp0.
    // This writer must be evicted when a new writer for dateB is created on the same tp0.
    val writerA = writerInNoWriterState(tp0, Some(Offset(10)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.writerCount shouldBe 1

    // Drive the production write() path for dateB.
    // Internally this calls createWriter → writers.put(keyB, writerB) →
    //   evictIdleWriters(tp0, Some(keyB)).
    // At the eviction point, writerB is still in NoWriter (isIdle=true), but the Some(keyB)
    // exclusion prevents it from being evicted; writerA (dateA, idle, not excluded) IS evicted.
    val msg = MessageDetail(
      key       = NullSinkData(None),
      value     = NullSinkData(None),
      headers   = Map.empty,
      timestamp = None,
      topic     = Topic("topic"),
      partition = 0,
      offset    = Offset(11),
    )
    val tpo = TopicPartitionOffset(Topic("topic"), 0, Offset(11))

    wm.write(tpo, msg).value

    // writerA (dateA) was evicted; writerB (dateB) was preserved — exactly 1 writer remains.
    wm.writerCount shouldBe 1

    // The surviving writer is the one for dateB (its granular lock was NOT evicted).
    verify(indexManager).evictGranularLock(tp0, WriterManager.derivePartitionKey(dateA).get)
    verify(indexManager, never).evictGranularLock(tp0, WriterManager.derivePartitionKey(dateB).get)
  }

  // ── Selective commit (PARTITIONBY) ──────────────────────────────────────────────────
  // These tests pin the safety contract for selective commit (see "Selective commit
  // fan-out" in `docs/datalake-exactly-once-partitionby.md`):
  // selective commit reduces the *CommitSet* but never the *BarrierSet*. preCommit must
  // continue to scan every active writer on the topic-partition when computing
  // globalSafeOffset, so the consumer-committable offset is bounded by the slowest active
  // writer's `firstBufferedOffset`.

  test("commitFlushableWriters commits only flushable writers and leaves non-flushable siblings in Writing") {
    // WriterManager-level wiring guard: routing `commitFlushableWriters` through
    // `WriterCommitManager` must invoke `Writer.commit` ONLY on the writer matching the
    // selective filter; the non-flushable sibling stays untouched.
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager)

    val flushableWriter = mock[Writer[FileMetadata]]
    when(flushableWriter.shouldFlush).thenReturn(true)
    when(flushableWriter.hasPendingUpload).thenReturn(false)
    when(flushableWriter.commit).thenReturn(Right(()))

    val siblingWriter = mock[Writer[FileMetadata]]
    when(siblingWriter.shouldFlush).thenReturn(false)
    when(siblingWriter.hasPendingUpload).thenReturn(false)

    wm.putWriter(MapKey(tp0, dateA), flushableWriter)
    wm.putWriter(MapKey(tp0, dateB), siblingWriter)

    wm.commitFlushableWriters().value shouldBe ()

    verify(flushableWriter).commit
    verify(siblingWriter, never).commit
  }

  test("preCommit barrier respects unflushed sibling after selective commit (single TP)") {
    // DoD 4a safety proof. Writer A has just selectively committed (now in NoWriter at
    // offset 105); Writer B is still buffering offsets 100..104 below its flush threshold.
    // globalSafeOffset must equal min(firstBufferedOffsets) = 100, NOT max(committed)+1
    // = 106. The master lock advances to 100, and the safeOffsetBarrierWriters gauge
    // reports the single barrier-holder so operators can correlate the consumer-lag floor
    // with the buffering writer.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    val writerA = writerInNoWriterState(tp0, Some(Offset(105)))
    val writerB = writerInWritingState(tp0,
                                       committedOffset     = None,
                                       firstBufferedOffset = Offset(100),
                                       uncommittedOffset   = Offset(104),
    )
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    val result = wm.preCommit(currentOffsets(tp0, 200))
    result should contain key tp0
    result(tp0).offset() shouldBe 100L

    verify(indexManager).updateMasterLock(tp0, Offset(100))
    metrics.getSafeOffsetBarrierWriters shouldBe 1
  }

  test("preCommit no-buffer fallback remains max(committed) + 1 after a selective commit drains every active writer") {
    // After selective commit drained both writers (both now NoWriter), there are no
    // buffered offsets — the safe-offset formula falls back to `max(committed) + 1`,
    // unchanged from the legacy fan-out behaviour.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    val writerA = writerInNoWriterState(tp0, Some(Offset(120)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(140)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    val result = wm.preCommit(currentOffsets(tp0, 200))
    result should contain key tp0
    result(tp0).offset() shouldBe 141L

    metrics.getSafeOffsetBarrierWriters shouldBe 0
  }

  test("HWM never regresses across selective commit + idle eviction + re-creation of a sibling writer") {
    // DoD 4a headline regression guard for invariant 3 (safeOffsetHighWatermarks
    // monotonic floor). Sequence:
    //   1. Two writers committed at 200 and 300 → globalSafeOffset = 301, HWM caches 301.
    //   2. The high-offset sibling is evicted (idle); a fresh sibling is created at the
    //      same key but with a much lower committed offset (50).
    //   3. The next preCommit must NOT regress below 301 — the HWM keeps the floor even
    //      though calculated_safeOffset drops to 201.
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val wm = buildWriterManager(indexManager)

    val writerA = writerInNoWriterState(tp0, Some(Offset(200)))
    val writerB = writerInNoWriterState(tp0, Some(Offset(300)))
    wm.putWriter(MapKey(tp0, dateA), writerA)
    wm.putWriter(MapKey(tp0, dateB), writerB)

    wm.preCommit(currentOffsets(tp0, 1000))(tp0).offset() shouldBe 301L

    // Simulate idle-writer eviction of the high-offset sibling (writerB) followed by
    // re-creation at a much lower committed offset (e.g. a fresh partition key reusing
    // the same MapKey via dateB).
    writerB.close()
    val replacementSibling = writerInNoWriterState(tp0, Some(Offset(50)))
    wm.putWriter(MapKey(tp0, dateB), replacementSibling)

    val result = wm.preCommit(currentOffsets(tp0, 1000))
    // calculatedSafeOffset = max(200, 50) + 1 = 201; HWM = 301; max -> 301.
    result(tp0).offset() shouldBe 301L
  }

  test("preCommit barrier holds across consecutive selective commit cycles") {
    // Three back-to-back selective flushes of writer A while writer B keeps buffering.
    // After every cycle the barrier is held by writer B's firstBufferedOffset (100), so
    // globalSafeOffset must NEVER exceed 100, regardless of how many times A commits a
    // higher offset. This is the multi-cycle counterpart of the single-cycle barrier
    // proof above and exercises both invariant 3 (monotonic HWM floor) and invariant 8
    // (BarrierSet remains the entire active-writer set).
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(None)
    when(indexManager.updateMasterLock(any[TopicPartition], any[Offset])).thenReturn(Right(()))
    when(indexManager.cleanUpObsoleteLocks(any[TopicPartition], any[Offset], any[Set[String]])).thenReturn(Right(()))

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(indexManager, metrics)

    val writerB = writerInWritingState(tp0,
                                       committedOffset     = None,
                                       firstBufferedOffset = Offset(100),
                                       uncommittedOffset   = Offset(110),
    )
    wm.putWriter(MapKey(tp0, dateB), writerB)

    // Three commit cycles with A landing at increasing offsets. Each cycle replaces the
    // dateA writer in NoWriter state at progressively higher committed offsets — the
    // post-selective-commit shape after `Writing -> Uploading -> NoWriter`.
    Seq(150L, 250L, 350L).foreach { committed =>
      val newA = writerInNoWriterState(tp0, Some(Offset(committed)))
      wm.putWriter(MapKey(tp0, dateA), newA)
      val result = wm.preCommit(currentOffsets(tp0, 5000))
      result(tp0).offset() shouldBe 100L
      newA.close()
    }

    // The barrier-writer count remained 1 across every cycle.
    metrics.getSafeOffsetBarrierWriters shouldBe 1
  }

  test("eviction is scoped to the target TopicPartition and does not evict idle writers from other partitions") {
    val indexManager = mock[IndexManager]
    val wm           = buildWriterManager(indexManager)

    val tp1 = Topic("topic").withPartition(1)

    val writerTp0A = writerInNoWriterState(tp0, Some(Offset(10)))
    val writerTp0B = writerInNoWriterState(tp0, Some(Offset(20)))
    val writerTp1A = writerInNoWriterState(tp1, Some(Offset(30)))
    val writerTp1B = writerInNoWriterState(tp1, Some(Offset(40)))
    wm.putWriter(MapKey(tp0, dateA), writerTp0A)
    wm.putWriter(MapKey(tp0, dateB), writerTp0B)
    wm.putWriter(MapKey(tp1, dateA), writerTp1A)
    wm.putWriter(MapKey(tp1, dateB), writerTp1B)

    wm.writerCount shouldBe 4

    wm.evictIdleWritersNow(tp0)

    // Only tp0's idle writers evicted; tp1's idle writers remain untouched
    wm.writerCount shouldBe 2
    verify(indexManager).evictGranularLock(tp0, WriterManager.derivePartitionKey(dateA).get)
    verify(indexManager).evictGranularLock(tp0, WriterManager.derivePartitionKey(dateB).get)
    verify(indexManager, never).evictGranularLock(eqTo(tp1), any[String])
  }

  // ── Partition-batch commit mode: preCommit ──────────────────────────────────────────
  // In batch mode `preCommit` is a pure read: it computes the same globalSafeOffset barrier as
  // granular mode but never writes the master lock, because the only thing allowed to advance
  // the durable floor is a completed batch CAS.

  test("[NL] batch: preCommit returns the Staged writer's firstBufferedOffset, not the idle sibling's") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(99)))

    val wm = buildWriterManager(indexManager, commitMode = CommitMode.Batch)
    wm.putWriter(
      MapKey(tp0, dateA),
      writerInStagedState(tp0, Some(Offset(99)), firstBufferedOffset = Offset(100), uncommittedOffset = Offset(199)),
    )
    wm.putWriter(MapKey(tp0, dateB), writerInNoWriterState(tp0, Some(Offset(250))))

    wm.preCommit(currentOffsets(tp0, 400)).get(tp0).value.offset() shouldBe 100L
    verify(indexManager, never).updateMasterLock(any[TopicPartition], any[Offset])
  }

  test("[NL] batch: preCommit tracks the buffered floor across null-skipped records and then advances") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(250)))

    val wm = buildWriterManager(indexManager, commitMode = CommitMode.Batch)
    // Offsets 251..260 were null-skipped, so no writer buffers them; 261 is buffered.
    wm.putWriter(
      MapKey(tp0, dateA),
      writerInWritingState(tp0, Some(Offset(250)), firstBufferedOffset = Offset(261), uncommittedOffset = Offset(261)),
    )

    wm.preCommit(currentOffsets(tp0, 400)).get(tp0).value.offset() shouldBe 261L

    // After the batch commits at 261 every writer is idle, so the barrier moves to 262.
    wm.writerFor(MapKey(tp0, dateA)).value.forceWriteState(NoWriter(CommitState(tp0, Some(Offset(261)))))
    wm.preCommit(currentOffsets(tp0, 400)).get(tp0).value.offset() shouldBe 262L
    verify(indexManager, never).updateMasterLock(any[TopicPartition], any[Offset])
  }

  test("[B] batch: the high-watermark invariant still holds across cleanUp and re-seed") {
    val indexManager = mock[IndexManager]
    when(indexManager.getSeekedOffsetForTopicPartition(tp0)).thenReturn(Some(Offset(199)))

    val wm = buildWriterManager(indexManager, commitMode = CommitMode.Batch)
    wm.putWriter(MapKey(tp0, dateA), writerInNoWriterState(tp0, Some(Offset(250))))
    wm.preCommit(currentOffsets(tp0, 400)).get(tp0).value.offset() shouldBe 251L

    // Rollback drops the writers and the HWM, but `lastReturnedSafeOffset` is preserved so the
    // re-seed cannot hand Kafka a lower offset than it has already been given.
    wm.cleanUp(tp0)
    wm.putWriter(MapKey(tp0, dateB), writerInNoWriterState(tp0, Some(Offset(210))))

    // The `require`s inside getOffsetAndMeta are the assertion here: a regression throws.
    wm.preCommit(currentOffsets(tp0, 400)).get(tp0).value.offset() shouldBe 251L
    verify(indexManager, never).updateMasterLock(any[TopicPartition], any[Offset])
  }
}
