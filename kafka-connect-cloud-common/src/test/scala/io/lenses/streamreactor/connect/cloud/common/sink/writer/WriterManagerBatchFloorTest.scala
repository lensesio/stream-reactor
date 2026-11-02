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

import cats.data.Validated
import cats.implicits.catsSyntaxEitherId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StringSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable

/**
 * The batch-mode deduplication floor: `N <= max(masterW(tp), maxBuffered(tp))`.
 *
 * Granular mode asks each writer's own lock whether an offset has already been handled, which is
 * only correct if a replayed offset routes back to the same key. Batch mode replaces that with a
 * topic-partition-level floor, so a key rotation between the original delivery and the replay
 * cannot resurrect an offset the task already has in hand.
 */
class WriterManagerBatchFloorTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("floor-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)

  private val bucket        = "test-bucket"
  private val directoryName = ".indexes"
  private val tp            = Topic("orders").withPartition(0)

  private val keyField: PartitionField = ValuePartitionField(PartitionNamePath("k"))
  private def pv(name: String): immutable.Map[PartitionField, String] = Map(keyField -> name)
  private val pvA = pv("A")
  private val pvB = pv("B")
  private val pvC = pv("C")

  private var storage:      InMemoryStorageInterface = _
  private var indexManager: IndexManagerV2           = _

  private def bucketAndPrefix(topicPartition: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/")).asRight

  /** Mutable partition-values holder: the seam a wall-clock SMT would move under us. */
  private class RotatingKeyNamer(initial: immutable.Map[PartitionField, String]) {
    @volatile var current: immutable.Map[PartitionField, String] = initial
    val keyNamer: KeyNamer = {
      val m = mock[KeyNamer]
      when(m.processPartitionValues(any[MessageDetail], any[TopicPartition]))
        .thenAnswer((_: MessageDetail, _: TopicPartition) => current.asRight[SinkError])
      m
    }
  }

  /** A format writer that fails `write` exactly once, for one nominated offset. */
  private class FailOnceAtOffset(doomed: Long) {
    private val armed = new AtomicBoolean(true)
    val formatWriter: FormatWriter = {
      val m = mock[FormatWriter]
      when(m.write(any[MessageDetail])).thenAnswer { (md: MessageDetail) =>
        if (md.offset.value == doomed && armed.compareAndSet(true, false))
          new RuntimeException(s"injected write failure at ${md.offset.value}").asLeft
        else ().asRight
      }
      when(m.complete()).thenReturn(().asRight)
      when(m.rolloverFileOnSchemaChange()).thenReturn(false)
      m
    }
  }

  override def beforeEach(): Unit = {
    storage      = new InMemoryStorageInterface()
    indexManager = buildIndexManager(CommitMode.Batch)
  }

  override def afterEach(): Unit =
    Option(indexManager).foreach(_.close())

  private def buildIndexManager(commitMode: CommitMode): IndexManagerV2 = {
    implicit val si: InMemoryStorageInterface = storage
    val im = new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = Int.MaxValue,
      gcSweepMinAgeSeconds        = Int.MaxValue,
      gcSweepEnabled              = false,
      commitMode                  = commitMode,
    )(si, connectorTaskId)
    im.open(Set(tp)).value
    // Seed the durable floor: globalSafeOffset 100 -> committedOffset 99.
    im.updateMasterLock(tp, Offset(100)).value
    im
  }

  private def buildWriterManager(
    im:         IndexManagerV2,
    metrics:    CloudSinkMetrics,
    namer:      RotatingKeyNamer,
    fw:         FormatWriter,
    commitMode: CommitMode,
  ): WriterManager[FakeFileMetadata] = {
    val neverFlush = mock[CommitPolicy]
    when(neverFlush.shouldFlush(any[CommitContext])).thenReturn(false)

    new WriterManager[FakeFileMetadata](
      commitPolicyFn              = _ => neverFlush.asRight,
      bucketAndPrefixFn           = bucketAndPrefix,
      keyNamerFn                  = _ => namer.keyNamer.asRight,
      stagingFilenameFn           = (_, _) => Files.createTempFile("floor-", ".tmp").toFile.asRight,
      objKeyBuilderFn             = (_, _) => mock[ObjectKeyBuilder],
      formatWriterFn              = (_, _) => fw.asRight,
      indexManager                = im,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      commitMode                  = commitMode,
      metrics                     = metrics,
    )
  }

  private def message(offset: Long): MessageDetail =
    MessageDetail(
      key       = StringSinkData("k", None),
      value     = StringSinkData("v", None),
      headers   = Map.empty,
      timestamp = None,
      topic     = tp.topic,
      partition = tp.partition,
      offset    = Offset(offset),
    )

  private def deliver(wm: WriterManager[FakeFileMetadata], offset: Long): Either[SinkError, Unit] =
    wm.write(tp.withOffset(Offset(offset)), message(offset))

  test("[ND] offsets at or below the master floor are skipped whichever key they route to; 100 is written") {
    val metrics = new CloudSinkMetrics()
    val namer   = new RotatingKeyNamer(pvA)
    val fw      = new FailOnceAtOffset(-1L).formatWriter
    val wm      = buildWriterManager(indexManager, metrics, namer, fw, CommitMode.Batch)

    deliver(wm, 50L).value
    namer.current = pvB
    deliver(wm, 99L).value

    metrics.getDuplicateRecordsSkippedTotal shouldBe 2L
    metrics.getRecordsWrittenTotal shouldBe 0L

    namer.current = pvC
    deliver(wm, 100L).value
    metrics.getRecordsWrittenTotal shouldBe 1L
    metrics.getDuplicateRecordsSkippedTotal shouldBe 2L

    storage.keysUnder(bucket, "data/") shouldBe empty
    wm.close()
  }

  test("[ND] in-process re-delivery after a key rotation does not re-buffer offsets already held") {
    val metrics = new CloudSinkMetrics()
    val namer   = new RotatingKeyNamer(pvA)
    val fw      = new FailOnceAtOffset(103L).formatWriter
    val wm      = buildWriterManager(indexManager, metrics, namer, fw, CommitMode.Batch)

    (100L to 102L).foreach(o => deliver(wm, o).value)
    deliver(wm, 103L).left.value.rollBack() shouldBe false
    metrics.getRecordsWrittenTotal shouldBe 3L

    // The SMT rotates the key; Connect re-delivers the whole batch from 100.
    namer.current = pvC
    (100L to 103L).foreach(o => deliver(wm, o).value)

    metrics.getDuplicateRecordsSkippedTotal shouldBe 3L
    metrics.getRecordsWrittenTotal shouldBe 4L

    // Writer A still holds 100..102: they were not lost, just not re-buffered under C.
    wm.writerFor(MapKey(tp, pvA)).value.getFirstBufferedOffset shouldBe Some(Offset(100))
    wm.writerFor(MapKey(tp, pvC)).value.getFirstBufferedOffset shouldBe Some(Offset(103))
    storage.keysUnder(bucket, "data/") shouldBe empty
    wm.close()
  }

  test("[ND] the floor is per topic-partition, not per key: an interleaved offset is skipped under any key") {
    val metrics = new CloudSinkMetrics()
    val namer   = new RotatingKeyNamer(pvA)
    val fw      = new FailOnceAtOffset(-1L).formatWriter
    val wm      = buildWriterManager(indexManager, metrics, namer, fw, CommitMode.Batch)

    deliver(wm, 100L).value
    namer.current = pvB
    deliver(wm, 101L).value
    namer.current = pvA
    deliver(wm, 102L).value
    metrics.getRecordsWrittenTotal shouldBe 3L

    // 102 arrived under A; re-delivering it under B must still be skipped.
    namer.current = pvB
    deliver(wm, 102L).value

    metrics.getDuplicateRecordsSkippedTotal shouldBe 1L
    metrics.getRecordsWrittenTotal shouldBe 3L
    wm.writerFor(MapKey(tp, pvB)).value.getFirstBufferedOffset shouldBe Some(Offset(101))
    wm.close()
  }

  test("[NL] cleanUp(tp) resets the buffered floor so a rolled-back offset is written again") {
    val metrics = new CloudSinkMetrics()
    val namer   = new RotatingKeyNamer(pvA)
    val fw      = new FailOnceAtOffset(-1L).formatWriter
    val wm      = buildWriterManager(indexManager, metrics, namer, fw, CommitMode.Batch)

    (100L to 102L).foreach(o => deliver(wm, o).value)
    metrics.getRecordsWrittenTotal shouldBe 3L

    // Rollback: the buffered records are discarded, so nothing is "already held" any more.
    wm.cleanUp(tp)
    wm.writerCount shouldBe 0

    namer.current = pvC
    deliver(wm, 100L).value
    metrics.getRecordsWrittenTotal shouldBe 4L
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    wm.close()
  }

  test("[NL] close() resets the buffered floor so a re-delivered offset is written again") {
    val metrics = new CloudSinkMetrics()
    val namer   = new RotatingKeyNamer(pvA)
    val fw      = new FailOnceAtOffset(-1L).formatWriter
    val wm      = buildWriterManager(indexManager, metrics, namer, fw, CommitMode.Batch)

    (100L to 102L).foreach(o => deliver(wm, o).value)
    wm.close()
    wm.writerCount shouldBe 0

    namer.current = pvC
    deliver(wm, 100L).value
    metrics.getRecordsWrittenTotal shouldBe 4L
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    wm.close()
  }

  test("[B] granular mode is unchanged: the same rotation still re-buffers the already-held offsets") {
    indexManager.close()
    storage      = new InMemoryStorageInterface()
    indexManager = buildIndexManager(CommitMode.Granular)

    val metrics = new CloudSinkMetrics()
    val namer   = new RotatingKeyNamer(pvA)
    val fw      = new FailOnceAtOffset(103L).formatWriter
    val wm      = buildWriterManager(indexManager, metrics, namer, fw, CommitMode.Granular)

    (100L to 102L).foreach(o => deliver(wm, o).value)
    deliver(wm, 103L).left.value.rollBack() shouldBe false
    metrics.getRecordsWrittenTotal shouldBe 3L

    namer.current = pvC
    (100L to 103L).foreach(o => deliver(wm, o).value)

    // Key C's granular lock does not exist, so its floor is the master offset (99) and every
    // re-delivered offset is buffered a second time. This is the behaviour batch mode fixes.
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    metrics.getRecordsWrittenTotal shouldBe 7L
    wm.close()
  }
}
