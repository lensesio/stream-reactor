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
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
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
import io.lenses.streamreactor.connect.cloud.common.storage.StorageInterface
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.immutable

/**
 * Acceptance test for `commit.mode=batch`, parameterised on [[CommitMode]].
 *
 * Reproduces the "partial commit failure + restart inside the same window" scenario end to
 * end against a real `WriterManager`, a real `IndexManagerV2` and an in-memory storage, and
 * contrasts the granular outcome (which loses and duplicates data under a wall-clock
 * PARTITIONBY key) with the batch outcome (which does neither).
 *
 * Timeline under test:
 *   - W10 buffers 100..199 under partition key `date=...-10`, W11 buffers 200..250 under `date=...-11`.
 *   - A flush commits both. W10's upload fails transiently.
 *   - preCommit computes globalSafeOffset = 100 (W10's firstBufferedOffset).
 *   - Rebalance/close before recommitPending succeeds -> the staging files are deleted.
 *   - Restart seeks to the master `committedOffset` (99) and Kafka re-delivers from there.
 *
 * The granular and batch prefixes differ in exactly one observable way, and that difference
 * is the whole point of the mode: in granular mode W11's commit lands independently (its own
 * granular lock advances to 250 and its object reaches a final path) while W10's does not, so
 * the durable state after the flush is *partial*. In batch mode the commit point is a single
 * master-lock CAS, so a failure anywhere before it leaves *nothing* durable.
 *
 * Consequence for the batch expectations: because the batch prefix commits atomically, the
 * post-restart counters for the batch tests cannot be read off the granular prefix's partial
 * durable state. Those tests therefore assert the same *guarantees* (no loss, no duplication)
 * by first establishing a durable batch commit in the restart task and then replaying across a
 * key rotation.
 */
class BatchCommitScenarioTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("partial-commit-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)

  private val bucket        = "test-bucket"
  private val directoryName = ".indexes"
  private val tp            = Topic("orders").withPartition(0)
  // Interpolating `tp.topic` (not `tp.topic.value`) mirrors IndexManagerV2.generateLockFilePath
  // and Writer's temp-path construction exactly.
  private val masterLockPath  = s"$directoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}.lock"
  private val batchTempPrefix = s".temp-upload/${connectorTaskId.name}/${tp.topic}/${tp.partition}/"

  private val dateField: PartitionField = ValuePartitionField(PartitionNamePath("date"))

  private val pvHour10: immutable.Map[PartitionField, String] = Map(dateField -> "2026-09-10-10")
  private val pvHour11: immutable.Map[PartitionField, String] = Map(dateField -> "2026-09-10-11")
  private val pvHour12: immutable.Map[PartitionField, String] = Map(dateField -> "2026-09-10-12")
  private val pvHour13: immutable.Map[PartitionField, String] = Map(dateField -> "2026-09-10-13")

  private val k10 = WriterManager.derivePartitionKey(pvHour10).get
  private val k11 = WriterManager.derivePartitionKey(pvHour11).get

  private def bucketAndPrefix(topicPartition: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/")).asRight

  private def buildIndexManager(
    storage:    StorageInterface[FakeFileMetadata],
    commitMode: CommitMode,
  ): IndexManagerV2 = {
    implicit val si: StorageInterface[FakeFileMetadata] = storage
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = Int.MaxValue,
      gcSweepMinAgeSeconds        = Int.MaxValue,
      gcSweepEnabled              = false,
      commitMode                  = commitMode,
    )(si, connectorTaskId)
  }

  /**
   * `WriterManager` whose `KeyNamer` always derives `partitionValues` -- this is the SMT seam.
   * Passing a fixed map models a wall-clock SMT that stamps the same hour for every record it
   * sees during one task lifetime.
   */
  private def buildWriterManager(
    im:              IndexManagerV2,
    storage:         StorageInterface[FakeFileMetadata],
    metrics:         CloudSinkMetrics,
    partitionValues: immutable.Map[PartitionField, String],
    commitPolicy:    CommitPolicy,
    commitMode:      CommitMode,
  ): WriterManager[FakeFileMetadata] = {
    val keyNamer = mock[KeyNamer]
    when(keyNamer.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(partitionValues.asRight[SinkError])

    val formatWriter = mock[FormatWriter]
    when(formatWriter.write(any[MessageDetail])).thenReturn(().asRight)
    when(formatWriter.complete()).thenReturn(().asRight)
    when(formatWriter.rolloverFileOnSchemaChange()).thenReturn(false)

    new WriterManager[FakeFileMetadata](
      commitPolicyFn    = _ => commitPolicy.asRight,
      bucketAndPrefixFn = bucketAndPrefix,
      keyNamerFn        = _ => keyNamer.asRight,
      // The FormatWriter is a mock, so it never puts bytes on disk. Seed the staging file so it
      // passes `UploadableFile.validate`'s zero-byte guard, exactly as a real writer's would.
      stagingFilenameFn = (_, _) => tempFileWith("staged payload").asRight,
      objKeyBuilderFn = (_, pv) => {
        val okb = mock[ObjectKeyBuilder]
        when(okb.build(any[Offset], any[Offset], any[Long], any[Long], any[Long])).thenAnswer {
          (first: Offset, _: Offset, _: Long, _: Long, _: Long) =>
            val keySegment = WriterManager.derivePartitionKey(pv).getOrElse("nokey")
            CloudLocation(bucket,
                          path = Some(s"data/${tp.topic.value}/${tp.partition}/$keySegment-${first.value}.json"),
            ).asRight
        }
        okb
      },
      formatWriterFn              = (_, _) => formatWriter.asRight,
      indexManager                = im,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      commitMode                  = commitMode,
      metrics                     = metrics,
    )
  }

  private def buildBufferingWriter(
    im:           IndexManagerV2,
    storage:      StorageInterface[FakeFileMetadata],
    partitionKey: String,
    stagingFile:  File,
    finalPath:    String,
    formatWriter: FormatWriter,
  ): Writer[FakeFileMetadata] = {
    val objectKeyBuilder = mock[ObjectKeyBuilder]
    when(objectKeyBuilder.build(any[Offset], any[Offset], any[Long], any[Long], any[Long]))
      .thenReturn(CloudLocation(bucket, path = Some(finalPath)).asRight)

    val alwaysFlush = mock[CommitPolicy]
    when(alwaysFlush.shouldFlush(any[CommitContext])).thenReturn(true)

    new Writer[FakeFileMetadata](
      tp,
      alwaysFlush,
      im,
      stagingFilenameFn = () => stagingFile.asRight,
      objectKeyBuilder,
      formatWriterFn = _ => formatWriter.asRight,
      mock[SchemaChangeDetector],
      new PendingOperationsProcessors(storage),
      partitionKey     = Some(partitionKey),
      lastSeekedOffset = Some(Offset(99)),
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

  private def tempFileWith(payload: String): File = {
    val f = Files.createTempFile("partial-commit-", ".tmp").toFile
    Files.write(f.toPath, payload.getBytes(StandardCharsets.UTF_8))
    f.deleteOnExit()
    f
  }

  /** Fails `uploadFile` for one specific staging file; everything else behaves normally. */
  private final class FailUploadsFrom(val doomedFile: File) extends InMemoryStorageInterface {
    override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] =
      if (source.file.getAbsolutePath == doomedFile.getAbsolutePath)
        UploadFailedError(new RuntimeException("transient: connection reset"), doomedFile).asLeft
      else super.uploadFile(source, bucket, path)
  }

  /** A `CommitPolicy` whose verdict is driven by a mutable flag, so tests can flush on demand. */
  private final class TogglePolicy {
    val flush: AtomicBoolean = new AtomicBoolean(false)
    val policy: CommitPolicy = {
      val p = mock[CommitPolicy]
      when(p.shouldFlush(any[CommitContext])).thenAnswer((_: CommitContext) => flush.get())
      p
    }
  }

  /**
   * Drives the shared prefix of the scenario: partial commit, preCommit, rebalance close.
   * Returns the storage so the restart half can be run against the same durable state.
   */
  private def runUpToRebalance(commitMode: CommitMode): FailUploadsFrom = {
    val w10Staging = tempFileWith("offsets 100..199")
    val w11Staging = tempFileWith("offsets 200..250")
    val storage    = new FailUploadsFrom(w10Staging)

    val im = buildIndexManager(storage, commitMode)
    im.open(Set(tp)).value

    commitMode match {
      case CommitMode.Granular =>
        im.ensureGranularLock(tp, k10).value
        im.ensureGranularLock(tp, k11).value
      case CommitMode.Batch =>
        // Batch mode never writes the master lock from `preCommit`, so seed the durable floor
        // the previous owner would have left behind (globalSafeOffset 100 -> committedOffset 99).
        // The granular arm reaches the same 99 via `preCommit`'s `updateMasterLock`.
        im.updateMasterLock(tp, Offset(100)).value
    }

    val metrics     = new CloudSinkMetrics()
    val alwaysFlush = mock[CommitPolicy]
    when(alwaysFlush.shouldFlush(any[CommitContext])).thenReturn(true)
    val wm = buildWriterManager(im, storage, metrics, pvHour10, alwaysFlush, commitMode)

    val fw10 = mock[FormatWriter]
    when(fw10.complete()).thenReturn(().asRight)
    val fw11 = mock[FormatWriter]
    when(fw11.complete()).thenReturn(().asRight)

    val w10 = buildBufferingWriter(im, storage, k10, w10Staging, "data/orders/0/hour10-100.json", fw10)
    val w11 = buildBufferingWriter(im, storage, k11, w11Staging, "data/orders/0/hour11-200.json", fw11)

    w10.forceWriteState(
      Writing(
        CommitState(tp, Some(Offset(99))),
        fw10,
        w10Staging,
        firstBufferedOffset     = Offset(100),
        uncommittedOffset       = Offset(199),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 2L,
      ),
    )
    w11.forceWriteState(
      Writing(
        CommitState(tp, Some(Offset(99))),
        fw11,
        w11Staging,
        firstBufferedOffset     = Offset(200),
        uncommittedOffset       = Offset(250),
        earliestRecordTimestamp = 3L,
        latestRecordTimestamp   = 4L,
      ),
    )

    wm.putWriter(MapKey(tp, pvHour10), w10)
    wm.putWriter(MapKey(tp, pvHour11), w11)

    val masterBefore = storage.snapshot(bucket)(masterLockPath)

    // ── the flush: W10's upload fails transiently ─────────────────────────────────────
    val flushResult = wm.commitFlushableWriters()
    flushResult.isLeft shouldBe true
    val batch = flushResult.left.value.asInstanceOf[BatchCloudSinkError]
    batch.fatal shouldBe empty
    // rollBack()==false is what keeps CloudSinkTask.handleErrors from calling cleanUp,
    // so W10's staging file survives for recommitPending.
    batch.rollBack() shouldBe false

    commitMode match {
      case CommitMode.Granular =>
        // W11 committed durably on its own granular lock; W10 is parked in Uploading.
        im.getSeekedOffsetForPartitionKey(tp, k11) shouldBe Right(Some(Offset(250)))
        im.getSeekedOffsetForPartitionKey(tp, k10) shouldBe Right(None)
        w10.currentWriteState shouldBe a[Uploading]
        w11.currentWriteState shouldBe a[NoWriter]
        storage.keysUnder(bucket, "data/orders/0/hour11").toList should have size 1

      case CommitMode.Batch =>
        // The single commit point is the master-lock CAS, which was never reached: the master
        // lock is byte-for-byte and eTag-for-eTag unchanged and nothing reached a final path.
        val masterAfter = storage.snapshot(bucket)(masterLockPath)
        masterAfter.eTag shouldBe masterBefore.eTag
        new String(masterAfter.bytes, StandardCharsets.UTF_8) shouldBe
          new String(masterBefore.bytes, StandardCharsets.UTF_8)
        w10.currentWriteState shouldBe a[Uploading]
        // W11 staged successfully: its object is under the connector-scoped batch temp prefix
        // and W10's is not.
        w11.hasPendingUpload shouldBe true
        storage.keysUnder(bucket, batchTempPrefix) should have size 1
        storage.keysUnder(bucket, "data/orders/0/hour11").toList shouldBe Nil
    }

    w10.hasPendingUpload shouldBe true
    w10Staging.exists() shouldBe true

    // ── preCommit: globalSafeOffset is pinned to W10's firstBufferedOffset ────────────
    val precommitted = wm.preCommit(Map(tp -> new OffsetAndMetadata(300)))
    precommitted(tp).offset() shouldBe 100L
    im.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(99))

    // ── rebalance before recommitPending succeeds ─────────────────────────────────────
    wm.close()
    w10Staging.exists() shouldBe false
    storage.keysUnder(bucket, "data/orders/0/hour10").toList shouldBe Nil

    im.close()
    storage
  }

  /** Opens a fresh manager pair over `storage` (a restart) and delivers `offsets` under `pv`. */
  private def restartAndDeliver(
    storage:    FailUploadsFrom,
    commitMode: CommitMode,
    pv:         immutable.Map[PartitionField, String],
    offsets:    Seq[Long],
    flushAtEnd: Boolean,
    expectSeek: Long = 99L,
  ): (CloudSinkMetrics, IndexManagerV2, WriterManager[FakeFileMetadata]) = {
    val im = buildIndexManager(storage, commitMode)
    im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(expectSeek)))

    val metrics = new CloudSinkMetrics()
    val policy  = new TogglePolicy
    val wm      = buildWriterManager(im, storage, metrics, pv, policy.policy, commitMode)

    offsets.foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    if (flushAtEnd) {
      policy.flush.set(true)
      wm.commitFlushableWriters().value
    }
    (metrics, im, wm)
  }

  test(
    "[B] granular: restart inside the same hour with a wall-clock key silently drops re-delivered 100..199 (DATA LOSS)",
  ) {
    val storage = runUpToRebalance(CommitMode.Granular)

    // Restart at 11:2x. The wall-clock SMT now stamps hour 11 for every record, so the
    // re-delivered 100..199 route to K11 instead of K10.
    val (metrics, im, wm) = restartAndDeliver(storage, CommitMode.Granular, pvHour11, 100L to 199L, flushAtEnd = false)

    metrics.getDuplicateRecordsSkippedTotal shouldBe 100L
    metrics.getRecordsWrittenTotal shouldBe 0L

    wm.close()
    im.close()
  }

  test(
    "[B] granular: restart after the hour boundary re-writes already-committed 200..250 (DUPLICATION)",
  ) {
    val storage = runUpToRebalance(CommitMode.Granular)

    // Restart after 12:00. The key is brand new, so the granular lock does not exist and
    // createWriter falls back to the master-lock floor of 99.
    val (metrics, im, wm) = restartAndDeliver(storage, CommitMode.Granular, pvHour12, 100L to 250L, flushAtEnd = false)

    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    metrics.getRecordsWrittenTotal shouldBe 151L

    wm.close()
    im.close()
  }

  test(
    "[B] granular: with a deterministic key the same failure sequence loses nothing and duplicates nothing",
  ) {
    val storage = runUpToRebalance(CommitMode.Granular)

    val (metrics10, im10, wm10) =
      restartAndDeliver(storage, CommitMode.Granular, pvHour10, 100L to 199L, flushAtEnd = false)
    metrics10.getDuplicateRecordsSkippedTotal shouldBe 0L
    metrics10.getRecordsWrittenTotal shouldBe 100L
    wm10.close()
    im10.close()

    val (metrics11, im11, wm11) =
      restartAndDeliver(storage, CommitMode.Granular, pvHour11, 200L to 250L, flushAtEnd = false)
    metrics11.getDuplicateRecordsSkippedTotal shouldBe 51L
    metrics11.getRecordsWrittenTotal shouldBe 0L
    wm11.close()
    im11.close()
  }

  test(
    "[NL] batch: restart inside the same hour with a wall-clock key writes the re-delivered 100..199 exactly once",
  ) {
    val storage = runUpToRebalance(CommitMode.Batch)

    val (metrics, im, wm) = restartAndDeliver(storage, CommitMode.Batch, pvHour11, 100L to 199L, flushAtEnd = true)

    metrics.getRecordsWrittenTotal shouldBe 100L
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    // Exactly one object covers the recovered range; nothing was silently dropped by a
    // per-key lock that happened to be ahead of the replay.
    storage.keysUnder(bucket, "data/") should have size 1
    im.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(199))

    wm.close()
    im.close()
  }

  test(
    "[ND] batch: after a durable batch commit a post-boundary restart skips 200..250 regardless of which key they route to",
  ) {
    val storage = runUpToRebalance(CommitMode.Batch)

    // Stage 1: the replay writes 100..250 once (nothing was durable, so nothing is skipped)
    // and the batch commit advances the master floor to 250.
    val (m1, im1, wm1) = restartAndDeliver(storage, CommitMode.Batch, pvHour12, 100L to 250L, flushAtEnd = true)
    m1.getRecordsWrittenTotal shouldBe 151L
    m1.getDuplicateRecordsSkippedTotal shouldBe 0L
    val objectsAfterFirstCommit = storage.keysUnder(bucket, "data/").size
    im1.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(250))
    wm1.close()
    im1.close()

    // Stage 2: a further restart after another key rotation re-delivers 200..250. The
    // topic-partition floor skips all 51 even though the key is brand new -- the granular
    // mode equivalent re-writes them.
    val im2 = buildIndexManager(storage, CommitMode.Batch)
    im2.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(250)))
    val m2  = new CloudSinkMetrics()
    val pol = new TogglePolicy
    val wm2 = buildWriterManager(im2, storage, m2, pvHour13, pol.policy, CommitMode.Batch)
    (200L to 250L).foreach(o => wm2.write(tp.withOffset(Offset(o)), message(o)).value)

    m2.getDuplicateRecordsSkippedTotal shouldBe 51L
    m2.getRecordsWrittenTotal shouldBe 0L
    storage.keysUnder(bucket, "data/").size shouldBe objectsAfterFirstCommit

    // 251 is genuinely new and is written.
    wm2.write(tp.withOffset(Offset(251)), message(251)).value
    m2.getRecordsWrittenTotal shouldBe 1L

    wm2.close()
    im2.close()
  }

  test(
    "[B] batch: with a deterministic key the same failure sequence loses nothing and duplicates nothing",
  ) {
    val storage = runUpToRebalance(CommitMode.Batch)

    val (metrics10, im10, wm10) =
      restartAndDeliver(storage, CommitMode.Batch, pvHour10, 100L to 199L, flushAtEnd = true)
    metrics10.getDuplicateRecordsSkippedTotal shouldBe 0L
    metrics10.getRecordsWrittenTotal shouldBe 100L
    val afterFirst = storage.keysUnder(bucket, "data/").size
    afterFirst shouldBe 1
    wm10.close()
    im10.close()

    // The first batch committed at 199, so this restart seeks there rather than to the seed floor.
    val (metrics11, im11, wm11) =
      restartAndDeliver(storage, CommitMode.Batch, pvHour11, 200L to 250L, flushAtEnd = true, expectSeek = 199L)
    metrics11.getDuplicateRecordsSkippedTotal shouldBe 0L
    metrics11.getRecordsWrittenTotal shouldBe 51L
    storage.keysUnder(bucket, "data/").size shouldBe afterFirst + 1
    wm11.close()
    im11.close()

    // Re-delivering the whole range now finds it durable and skips all of it.
    val im12 = buildIndexManager(storage, CommitMode.Batch)
    im12.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(250)))
    val m12 = new CloudSinkMetrics()
    val wm12 =
      buildWriterManager(im12, storage, m12, pvHour10, new TogglePolicy().policy, CommitMode.Batch)
    (100L to 250L).foreach(o => wm12.write(tp.withOffset(Offset(o)), message(o)).value)
    m12.getDuplicateRecordsSkippedTotal shouldBe 151L
    m12.getRecordsWrittenTotal shouldBe 0L
    storage.keysUnder(bucket, "data/").size shouldBe afterFirst + 1
    wm12.close()
    im12.close()
  }
}
