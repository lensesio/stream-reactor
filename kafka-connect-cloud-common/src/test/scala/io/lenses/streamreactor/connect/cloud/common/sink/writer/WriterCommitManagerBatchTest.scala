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
import io.circe.Encoder
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.BatchCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitContext
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.BatchCommitLocks
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CommitMode
import io.lenses.streamreactor.connect.cloud.common.sink.seek.CopyOperation
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexFile
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManagerV2
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectProtection
import io.lenses.streamreactor.connect.cloud.common.sink.seek.ObjectWithETag
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailDeleteAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailMoveAt
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailWriteAt
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import scala.collection.immutable
import scala.collection.mutable

/**
 * The partition-batch commit protocol: stage every non-idle writer on the topic-partition, record
 * one `PendingState` of `CopyOperation`s onto the master lock with a single eTag CAS, drive the
 * chain, finalise the writers, then delete the temps best-effort.
 *
 * The CAS is the only commit point. Everything before it is retryable with no durable trace, and
 * everything after it is recoverable from what the CAS recorded.
 */
class WriterCommitManagerBatchTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("batch-commit-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)

  private val bucket         = "test-bucket"
  private val directoryName  = ".indexes"
  private val tp             = Topic("orders").withPartition(0)
  private val masterLockPath = s"$directoryName/${connectorTaskId.name}/.locks/${tp.topic}/${tp.partition}.lock"
  private val batchTempRoot  = s".temp-upload/${connectorTaskId.name}/${tp.topic}/${tp.partition}/"

  private val keyField: PartitionField = ValuePartitionField(PartitionNamePath("k"))
  private def pv(name: String): immutable.Map[PartitionField, String] = Map(keyField -> name)

  private var storage: RecordingStorage = _

  override def beforeEach(): Unit = storage = new RecordingStorage

  /**
   * Records master-lock payloads and upload calls, and can fail one nominated staging file.
   *
   * Because every batch attempt stages under a fresh batch id (LC-349), a temp path is only known
   * once the attempt uploads it. `uploadETags` captures each temp's eTag at upload time, and
   * `failMoveOfTempFor` arms a one-shot `FailMoveAt` on the current attempt's temp for a final path.
   */
  private class RecordingStorage extends InMemoryStorageInterface {
    val uploads = new AtomicInteger(0)
    val failUploadFor:     AtomicReference[Option[File]]   = new AtomicReference(None)
    val failMoveOfTempFor: AtomicReference[Option[String]] = new AtomicReference(None)
    val uploadETags  = mutable.Map.empty[String, String]
    val masterWrites = mutable.ListBuffer.empty[IndexFile]

    override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] = {
      uploads.incrementAndGet()
      failUploadFor.get() match {
        case Some(doomed) if source.file.getAbsolutePath == doomed.getAbsolutePath =>
          UploadFailedError(new RuntimeException("transient: connection reset"), doomed).asLeft
        case _ =>
          val result = super.uploadFile(source, bucket, path)
          result.foreach { eTag =>
            uploadETags += path -> eTag
            failMoveOfTempFor.get() match {
              case Some(finalPath) if path.startsWith(".temp-upload/") && path.endsWith(s"/$finalPath") =>
                failMoveOfTempFor.set(None)
                val _ = arm(FailMoveAt(bucket, path))
              case _ =>
            }
          }
          result
      }
    }

    override def writeBlobToFile[O](
      bucket:           String,
      path:             String,
      objectProtection: ObjectProtection[O],
    )(
      implicit
      encoder: Encoder[O],
    ): Either[UploadError, ObjectWithETag[O]] = {
      val result = super.writeBlobToFile(bucket, path, objectProtection)
      if (result.isRight && path == masterLockPath) {
        objectProtection.wrappedObject match {
          case idx: IndexFile => masterWrites += idx
          case _ =>
        }
      }
      result
    }

    /** Copies the moved object back to its source slot, modelling a copy-then-delete rename. */
    protected def reinstate(oldBucket: String, oldPath: String, newBucket: String, newPath: String): Unit = {
      val _ = writeStringToFile(
        oldBucket,
        oldPath,
        io.lenses.streamreactor.connect.cloud.common.model.UploadableString(
          getBlobAsString(newBucket, newPath).getOrElse(""),
        ),
      )
    }
  }

  private def bucketAndPrefix(topicPartition: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/")).asRight

  private def buildIndexManager(seedMasterAt: Option[Long] = Some(100L)): IndexManagerV2 = {
    implicit val si: InMemoryStorageInterface = storage
    val im = new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = Int.MaxValue,
      gcSweepMinAgeSeconds        = Int.MaxValue,
      gcSweepEnabled              = false,
      commitMode                  = CommitMode.Batch,
    )(si, connectorTaskId)
    im.open(Set(tp)).value
    seedMasterAt.foreach(o => im.updateMasterLock(tp, Offset(o)).value)
    storage.masterWrites.clear()
    im
  }

  private def stagingWith(payload: String): File = {
    val f = Files.createTempFile("batch-commit-", ".tmp").toFile
    Files.write(f.toPath, payload.getBytes(StandardCharsets.UTF_8))
    f.deleteOnExit()
    f
  }

  private def alwaysFlush: CommitPolicy = {
    val p = mock[CommitPolicy]
    when(p.shouldFlush(any[CommitContext])).thenReturn(true)
    p
  }

  private def neverFlush: CommitPolicy = {
    val p = mock[CommitPolicy]
    when(p.shouldFlush(any[CommitContext])).thenReturn(false)
    p
  }

  private def buildWriter(
    im:           IndexManagerV2,
    key:          String,
    finalPath:    String,
    commitPolicy: CommitPolicy     = null,
    metrics:      CloudSinkMetrics = new CloudSinkMetrics(),
  ): Writer[FakeFileMetadata] = {
    val okb = mock[ObjectKeyBuilder]
    when(okb.build(any[Offset], any[Offset], any[Long], any[Long], any[Long]))
      .thenReturn(CloudLocation(bucket, path = Some(finalPath)).asRight)
    val fw = mock[FormatWriter]
    when(fw.complete()).thenReturn(().asRight)

    new Writer[FakeFileMetadata](
      tp,
      Option(commitPolicy).getOrElse(alwaysFlush),
      im,
      stagingFilenameFn = () => stagingWith("unused").asRight,
      okb,
      formatWriterFn = _ => fw.asRight,
      mock[SchemaChangeDetector],
      new PendingOperationsProcessors(storage),
      partitionKey     = Some(key),
      lastSeekedOffset = Some(Offset(99)),
      metrics          = metrics,
    )
  }

  private def writing(writer: Writer[FakeFileMetadata], file: File, first: Long, last: Long): Unit = {
    val fw = mock[FormatWriter]
    when(fw.complete()).thenReturn(().asRight)
    writer.forceWriteState(
      Writing(
        CommitState(tp, Some(Offset(99))),
        fw,
        file,
        firstBufferedOffset     = Offset(first),
        uncommittedOffset       = Offset(last),
        earliestRecordTimestamp = 1L,
        latestRecordTimestamp   = 2L,
      ),
    )
  }

  private def sourceOf(entries: (String, Writer[FakeFileMetadata])*): WriterSource[FakeFileMetadata] =
    new WriterSource[FakeFileMetadata] {
      private val all = entries.map { case (k, w) => MapKey(tp, pv(k)) -> w }.toList
      override def iterator: Iterator[(MapKey, Writer[FakeFileMetadata])] = all.iterator
      override def iteratorForTopicPartition(t: TopicPartition): Iterator[(MapKey, Writer[FakeFileMetadata])] =
        all.iterator.filter(_._1.topicPartition == t)
    }

  private def commitManager(
    im:         IndexManagerV2,
    source:     WriterSource[FakeFileMetadata],
    metrics:    CloudSinkMetrics = new CloudSinkMetrics(),
    commitMode: CommitMode       = CommitMode.Batch,
  ): WriterCommitManager[FakeFileMetadata] =
    new WriterCommitManager[FakeFileMetadata](
      source,
      im,
      new PendingOperationsProcessors(storage),
      commitMode,
      metrics,
    )

  private def masterLock(): IndexFile =
    storage.getBlobAsObject[IndexFile](bucket, masterLockPath).value.wrappedObject

  private def finalKeys(): Seq[String] = storage.keysUnder(bucket, "data/")
  private def tempKeys():  Seq[String] = storage.keysUnder(bucket, batchTempRoot)

  test("[NL] a batch of two Writing writers commits both files and advances the master to the batch offset") {
    val im      = buildIndexManager()
    val metrics = new CloudSinkMetrics()
    val fileA   = stagingWith("A: 100..199")
    val fileB   = stagingWith("B: 200..250")
    val wA      = buildWriter(im, "A", "data/orders/0/a.json")
    val wB      = buildWriter(im, "B", "data/orders/0/b.json")
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)

    val result = commitManager(im, sourceOf("A" -> wA, "B" -> wB), metrics).commitBatch(tp)

    result.value shouldBe (())
    finalKeys() should contain theSameElementsAs Seq("data/orders/0/a.json", "data/orders/0/b.json")
    masterLock().committedOffset shouldBe Some(Offset(250))
    masterLock().pendingState shouldBe None
    wA.currentWriteState shouldBe a[NoWriter]
    wB.currentWriteState shouldBe a[NoWriter]
    wA.getCommittedOffset shouldBe Some(Offset(250))
    wB.getCommittedOffset shouldBe Some(Offset(250))
    tempKeys() shouldBe empty
    fileA.exists() shouldBe false
    fileB.exists() shouldBe false
    metrics.getBatchCommitsTotal shouldBe 1L
    metrics.getBatchCommitFilesTotal shouldBe 2L
    im.close()
  }

  test("[ND] the CAS records exactly one Copy per staged writer, each pinned to that temp's eTag") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val fileB = stagingWith("B")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json")
    val wB    = buildWriter(im, "B", "data/orders/0/b.json")
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)

    val cm = commitManager(im, sourceOf("A" -> wA, "B" -> wB))

    // The temp eTags are captured at upload time (RecordingStorage.uploadETags), before the copies
    // move the temps away.
    cm.commitBatch(tp).value

    val casPayload = storage.masterWrites.head
    casPayload.committedOffset shouldBe Some(Offset(99))
    val pending = casPayload.pendingState.value
    pending.pendingOffset shouldBe Offset(250)
    val ops = pending.pendingOperations.toList
    ops should have size 2
    ops.foreach(_ shouldBe a[CopyOperation])
    val copies = ops.collect { case c: CopyOperation => c }
    copies.map(_.destination) should contain theSameElementsAs Seq("data/orders/0/a.json", "data/orders/0/b.json")
    copies.foreach(c => c.source should startWith(batchTempRoot))
    copies.foreach(c => c.eTag shouldBe storage.uploadETags(c.source))
    im.close()
  }

  test("[NL] the CAS committedOffset comes from the master lock, never from a writer's stale state") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json")
    writing(wA, fileA, 100, 199)
    // An idle sibling carrying a stale committed offset must not influence the CAS payload.
    val stale = buildWriter(im, "STALE", "data/orders/0/stale.json")
    stale.forceWriteState(NoWriter(CommitState(tp, Some(Offset(42)))))

    commitManager(im, sourceOf("A" -> wA, "STALE" -> stale)).commitBatch(tp).value

    storage.masterWrites.head.committedOffset shouldBe Some(Offset(99))
    im.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(199))
    // The idle writer was never staged, so no object was produced for it.
    finalKeys() shouldBe Seq("data/orders/0/a.json")
    im.close()
  }

  test("[NL] a staging failure before the CAS leaves the master lock and every final path untouched") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val fileB = stagingWith("B")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json")
    val wB    = buildWriter(im, "B", "data/orders/0/b.json")
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)
    storage.failUploadFor.set(Some(fileB))

    val masterBefore = storage.snapshot(bucket)(masterLockPath)
    val metrics      = new CloudSinkMetrics()
    val result       = commitManager(im, sourceOf("A" -> wA, "B" -> wB), metrics).commitBatch(tp)

    val batch = result.left.value.asInstanceOf[BatchCloudSinkError]
    batch.fatal shouldBe empty
    batch.rollBack() shouldBe false
    val masterAfter = storage.snapshot(bucket)(masterLockPath)
    masterAfter.eTag shouldBe masterBefore.eTag
    new String(masterAfter.bytes, StandardCharsets.UTF_8) shouldBe
      new String(masterBefore.bytes, StandardCharsets.UTF_8)
    wA.hasPendingUpload shouldBe true
    wB.currentWriteState shouldBe a[Uploading]
    fileA.exists() shouldBe true
    fileB.exists() shouldBe true
    tempKeys() should have size 1
    finalKeys() shouldBe empty
    metrics.getBatchCommitFailuresTotal shouldBe 1L
    im.close()
  }

  test("[ND] the retry after a staging failure re-stages every writer under the new batch id (LC-349)") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val fileB = stagingWith("B")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json")
    val wB    = buildWriter(im, "B", "data/orders/0/b.json")
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)
    storage.failUploadFor.set(Some(fileB))

    val cm = commitManager(im, sourceOf("A" -> wA, "B" -> wB))
    cm.commitBatch(tp).isLeft shouldBe true
    val uploadsAfterFirst = storage.uploads.get()
    val aTempAfterFirst   = tempKeys().head

    storage.failUploadFor.set(None)
    cm.commitBatch(tp).value

    // Both writers were uploaded under the retry's batch id: A's first temp is unreferenced and the
    // sweep may have deleted it, so the retry must not trust it (LC-349).
    storage.uploads.get() shouldBe (uploadsAfterFirst + 2)
    val copies = storage.masterWrites.head.pendingState.value.pendingOperations.toList
      .collect { case c: CopyOperation => c }
    copies.map(_.source) should not contain aTempAfterFirst
    copies.map(_.source.stripPrefix(batchTempRoot).takeWhile(_ != '/')).distinct should have size 1
    copies.foreach(c => c.eTag shouldBe storage.uploadETags(c.source))
    // The destinations are frozen across the retry.
    copies.map(_.destination) should contain theSameElementsAs Seq("data/orders/0/a.json", "data/orders/0/b.json")
    finalKeys() should contain theSameElementsAs Seq("data/orders/0/a.json", "data/orders/0/b.json")
    // The retry's temps were moved; only A's first, now orphaned, temp is left for the sweep.
    tempKeys() shouldBe Seq(aTempAfterFirst)
    im.close()
  }

  // LC-349: the orphan sweep may delete a Staged writer's temp between attempts, because nothing
  // references it until the CAS. The retry must re-stage it rather than record a dangling Copy.
  test("[NL] the retry after a staging failure commits both files even when the sweep deleted A's temp (LC-349)") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A-bytes")
    val fileB = stagingWith("B-bytes")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json")
    val wB    = buildWriter(im, "B", "data/orders/0/b.json")
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)
    storage.failUploadFor.set(Some(fileB))

    val cm = commitManager(im, sourceOf("A" -> wA, "B" -> wB))
    cm.commitBatch(tp).isLeft shouldBe true
    val aTempAfterFirst = tempKeys()
    aTempAfterFirst should have size 1

    // The sweep reaps A's unreferenced temp between the two attempts.
    storage.deleteFiles(bucket, aTempAfterFirst).value
    tempKeys() shouldBe empty

    storage.failUploadFor.set(None)
    cm.commitBatch(tp).value

    finalKeys() should contain theSameElementsAs Seq("data/orders/0/a.json", "data/orders/0/b.json")
    storage.getBlobAsString(bucket, "data/orders/0/a.json").value shouldBe "A-bytes"
    storage.getBlobAsString(bucket, "data/orders/0/b.json").value shouldBe "B-bytes"
    masterLock().pendingState shouldBe None
    masterLock().committedOffset shouldBe Some(Offset(250))
    wA.currentWriteState shouldBe a[NoWriter]
    wB.currentWriteState shouldBe a[NoWriter]
    tempKeys() shouldBe empty
    fileA.exists() shouldBe false
    fileB.exists() shouldBe false
    im.close()
  }

  test("[NL] a failed CAS is Fatal, changes nothing durable, and a restart replays from the old floor") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val fileB = stagingWith("B")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json")
    val wB    = buildWriter(im, "B", "data/orders/0/b.json")
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)

    val masterBefore = storage.snapshot(bucket)(masterLockPath)
    storage.arm(FailWriteAt(bucket, masterLockPath))

    val result = commitManager(im, sourceOf("A" -> wA, "B" -> wB)).commitBatch(tp)

    result.left.value shouldBe a[FatalCloudSinkError]
    wA.hasPendingUpload shouldBe true
    wB.hasPendingUpload shouldBe true
    tempKeys() should have size 2
    fileA.exists() shouldBe true
    fileB.exists() shouldBe true
    finalKeys() shouldBe empty
    val masterAfter = storage.snapshot(bucket)(masterLockPath)
    new String(masterAfter.bytes, StandardCharsets.UTF_8) shouldBe
      new String(masterBefore.bytes, StandardCharsets.UTF_8)
    im.close()

    // Recovery: a restart seeks the unchanged floor and sees no pending work.
    val restarted = buildIndexManager(seedMasterAt = None)
    restarted.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(99))
    masterLock().pendingState shouldBe None
    restarted.close()
  }

  test("[NL] a mid-chain copy failure is Fatal and the recorded remaining ops let a restart finish the batch") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val fileB = stagingWith("B")
    val fileC = stagingWith("C")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json")
    val wB    = buildWriter(im, "B", "data/orders/0/b.json")
    val wC    = buildWriter(im, "C", "data/orders/0/c.json")
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)
    writing(wC, fileC, 251, 300)

    // Fail the copy of the middle writer's temp from the current attempt.
    storage.failMoveOfTempFor.set(Some("data/orders/0/b.json"))

    val result = commitManager(im, sourceOf("A" -> wA, "B" -> wB, "C" -> wC)).commitBatch(tp)

    result.left.value shouldBe a[FatalCloudSinkError]
    finalKeys() shouldBe Seq("data/orders/0/a.json")
    val pending = masterLock().pendingState.value
    masterLock().committedOffset shouldBe Some(Offset(99))
    pending.pendingOperations.toList.collect { case cp: CopyOperation => cp.destination } shouldBe
      List("data/orders/0/b.json", "data/orders/0/c.json")
    wB.hasPendingUpload shouldBe true
    wC.hasPendingUpload shouldBe true
    im.close()

    // Recovery: open() resolves the recorded chain and every final path ends up written once.
    val restarted = buildIndexManager(seedMasterAt = None)
    restarted.getSeekedOffsetForTopicPartition(tp) shouldBe Some(Offset(300))
    masterLock().pendingState shouldBe None
    finalKeys() should contain theSameElementsAs
      Seq("data/orders/0/a.json", "data/orders/0/b.json", "data/orders/0/c.json")
    restarted.close()
  }

  test(
    "[NL] a last-copy failure is Fatal; a restart (IndexManagerV2.open) resumes the chain without re-uploading",
  ) {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json")
    writing(wA, fileA, 100, 199)

    // Fail the copy of A's temp from the current attempt.
    storage.failMoveOfTempFor.set(Some("data/orders/0/a.json"))

    val cm                = commitManager(im, sourceOf("A" -> wA))
    val first             = cm.commitBatch(tp)
    val uploadsAfterStage = storage.uploads.get()
    // A tail CopyOperation failure is Fatal (previously NonFatal) -- the data is already
    // durable at the temp and the lock's PendingState still references it, so recovery is via
    // IndexManagerV2.open() on restart, not an in-process recommitPending retry on the same task.
    first.left.value.rollBack() shouldBe true
    masterLock().pendingState should not be None
    im.close()

    // Restart: a fresh open() resumes the chain (the FailMoveAt hook was one-shot, so the mvFile
    // retry succeeds without re-uploading anything).
    val im2 = buildIndexManager(seedMasterAt = None)
    im2.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(199)))
    storage.uploads.get() shouldBe uploadsAfterStage
    finalKeys() shouldBe Seq("data/orders/0/a.json")
    masterLock().pendingState shouldBe None
    im2.close()
  }

  test("[B] a batch with only idle writers is a no-op with no storage calls and no CAS") {
    val im   = buildIndexManager()
    val idle = buildWriter(im, "A", "data/orders/0/a.json")
    idle.forceWriteState(NoWriter(CommitState(tp, Some(Offset(99)))))
    val before = storage.snapshot(bucket)

    commitManager(im, sourceOf("A" -> idle)).commitBatch(tp).value shouldBe (())

    storage.uploads.get() shouldBe 0
    storage.masterWrites shouldBe empty
    storage.snapshot(bucket).keySet shouldBe before.keySet
    im.close()
  }

  test("[ND] a post-commit temp delete failure does not fail the batch; the temp becomes sweep work") {
    // A provider whose rename is a server-side copy leaves the source object behind, so the
    // post-commit cleanup delete has something to fail on. That failure is pure hygiene: the
    // bytes are already at their final path and the offset is already committed.
    storage = new RecordingStorage {
      override def mvFile(
        oldBucket: String,
        oldPath:   String,
        newBucket: String,
        newPath:   String,
        maybeEtag: Option[String],
      ): Either[io.lenses.streamreactor.connect.cloud.common.storage.FileMoveError, Unit] = {
        val copied = super.mvFile(oldBucket, oldPath, newBucket, newPath, maybeEtag)
        copied.foreach(_ => reinstate(oldBucket, oldPath, newBucket, newPath))
        copied
      }
    }
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json")
    writing(wA, fileA, 100, 199)

    val stagedA = wA.stage("probe").value.value
    storage.arm(FailDeleteAt(bucket, stagedA.tempPath))

    commitManager(im, sourceOf("A" -> wA)).commitBatch(tp).value shouldBe (())

    wA.currentWriteState shouldBe a[NoWriter]
    finalKeys() shouldBe Seq("data/orders/0/a.json")
    masterLock().pendingState shouldBe None
    masterLock().committedOffset shouldBe Some(Offset(199))
    // The temp survived the failed cleanup and is now an orphan for the sweep to reap.
    storage.snapshot(bucket).keys should contain(stagedA.tempPath)
    im.close()
  }

  test("[B] batch routing pulls every writer on the topic-partition into the commit") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val fileB = stagingWith("B")
    // Only A is flushable; B keeps buffering under granular rules but must join the batch.
    val wA = buildWriter(im, "A", "data/orders/0/a.json", commitPolicy = alwaysFlush)
    val wB = buildWriter(im, "B", "data/orders/0/b.json", commitPolicy = neverFlush)
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)

    commitManager(im, sourceOf("A" -> wA, "B" -> wB)).commitFlushableWritersForTopicPartition(tp).value

    finalKeys() should contain theSameElementsAs Seq("data/orders/0/a.json", "data/orders/0/b.json")
    masterLock().committedOffset shouldBe Some(Offset(250))
    im.close()
  }

  test("[B] commitPending and commitForTopicPartition also commit the whole topic-partition batch") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val fileB = stagingWith("B")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json", commitPolicy = neverFlush)
    val wB    = buildWriter(im, "B", "data/orders/0/b.json", commitPolicy = neverFlush)
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)
    // A is parked mid-upload, B is still buffering: commitPending must take both.
    wA.stage("probe").value.value

    commitManager(im, sourceOf("A" -> wA, "B" -> wB)).commitPending().value

    finalKeys() should contain theSameElementsAs Seq("data/orders/0/a.json", "data/orders/0/b.json")

    val fileC = stagingWith("C")
    val wC    = buildWriter(im, "C", "data/orders/0/c.json", commitPolicy = neverFlush)
    writing(wC, fileC, 251, 300)
    commitManager(im, sourceOf("C" -> wC)).commitForTopicPartition(tp).value
    finalKeys() should contain("data/orders/0/c.json")
    im.close()
  }

  test("[B] granular mode routes to Writer.commit and never stages a writer") {
    val im    = buildIndexManager()
    val fileA = stagingWith("A")
    val wA    = buildWriter(im, "A", "data/orders/0/a.json", commitPolicy = alwaysFlush)
    val wB    = buildWriter(im, "B", "data/orders/0/b.json", commitPolicy = neverFlush)
    val fileB = stagingWith("B")
    writing(wA, fileA, 100, 199)
    writing(wB, fileB, 200, 250)
    im.ensureGranularLock(tp, "A").value

    commitManager(im, sourceOf("A" -> wA, "B" -> wB), commitMode = CommitMode.Granular)
      .commitFlushableWritersForTopicPartition(tp).value

    // Granular selective commit: only A committed, and it went through the Upload/Copy/Delete
    // chain, so nothing is left under the batch temp prefix.
    finalKeys() shouldBe Seq("data/orders/0/a.json")
    wB.currentWriteState shouldBe a[Writing]
    storage.keysUnder(bucket, batchTempRoot) shouldBe empty
    im.close()
  }

  // A duplicate test ("preCommit while a batch is staged returns the earliest buffered offset,
  // not the batch offset") was removed: it was the same scenario as the equivalent test in
  // WriterManagerPreCommitTest but at the wrong layer (a real WriterManager built inline inside
  // a WriterCommitManager-focused test file) and missing the `never updateMasterLock` assertion.

  test("[M'3] a sweep started while the batch is staging leaves A's fresh temp alone; the commit completes first") {
    // LC-349 M': the periodic sweep runs with min age 0 while B is uploading, after A has staged.
    // A's temp is listed and unreferenced (the CAS has not happened yet), so only the partition
    // lock held by commitBatch keeps the sweep from deleting it.
    val locks               = BatchCommitLocks()
    val lock                = locks.lockFor(tp)
    val sweepFinished       = new CountDownLatch(1)
    val sweepEscaped        = new AtomicReference[Option[Throwable]](None)
    val finalsAtSweepDelete = new AtomicReference[Option[Seq[String]]](None)
    val imRef               = new AtomicReference[IndexManagerV2]()

    val sweepThread = new Thread(() =>
      try imRef.get().sweepAllBatchTemps()
      catch { case t: Throwable => sweepEscaped.set(Some(t)) }
      finally sweepFinished.countDown(),
    )

    storage = new RecordingStorage {
      private val bUploadHookArmed = new java.util.concurrent.atomic.AtomicBoolean(true)

      override def uploadFile(source: UploadableFile, b: String, path: String): Either[UploadError, String] = {
        if (
          path.startsWith(batchTempRoot) && path.endsWith("/data/orders/0/b.json") && bUploadHookArmed.getAndSet(false)
        ) {
          // Only START the sweep and wait until it is either blocked on the partition lock or
          // done. Never wait for it to finish: commitBatch holds the lock, so that would deadlock.
          sweepThread.start()
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
          while (!lock.hasQueuedThreads && sweepFinished.getCount > 0) {
            if (System.nanoTime() > deadline) fail("deadlock guard: sweep neither queued nor finished")
            LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100))
          }
        }
        super.uploadFile(source, b, path)
      }

      override def deleteFiles(b: String, files: Seq[String]): Either[FileDeleteError, Unit] = {
        if (files.exists(_.startsWith(batchTempRoot))) finalsAtSweepDelete.compareAndSet(None, Some(finalKeys()))
        super.deleteFiles(b, files)
      }
    }

    val im = {
      implicit val si: InMemoryStorageInterface = storage
      new IndexManagerV2(
        bucketAndPrefixFn           = bucketAndPrefix,
        pendingOperationsProcessors = new PendingOperationsProcessors(storage),
        directoryFileName           = directoryName,
        gcIntervalSeconds           = Int.MaxValue,
        gcSweepIntervalSeconds      = 0,
        gcSweepMinAgeSeconds        = 0,
        gcSweepEnabled              = false,
        commitMode                  = CommitMode.Batch,
        batchCommitLocks            = locks,
      )(si, connectorTaskId)
    }
    imRef.set(im)
    try {
      im.open(Set(tp)).value
      im.updateMasterLock(tp, Offset(100L)).value

      val fileA = stagingWith("A: 100..199")
      val fileB = stagingWith("B: 200..250")
      val wA    = buildWriter(im, "A", "data/orders/0/a.json")
      val wB    = buildWriter(im, "B", "data/orders/0/b.json")
      writing(wA, fileA, 100, 199)
      writing(wB, fileB, 200, 250)

      val cm = new WriterCommitManager[FakeFileMetadata](
        sourceOf("A" -> wA, "B" -> wB),
        im,
        new PendingOperationsProcessors(storage),
        CommitMode.Batch,
        new CloudSinkMetrics(),
        locks,
      )
      val result = cm.commitBatch(tp)

      sweepFinished.await(10, TimeUnit.SECONDS) shouldBe true
      sweepEscaped.get() shouldBe None
      result.value shouldBe (())
      finalKeys() should contain theSameElementsAs Seq("data/orders/0/a.json", "data/orders/0/b.json")
      masterLock().committedOffset shouldBe Some(Offset(250))
      masterLock().pendingState shouldBe None
      // The sweep's delete ran only after the commit had landed both files.
      finalsAtSweepDelete.get().value should contain theSameElementsAs Seq(
        "data/orders/0/a.json",
        "data/orders/0/b.json",
      )
    } finally im.close()
  }
}
