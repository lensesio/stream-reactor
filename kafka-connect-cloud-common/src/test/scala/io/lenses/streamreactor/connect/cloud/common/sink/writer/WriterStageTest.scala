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
import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.UploadableFile
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.FatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.storage.NonExistingFileError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.storage.UploadFailedError
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.OptionValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

/**
 * Unit coverage for the `Staged` write state and the `stage` / `finalizeCommit` pair that
 * partition-batch commit is built on. `stage` performs the upload half of a commit (staging file
 * -> connector-scoped temp object) and records everything the later `CopyOperation` needs; the
 * master-lock CAS and the copy itself happen in `WriterCommitManager.commitBatch`.
 */
class WriterStageTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("stage-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => cats.data.Validated.valid(location)

  private val bucket    = "test-bucket"
  private val tp        = Topic("topic-x").withPartition(0)
  private val finalPath = "data/topic-x/0/final-100.jsonl"
  private val batchUuid = "b0000000-0000-0000-0000-00000000000f"

  private def tempPrefix(uuid: String = batchUuid) =
    s".temp-upload/${connectorTaskId.name}/${tp.topic}/${tp.partition}/$uuid/"

  private def stagingFile(payload: String = "buffered records"): File = {
    val f = Files.createTempFile("writer-stage-", ".tmp").toFile
    Files.write(f.toPath, payload.getBytes(StandardCharsets.UTF_8))
    f.deleteOnExit()
    f
  }

  /** Counts `uploadFile` invocations so "no storage call" can be asserted directly. */
  private class CountingStorage extends InMemoryStorageInterface {
    val uploads = new AtomicInteger(0)
    override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] = {
      uploads.incrementAndGet()
      super.uploadFile(source, bucket, path)
    }
  }

  /** Records every uploadFile destination path so temp-path shapes can be asserted. */
  private class PathCapturingStorage extends InMemoryStorageInterface {
    @volatile var uploadPaths: List[String] = Nil
    override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] = {
      uploadPaths = uploadPaths :+ path
      super.uploadFile(source, bucket, path)
    }
  }

  private class FailingUploadStorage(err: File => UploadError) extends CountingStorage {
    override def uploadFile(source: UploadableFile, bucket: String, path: String): Either[UploadError, String] = {
      uploads.incrementAndGet()
      err(source.file).asLeft
    }
  }

  private def buildWriter(
    storage:      InMemoryStorageInterface,
    file:         File,
    formatWriter: FormatWriter     = null,
    metrics:      CloudSinkMetrics = new CloudSinkMetrics(),
    keyPath:      Option[String]   = Some(finalPath),
  ): Writer[FakeFileMetadata] = {
    val idx = mock[IndexManager]
    when(idx.indexingEnabled).thenReturn(true)
    // Echo index updates so a granular Writer.commit chain can run to completion.
    when(idx.updateForPartitionKey(any[TopicPartition], any[String], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, _: String, co: Option[Offset], _: Option[PendingState]) => co.asRight[SinkError])
    when(idx.update(any[TopicPartition], any[Option[Offset]], any[Option[PendingState]]))
      .thenAnswer((_: TopicPartition, co: Option[Offset], _: Option[PendingState]) => co.asRight[SinkError])

    val okb = mock[ObjectKeyBuilder]
    when(okb.build(any[Offset], any[Offset], any[Long], any[Long], any[Long]))
      .thenReturn(CloudLocation(bucket, path = keyPath).asRight)

    val fw = Option(formatWriter).getOrElse {
      val m = mock[FormatWriter]
      when(m.complete()).thenReturn(().asRight)
      m
    }

    new Writer[FakeFileMetadata](
      tp,
      mock[CommitPolicy],
      idx,
      stagingFilenameFn = () => file.asRight,
      okb,
      formatWriterFn = _ => fw.asRight,
      mock[SchemaChangeDetector],
      new PendingOperationsProcessors(storage),
      partitionKey     = Some("date=2026-09-10"),
      lastSeekedOffset = Some(Offset(99)),
      metrics          = metrics,
    )
  }

  private def writingState(file: File, fw: FormatWriter): Writing =
    Writing(
      CommitState(tp, Some(Offset(99))),
      fw,
      file,
      firstBufferedOffset     = Offset(100),
      uncommittedOffset       = Offset(150),
      earliestRecordTimestamp = 1L,
      latestRecordTimestamp   = 100L,
    )

  private def uploadingState(file: File): Uploading =
    Uploading(
      CommitState(tp, Some(Offset(99))),
      file,
      firstBufferedOffset     = Offset(100),
      uncommittedOffset       = Offset(150),
      earliestRecordTimestamp = 1L,
      latestRecordTimestamp   = 100L,
      recordCount             = 7L,
    )

  test("[ND] stage on a Writing writer completes the format writer once and uploads exactly one temp object") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val fw      = mock[FormatWriter]
    when(fw.complete()).thenReturn(().asRight)
    val writer = buildWriter(storage, file, fw)
    writer.forceWriteState(writingState(file, fw))

    val staged = writer.stage(batchUuid).value.value

    verify(fw, times(1)).complete()
    storage.uploads.get() shouldBe 1
    val keys = storage.keysUnder(bucket, tempPrefix())
    keys should have size 1
    staged.tempPath shouldBe keys.head
    staged.tempETag shouldBe storage.snapshot(bucket)(keys.head).eTag
    staged.finalPath shouldBe finalPath
    staged.bucket shouldBe bucket
    staged.firstBufferedOffset shouldBe Offset(100)
    staged.uncommittedOffset shouldBe Offset(150)
    writer.currentWriteState shouldBe staged
  }

  test("[ND] the staged CopyOperation is built from the staged fields") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val writer  = buildWriter(storage, file)
    val fw      = mock[FormatWriter]
    when(fw.complete()).thenReturn(().asRight)
    writer.forceWriteState(writingState(file, fw))

    val staged = writer.stage(batchUuid).value.value

    staged.copyOp.bucket shouldBe bucket
    staged.copyOp.source shouldBe staged.tempPath
    staged.copyOp.destination shouldBe finalPath
    staged.copyOp.eTag shouldBe staged.tempETag
  }

  test("[ND] stage on an already-Staged writer is idempotent and performs no storage call") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val fw      = mock[FormatWriter]
    when(fw.complete()).thenReturn(().asRight)
    val writer = buildWriter(storage, file, fw)
    writer.forceWriteState(writingState(file, fw))

    val first = writer.stage(batchUuid).value.value
    storage.uploads.get() shouldBe 1

    val second = writer.stage("a-different-batch-uuid").value.value

    storage.uploads.get() shouldBe 1
    second shouldBe first
    storage.keysUnder(bucket, s".temp-upload/") should have size 1
  }

  test("[NL] stage on an Uploading writer (a previously failed upload) uploads and keeps the local file") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val writer  = buildWriter(storage, file)
    writer.forceWriteState(uploadingState(file))

    val staged = writer.stage(batchUuid).value.value

    storage.uploads.get() shouldBe 1
    storage.keysUnder(bucket, tempPrefix()) should have size 1
    staged.recordCount shouldBe 7L
    file.exists() shouldBe true
  }

  test("[NL] a transient stage upload failure is NonFatal, leaves the writer Uploading and writes nothing") {
    val storage = new FailingUploadStorage(f => UploadFailedError(new RuntimeException("connection reset"), f))
    val file    = stagingFile()
    val writer  = buildWriter(storage, file)
    writer.forceWriteState(uploadingState(file))

    val err = writer.stage(batchUuid).left.value

    err shouldBe a[NonFatalCloudSinkError]
    err.rollBack() shouldBe false
    writer.currentWriteState shouldBe a[Uploading]
    file.exists() shouldBe true
    storage.keysUnder(bucket, ".temp-upload/") shouldBe empty

    // Recovery: the retry succeeds against a healthy storage and produces the temp object.
    val healthy    = new CountingStorage
    val recovering = buildWriter(healthy, file)
    recovering.forceWriteState(uploadingState(file))
    recovering.stage(batchUuid).value.value
    healthy.keysUnder(bucket, tempPrefix()) should have size 1
  }

  test("[NL] stage with a missing staging file is Fatal, rolls back and names the staging path") {
    val storage = new FailingUploadStorage(f => NonExistingFileError(f))
    val missing = new File("/nonexistent/stage-test/staging.tmp")
    missing.exists() shouldBe false
    val writer = buildWriter(storage, missing)
    writer.forceWriteState(uploadingState(missing))

    val err = writer.stage(batchUuid).left.value

    err shouldBe a[FatalCloudSinkError]
    err.rollBack() shouldBe true
    err.topicPartitions() shouldBe Set(tp)
    err.message() should include(missing.getAbsolutePath)
    storage.keysUnder(bucket, ".temp-upload/") shouldBe empty

    // Recovery: cleanUp/close disposes the writer and the restart replays from the master floor.
    writer.close()
    writer.currentWriteState shouldBe a[NoWriter]
  }

  test("[NL] stage on Writing escalates an IOException-caused complete() failure to Fatal") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val fw      = mock[FormatWriter]
    when(fw.complete()).thenReturn(
      NonFatalCloudSinkError("disk full", new IOException("No space left on device").some).asLeft,
    )
    val writer = buildWriter(storage, file, fw)
    writer.forceWriteState(writingState(file, fw))

    val err = writer.stage(batchUuid).left.value

    err shouldBe a[FatalCloudSinkError]
    err.rollBack() shouldBe true
    storage.uploads.get() shouldBe 0
    storage.keysUnder(bucket, ".temp-upload/") shouldBe empty
  }

  test("[NL] stage on Writing keeps a non-IOException complete() failure NonFatal") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val fw      = mock[FormatWriter]
    when(fw.complete()).thenReturn(
      NonFatalCloudSinkError("bad schema", new IllegalArgumentException("nope").some).asLeft,
    )
    val writer = buildWriter(storage, file, fw)
    writer.forceWriteState(writingState(file, fw))

    val err = writer.stage(batchUuid).left.value

    err shouldBe a[NonFatalCloudSinkError]
    err.rollBack() shouldBe false
    storage.uploads.get() shouldBe 0
  }

  test("[B] stage on a NoWriter writer returns Right(None) and performs no storage call") {
    val storage = new CountingStorage
    val writer  = buildWriter(storage, stagingFile())

    writer.stage(batchUuid).value shouldBe None

    storage.uploads.get() shouldBe 0
    storage.snapshot(bucket) shouldBe empty
  }

  test("[NL] finalizeCommit moves the writer to NoWriter at the batch offset, deletes the local file") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val metrics = new CloudSinkMetrics()
    val writer  = buildWriter(storage, file, metrics = metrics)
    writer.forceWriteState(uploadingState(file))
    writer.stage(batchUuid).value.value

    writer.finalizeCommit(Offset(250))

    writer.currentWriteState shouldBe a[NoWriter]
    writer.getCommittedOffset shouldBe Some(Offset(250))
    file.exists() shouldBe false
    metrics.getFilesCommittedTotal shouldBe 1L
    metrics.getRecordsCommittedTotal shouldBe 7L
    // The temp object is untouched by finalizeCommit; commitBatch deletes it best-effort.
    storage.keysUnder(bucket, tempPrefix()) should have size 1
  }

  test("[NL] finalizeCommit is monotone: a lower offset never lowers the committed offset") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val writer  = buildWriter(storage, file)
    // A writer whose commitState already records a higher committed offset than the batch offset.
    writer.forceWriteState(uploadingState(file).copy(commitState = CommitState(tp, Some(Offset(300)))))
    writer.stage(batchUuid).value.value

    writer.finalizeCommit(Offset(250))
    writer.getCommittedOffset shouldBe Some(Offset(300))

    // A second finalizeCommit on the now-NoWriter writer is a defensive no-op.
    writer.finalizeCommit(Offset(10))
    writer.getCommittedOffset shouldBe Some(Offset(300))
  }

  test("[NL] a Staged writer is a first-buffered-offset barrier, has a pending upload, is not idle or flushable") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val writer  = buildWriter(storage, file)
    writer.forceWriteState(uploadingState(file))
    writer.stage(batchUuid).value.value

    writer.getFirstBufferedOffset shouldBe Some(Offset(100))
    writer.hasPendingUpload shouldBe true
    writer.isIdle shouldBe false
    writer.shouldFlush shouldBe false
  }

  test("[NL] close() on a Staged writer deletes the local file and leaves the temp object for the sweep") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val writer  = buildWriter(storage, file)
    writer.forceWriteState(uploadingState(file))
    val staged = writer.stage(batchUuid).value.value

    writer.close()

    writer.currentWriteState shouldBe a[NoWriter]
    file.exists() shouldBe false
    storage.snapshot(bucket).keys should contain(staged.tempPath)
  }

  test("[B] batch stage temps are connector-scoped; granular commit temps are not") {
    // Batch: stage() uploads under .temp-upload/<connector>/<topic>/<partition>/<batchUuid>/.
    val batchStorage = new PathCapturingStorage
    val batchFile    = stagingFile()
    val batchWriter  = buildWriter(batchStorage, batchFile)
    batchWriter.forceWriteState(uploadingState(batchFile))
    batchWriter.stage(batchUuid).value.value
    val batchPath = batchStorage.uploadPaths.head
    batchPath should startWith(s".temp-upload/${connectorTaskId.name}/${tp.topic}/${tp.partition}/")

    // Granular: Writer.commit uploads under .temp-upload/<topic>/<partition>/<uuid>/ — no connector
    // segment — so the batch sweep's connector-scoped prefix can never match a granular temp.
    val granularStorage = new PathCapturingStorage
    val granularFile    = stagingFile()
    val granularWriter  = buildWriter(granularStorage, granularFile)
    granularWriter.forceWriteState(uploadingState(granularFile))
    granularWriter.commit.value
    val granularPath = granularStorage.uploadPaths.head
    granularPath should startWith(s".temp-upload/${tp.topic}/${tp.partition}/")
    granularPath should not startWith s".temp-upload/${connectorTaskId.name}/"
  }

  test("[NL] commit on a Staged writer is Fatal so a routing bug cannot mix the two protocols") {
    val storage = new CountingStorage
    val file    = stagingFile()
    val writer  = buildWriter(storage, file)
    writer.forceWriteState(uploadingState(file))
    writer.stage(batchUuid).value.value

    val err = writer.commit.left.value

    err shouldBe a[FatalCloudSinkError]
    err.message() should include("Staged")
    storage.keysUnder(bucket, "data/") shouldBe empty
  }
}
