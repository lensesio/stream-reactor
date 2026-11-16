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
import io.lenses.streamreactor.common.config.base.intf.ConnectionConfig
import io.lenses.streamreactor.common.errors.NoopErrorPolicy
import io.lenses.streamreactor.common.util.JarManifest
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.config.traits.CloudSinkConfig
import io.lenses.streamreactor.connect.cloud.common.formats.writer.FormatWriter
import io.lenses.streamreactor.connect.cloud.common.formats.writer.MessageDetail
import io.lenses.streamreactor.connect.cloud.common.formats.writer.schema.SchemaChangeDetector
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartitionOffset
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.CloudSinkTask
import io.lenses.streamreactor.connect.cloud.common.sink.NonFatalCloudSinkError
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
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.io.FileOutputStream
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.mutable
import scala.util.Try

/**
 * `CloudSinkTask.close(partitions)` on a cooperative rebalance, against a real `WriterManager`,
 * a real `IndexManagerV2`, in-memory storage and real local staging files.
 *
 * Kafka Connect's cooperative revoke calls `close(revoked)` only: `open()` is skipped when nothing
 * is added, and the consumer position of a retained partition is never rewound. Anything the
 * close throws away for a retained partition is therefore never re-delivered. The close must
 * dispose exactly the revoked partitions' writers and state, and leave the retained ones'
 * writers, staging files, buffers and offset barriers as they were.
 */
class CooperativeRevokeCloseTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with MockitoSugar
    with ArgumentMatchersSugar {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("coop-revoke", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)

  private val bucket        = "test-bucket"
  private val directoryName = ".indexes"
  private val topic         = Topic("orders")
  private val retained      = topic.withPartition(0)
  private val revoked       = topic.withPartition(1)

  private val keyField: PartitionField = ValuePartitionField(PartitionNamePath("k"))

  private def dataPrefix(tp: TopicPartition): String = s"data/${tp.topic.value}/${tp.partition}/"

  private def bucketAndPrefix(tp: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(dataPrefix(tp))).asRight

  /** Writes each record's offset as one line of the staging file, so the uploads can be read back. */
  private final class OffsetLinesFormatWriter(file: File) extends FormatWriter {
    private val out     = new FileOutputStream(file, true)
    private var pointer = 0L
    private var closed  = false

    override def rolloverFileOnSchemaChange(): Boolean = false

    override def write(message: MessageDetail): Either[Throwable, Unit] =
      Try {
        val bytes = s"${message.offset.value}\n".getBytes(StandardCharsets.UTF_8)
        out.write(bytes)
        out.flush()
        pointer += bytes.length
      }.toEither

    override def getPointer: Long = pointer

    override def complete(): Either[SinkError, Unit] =
      Try(if (!closed) { closed = true; out.close() }).toEither.left.map(t =>
        NonFatalCloudSinkError(t.getMessage, Some(t)),
      )
  }

  private final class Fixture(val commitMode: CommitMode, val partitionBy: Boolean) {
    val storage = new InMemoryStorageInterface()
    val metrics = new CloudSinkMetrics()
    val flush   = new java.util.concurrent.atomic.AtomicBoolean(false)
    val staged: mutable.ListBuffer[File] = mutable.ListBuffer.empty

    val im: IndexManagerV2 = {
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
        metrics                     = metrics,
      )(si, connectorTaskId)
    }

    private val policy: CommitPolicy = {
      val p = mock[CommitPolicy]
      when(p.shouldFlush(any[CommitContext])).thenAnswer((_: CommitContext) => flush.get())
      p
    }

    private val keyNamer: KeyNamer = {
      val kn = mock[KeyNamer]
      when(kn.processPartitionValues(any[MessageDetail], any[TopicPartition])).thenAnswer {
        (m: MessageDetail, _: TopicPartition) =>
          (m.value match {
            case StringSinkData(v, _) if partitionBy => Map(keyField -> v)
            case _                                   => Map.empty[PartitionField, String]
          }).asRight[SinkError]
      }
      kn
    }

    val wm: WriterManager[FakeFileMetadata] = new WriterManager[FakeFileMetadata](
      commitPolicyFn    = _ => policy.asRight,
      bucketAndPrefixFn = bucketAndPrefix,
      keyNamerFn        = _ => keyNamer.asRight,
      stagingFilenameFn = (_, _) => {
        val f = Files.createTempFile("coop-revoke-", ".staging").toFile
        f.deleteOnExit()
        staged += f
        f.asRight
      },
      objKeyBuilderFn = (tp, values) => {
        val okb = mock[ObjectKeyBuilder]
        when(okb.build(any[Offset], any[Offset], any[Long], any[Long], any[Long])).thenAnswer {
          (first: Offset, _: Offset, _: Long, _: Long, _: Long) =>
            val seg = WriterManager.derivePartitionKey(values).getOrElse("nokey")
            CloudLocation(bucket, path = Some(s"${dataPrefix(tp)}$seg-${first.value}.txt")).asRight
        }
        okb
      },
      formatWriterFn              = (_, file) => new OffsetLinesFormatWriter(file).asRight,
      indexManager                = im,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = new PendingOperationsProcessors(storage),
      commitMode                  = commitMode,
      metrics                     = metrics,
    )

    val task: TestSinkTask = {
      val t = new TestSinkTask
      t.initialize(5, NoopErrorPolicy())
      t.writerManager   = wm
      t.indexManager    = im
      t.connectorTaskId = connectorTaskId
      t
    }

    /** The PARTITIONBY value a record is routed to (ignored without PARTITIONBY). */
    def write(tp: TopicPartition, offset: Long, key: String = "a"): Unit =
      wm.write(
        TopicPartitionOffset(tp.topic, tp.partition, Offset(offset)),
        MessageDetail(StringSinkData("k", None),
                      StringSinkData(key, None),
                      Map.empty,
                      None,
                      tp.topic,
                      tp.partition,
                      Offset(offset),
        ),
      ).value

    def flushAll(): Unit = {
      flush.set(true)
      try wm.commitFlushableWriters().value
      finally flush.set(false)
    }

    def preCommit(tp: TopicPartition): Option[Long] =
      wm.preCommit(Map(tp -> new OffsetAndMetadata(1000L))).get(tp).map(_.offset())

    def mapKey(tp: TopicPartition, key: String = "a"): MapKey =
      MapKey(tp, if (partitionBy) Map(keyField -> key) else Map.empty)

    def stagingFile(key: MapKey): Option[File] =
      wm.writerFor(key).map(_.currentWriteState).collect { case w: Writing => w.file }

    /** Every offset of `tp` that reached a final data path, in key order. */
    def durableOffsets(tp: TopicPartition): Seq[Long] =
      storage
        .keysUnder(bucket, dataPrefix(tp))
        .flatMap { path =>
          new String(storage.snapshot(bucket)(path).bytes, StandardCharsets.UTF_8).linesIterator
            .filter(_.nonEmpty)
            .map(_.toLong)
        }
        .sorted

    def close(): Unit = Try(im.close()).getOrElse(())
  }

  private val combos: Seq[(CommitMode, Boolean)] =
    for {
      mode        <- Seq(CommitMode.Granular, CommitMode.Batch)
      partitionBy <- Seq(true, false)
    } yield (mode, partitionBy)

  private def label(mode: CommitMode, partitionBy: Boolean): String =
    s"[$mode, ${if (partitionBy) "PARTITIONBY" else "no PARTITIONBY"}]"

  combos.foreach {
    case (mode, partitionBy) =>
      test(
        s"${label(mode, partitionBy)} a cooperative close of the revoked partition keeps the retained partition's " +
          "buffered records: every offset reaches storage and preCommit never passes the buffer",
      ) {
        val f = new Fixture(mode, partitionBy)
        try {
          f.im.open(Set(retained, revoked)).value
          // Retained: 0 and 1 durable, 2 buffered. With PARTITIONBY the keys are mixed: key a holds
          // only committed records, its sibling key b holds a committed record and the buffered one.
          val bufferedKey = if (partitionBy) "b" else "a"
          f.write(retained, 0, "a")
          f.write(retained, 1, bufferedKey)
          f.write(revoked, 0)
          f.flushAll()
          f.preCommit(retained) shouldBe Some(2L)
          f.write(retained, 2, bufferedKey)
          if (partitionBy) f.wm.writerFor(f.mapKey(retained, "a")) shouldBe defined
          f.write(revoked, 1)
          val retainedKey     = f.mapKey(retained, bufferedKey)
          val retainedWriter  = f.wm.writerFor(retainedKey)
          val retainedStaging = f.stagingFile(retainedKey)
          val revokedStaging  = f.stagingFile(f.mapKey(revoked))
          retainedStaging.map(_.exists()) shouldBe Some(true)

          // Cooperative revoke of `revoked` only: no open() follows, the retained position is not rewound.
          f.task.close(java.util.List.of(revoked.toKafka))

          val writerKept         = f.wm.writerFor(retainedKey).exists(w => retainedWriter.exists(_ eq w))
          val stagingKept        = retainedStaging.exists(_.exists())
          val preCommitAfterRevoke = f.preCommit(retained)

          // Connect keeps delivering the retained partition from where it was.
          f.write(retained, 3, bufferedKey)
          f.write(retained, 4, bufferedKey)
          f.flushAll()

          f.durableOffsets(retained) shouldBe Seq(0L, 1L, 2L, 3L, 4L)
          f.preCommit(retained) shouldBe Some(5L)
          preCommitAfterRevoke shouldBe Some(2L)
          writerKept shouldBe true
          stagingKept shouldBe true
          // The revoked partition's writer, staging file and state are gone.
          revokedStaging.map(_.exists()) shouldBe Some(false)
          f.wm.writerFor(f.mapKey(revoked)) shouldBe None
          f.wm.trackedTopicPartitions should not contain revoked
          f.im.getSeekedOffsetForTopicPartition(revoked) shouldBe None
          f.im.getSeekedOffsetForTopicPartition(retained) shouldBe defined
        } finally f.close()
      }
  }

  combos.foreach {
    case (mode, partitionBy) =>
      test(
        s"${label(mode, partitionBy)} an eager close of every partition disposes all writers and state, then a " +
          "re-open replays from the durable floor",
      ) {
        val f = new Fixture(mode, partitionBy)
        try {
          f.im.open(Set(retained, revoked)).value
          f.write(retained, 0)
          f.write(retained, 1)
          f.write(revoked, 0)
          f.flushAll()
          f.preCommit(retained) shouldBe Some(2L)
          f.write(retained, 2)
          val staging = f.stagingFile(f.mapKey(retained))

          f.task.close(java.util.List.of(retained.toKafka, revoked.toKafka))

          f.wm.writerCount shouldBe 0
          f.wm.trackedTopicPartitions shouldBe empty
          staging.map(_.exists()) shouldBe Some(false)

          // Eager rebalance: open(all) seeks to the durable floor and the buffered record is re-delivered.
          val seeks = f.im.open(Set(retained, revoked)).value
          seeks(retained) shouldBe Some(Offset(1L))
          // Connect re-delivers from the seek: 1 is already durable and is skipped.
          f.write(retained, 1)
          f.write(retained, 2)
          f.write(retained, 3)
          f.flushAll()
          f.durableOffsets(retained) shouldBe Seq(0L, 1L, 2L, 3L)
          f.preCommit(retained) shouldBe Some(4L)
        } finally f.close()
      }
  }

  test("a cooperative close of a partition with a mixed committed/buffered PARTITIONBY key set force-writes its " +
    "master lock and counts it under the revoke reason") {
    val f = new Fixture(CommitMode.Granular, partitionBy = true)
    try {
      f.im.open(Set(retained, revoked)).value
      // Key a holds only a committed record; its sibling key b a committed and a buffered one.
      f.write(revoked, 0, "a")
      f.write(revoked, 1, "b")
      f.flushAll()
      f.write(revoked, 2, "b")
      f.wm.writerFor(f.mapKey(revoked, "a")) shouldBe defined
      f.write(retained, 0, "a")

      f.task.close(java.util.List.of(revoked.toKafka))

      f.metrics.getMasterLockWriteForcedRevoke shouldBe 1L
      f.wm.writerFor(f.mapKey(retained, "a")) shouldBe defined
    } finally f.close()
  }

  test("closing a partition with no current writer clears its residual offset and force state") {
    val f = new Fixture(CommitMode.Granular, partitionBy = false)
    try {
      f.im.open(Set(retained, revoked)).value
      f.write(revoked, 0)
      f.flushAll()
      f.preCommit(revoked) shouldBe Some(1L)
      f.write(retained, 0)
      // A rollback discards the revoked partition's writers but keeps its returned offset and
      // schedules a forced master write.
      f.wm.cleanUp(revoked)
      f.wm.writerFor(f.mapKey(revoked)) shouldBe None
      f.wm.trackedTopicPartitions should contain(revoked)

      f.task.close(java.util.List.of(revoked.toKafka))

      f.wm.trackedTopicPartitions should not contain revoked
      f.wm.trackedTopicPartitions should contain(retained)
    } finally f.close()
  }

  test("stop() disposes every writer and all state, including a partition with no current writer") {
    val f = new Fixture(CommitMode.Granular, partitionBy = true)
    try {
      f.im.open(Set(retained, revoked)).value
      f.write(retained, 0)
      f.write(revoked, 0)
      f.flushAll()
      f.preCommit(revoked) shouldBe Some(1L)
      f.write(retained, 1)
      f.wm.cleanUp(revoked)

      f.wm.closeForStop()

      f.wm.writerCount shouldBe 0
      f.wm.trackedTopicPartitions shouldBe empty
    } finally f.close()
  }

  private class StubCC extends ConnectionConfig

  private class TestSinkTask
      extends CloudSinkTask[FakeFileMetadata, CloudSinkConfig[StubCC], StubCC, Unit](
        "test-connector",
        "",
        JarManifest.fromUrl(new URL("file:///not-a-jar")),
      ) {
    override def createClient(config: StubCC): Either[Throwable, Unit] = Right(())
    override def createStorageInterface(
      connectorTaskId: ConnectorTaskId,
      config:          CloudSinkConfig[StubCC],
      cloudClient:     Unit,
    ): StorageInterface[FakeFileMetadata] = throw new UnsupportedOperationException("not used")
    override def convertPropsToConfig(
      connectorTaskId: ConnectorTaskId,
      props:           Map[String, String],
    ): Either[Throwable, CloudSinkConfig[StubCC]] = Left(new UnsupportedOperationException("not used"))
  }
}
