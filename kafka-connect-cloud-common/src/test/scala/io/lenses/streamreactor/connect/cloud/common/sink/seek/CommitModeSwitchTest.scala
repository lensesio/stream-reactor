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
package io.lenses.streamreactor.connect.cloud.common.sink.seek

import cats.data.NonEmptyList
import cats.data.Validated
import cats.implicits.catsSyntaxEitherId
import io.circe.Encoder
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
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.conversion.StringSinkData
import io.lenses.streamreactor.connect.cloud.common.sink.metrics.CloudSinkMetrics
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.writer.WriterManager
import io.lenses.streamreactor.connect.cloud.common.storage.FileCreateError
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.storage.UploadError
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailDeleteAt
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
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable

/**
 * Granular -> batch migration. A batch-mode task opening over a granular-mode layout must:
 * snapshot and eTag-bump every legacy granular lock (fencing granular zombies and resolving their
 * in-flight chains), then purge them immediately once every lock on the TP has been resolved --
 * nothing is gated on a batch watermark, because `batchDedupFloor` is master-only and never
 * consults these locks for deduplication (see `IndexManagerV2.batchDedupFloor`). The accepted
 * trade-off is a bounded, one-time duplicate window: any record in
 * `(masterAtSwitch, maxLegacyLockAtSwitch]` is re-written (never lost), even one that would have
 * been skipped by the old per-key floor.
 */
class CommitModeSwitchTest
    extends AnyFunSuiteLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with ArgumentMatchersSugar
    with BeforeAndAfterEach {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("switch-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)
  private implicit val indexFileEncoder: Encoder[IndexFile] = IndexFile.indexFileEncoder

  private val bucket        = "test-bucket"
  private val directoryName = ".indexes"
  private val tp            = Topic("orders").withPartition(0)

  private val keyField: PartitionField = ValuePartitionField(PartitionNamePath("k"))
  private def pv(name:    String): immutable.Map[PartitionField, String] = Map(keyField -> name)
  private def keyOf(name: String): String                                = WriterManager.derivePartitionKey(pv(name)).get

  private def masterPath = IndexManagerV2.generateLockFilePath(connectorTaskId, tp, directoryName)
  private def granularPath(key: String) =
    IndexManagerV2.generateGranularLockFilePath(connectorTaskId, tp, key, directoryName)

  private var storage: InMemoryStorageInterface = _

  override def beforeEach(): Unit = storage = new InMemoryStorageInterface()

  private def bucketAndPrefix(topicPartition: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/")).asRight

  /** Directly writes an index file (bypassing eTag preconditions) and returns its eTag. */
  private def writeIndex(path: String, idx: IndexFile): String =
    storage.writeBlobToFile(bucket, path, NoOverwriteExistingObject(idx)).value.eTag

  private def eTagOf(path: String): String = storage.snapshot(bucket)(path).eTag

  /** Seeds a granular-mode master lock at committed offset `m`. */
  private def seedMaster(m: Long): Unit = {
    val _ = writeIndex(masterPath, IndexFile("prev-owner", Some(Offset(m)), None))
  }

  private def buildIndexManager(
    commitMode: CommitMode,
    st:         InMemoryStorageInterface = storage,
  ): IndexManagerV2 = {
    implicit val si: InMemoryStorageInterface = st
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(st),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = Int.MaxValue,
      gcSweepMinAgeSeconds        = Int.MaxValue,
      gcSweepEnabled              = false,
      commitMode                  = commitMode,
    )(si, connectorTaskId)
  }

  private def tempFileWith(payload: String): File = {
    val f = Files.createTempFile("switch-", ".tmp").toFile
    Files.write(f.toPath, payload.getBytes(StandardCharsets.UTF_8))
    f.deleteOnExit()
    f
  }

  private final class TogglePolicy {
    val flush = new java.util.concurrent.atomic.AtomicBoolean(false)
    val policy: CommitPolicy = {
      val p = mock[CommitPolicy]
      when(p.shouldFlush(any[CommitContext])).thenAnswer((_: CommitContext) => flush.get())
      p
    }
  }

  private def buildWriterManager(
    im:      IndexManagerV2,
    metrics: CloudSinkMetrics,
    values:  immutable.Map[PartitionField, String],
    policy:  CommitPolicy,
    st:      InMemoryStorageInterface = storage,
  ): WriterManager[FakeFileMetadata] = {
    val keyNamer = mock[KeyNamer]
    when(keyNamer.processPartitionValues(any[MessageDetail], any[TopicPartition]))
      .thenReturn(values.asRight[SinkError])
    val fw = mock[FormatWriter]
    when(fw.write(any[MessageDetail])).thenReturn(().asRight)
    when(fw.complete()).thenReturn(().asRight)
    when(fw.rolloverFileOnSchemaChange()).thenReturn(false)

    new WriterManager[FakeFileMetadata](
      commitPolicyFn    = _ => policy.asRight,
      bucketAndPrefixFn = bucketAndPrefix,
      keyNamerFn        = _ => keyNamer.asRight,
      stagingFilenameFn = (_, _) => tempFileWith("payload").asRight,
      objKeyBuilderFn = (_, values2) => {
        val okb = mock[ObjectKeyBuilder]
        when(okb.build(any[Offset], any[Offset], any[Long], any[Long], any[Long])).thenAnswer {
          (first: Offset, _: Offset, _: Long, _: Long, _: Long) =>
            val seg = WriterManager.derivePartitionKey(values2).getOrElse("nokey")
            CloudLocation(bucket,
                          path = Some(s"data/${tp.topic.value}/${tp.partition}/$seg-${first.value}.json"),
            ).asRight
        }
        okb
      },
      formatWriterFn              = (_, _) => fw.asRight,
      indexManager                = im,
      transformerF                = Right(_),
      schemaChangeDetector        = mock[SchemaChangeDetector],
      skipNullValues              = false,
      pendingOperationsProcessors = new PendingOperationsProcessors(st),
      commitMode                  = im.commitMode,
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

  test(
    "[Z] the first batch open eTag-bumps every legacy lock then purges them, leaving tmp orphans untouched",
  ) {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(120)), None))
    writeIndex(granularPath(keyOf("B")), IndexFile("prev", Some(Offset(140)), None))
    val tmpPath = granularPath(keyOf("A")) + ".tmp.0000abcd"
    val _       = writeIndex(tmpPath, IndexFile("prev", Some(Offset(999)), None))
    val tmpETag = eTagOf(tmpPath)

    val im = buildIndexManager(CommitMode.Batch)
    try {
      im.open(Set(tp)).value

      // Fenced (harmless if the bump immediately precedes the delete) AND purged: nothing gates
      // the delete on a batch watermark any more, because `batchDedupFloor` never reads these
      // locks (see the class doc above).
      im.legacyPurged(tp) shouldBe true
      storage.snapshot(bucket).keys should not contain granularPath(keyOf("A"))
      storage.snapshot(bucket).keys should not contain granularPath(keyOf("B"))
      // The `.lock.tmp` orphan is not a legacy lock; it is left alone (only the sweep, or an
      // operator, cleans it up).
      eTagOf(tmpPath) shouldBe tmpETag
    } finally im.close()
  }

  test(
    "[NL] a legacy lock ahead of the master floor does NOT skip replayed records: they are re-written, never lost",
  ) {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value
    im.legacyPurged(tp) shouldBe true
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy)

    // The master-only floor (99) does not know about A's old lock (150): every one of these
    // records is re-written, not skipped. This is the accepted, bounded duplicate window that
    // replaces the old per-key floor's silent skip -- see the class doc.
    (100L to 150L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    metrics.getRecordsWrittenTotal shouldBe 51L
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L

    wm.write(tp.withOffset(Offset(151)), message(151)).value
    metrics.getRecordsWrittenTotal shouldBe 52L

    wm.close()
    im.close()
  }

  test(
    "[NL] a legacy lock with a pending Copy/Delete chain is resolved and purged at open; replay re-writes, not skips",
  ) {
    seedMaster(99)
    val temp   = s".temp-upload/legacy/${tp.topic}/0/uuid/data/orders/0/legacy-A.json"
    val final_ = "data/orders/0/legacy-A.json"
    // Seed the temp object the pending Copy will move.
    storage.writeStringToFile(bucket,
                              temp,
                              io.lenses.streamreactor.connect.cloud.common.model.UploadableString("legacy payload"),
    ).value
    val tempETag = storage.snapshot(bucket)(temp).eTag
    writeIndex(
      granularPath(keyOf("A")),
      IndexFile(
        "prev",
        Some(Offset(100)),
        Some(
          PendingState(
            Offset(120),
            NonEmptyList.of(CopyOperation(bucket, temp, final_, tempETag), DeleteOperation(bucket, temp, tempETag)),
          ),
        ),
      ),
    )

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value

    // The chain resolved (the Copy landed the data) and the now-clean lock was purged.
    storage.keysUnder(bucket, "data/orders/0/legacy-A") should have size 1
    im.legacyPurged(tp) shouldBe true

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy)
    // Master floor is still 99: every one of these was already durably written by the resolved
    // chain, but the master-only floor re-writes them anyway (the accepted duplicate window).
    (100L to 120L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    metrics.getRecordsWrittenTotal shouldBe 21L
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    wm.close()
    im.close()
  }

  test(
    "[NL] a legacy pending chain whose upload never happened is dead-worker cleared and purged; replay re-writes",
  ) {
    seedMaster(99)
    val missing = new File("/nonexistent/switch-test/legacy-staging.tmp")
    val temp    = ".temp-upload/legacy/orders/0/uuid/data/orders/0/legacy-A.json"
    writeIndex(
      granularPath(keyOf("A")),
      IndexFile(
        "prev",
        Some(Offset(110)),
        Some(
          PendingState(
            Offset(130),
            NonEmptyList.of(
              UploadOperation(bucket, missing, temp),
              CopyOperation(bucket, temp, "data/orders/0/legacy-A.json", "x"),
              DeleteOperation(bucket, temp, "x"),
            ),
          ),
        ),
      ),
    )

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value

    // Dead-worker recovery: pending state cleared, lock fenced and then purged.
    im.legacyPurged(tp) shouldBe true
    storage.keysUnder(bucket, "data/orders/0/legacy-A") shouldBe empty

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy)
    // Master floor is 99: nothing here was ever durably written (the upload never happened), so
    // every record is correctly written -- not a duplicate.
    (100L to 111L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    metrics.getRecordsWrittenTotal shouldBe 12L
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    wm.close()
    im.close()
  }

  /**
   * A granular zombie writes the lock between the new owner's read and its eTag bump, so the bump
   * 412s. The owner re-reads, sees the zombie's higher offset, and uses it. Exhausting the bounded
   * re-reads is fatal.
   *
   * The invariant [Z] is exercised by mutating the stored lock (a fresh eTag at a higher offset)
   * immediately before each bump write, which is exactly the race the re-read loop guards against.
   */
  private final class RaceOnBumpStorage(lockPath: String, raceOffset: Long, raceTimes: Int)
      extends InMemoryStorageInterface {
    private val hits = new AtomicInteger(0)
    override def writeBlobToFile[O](
      b:   String,
      p:   String,
      obj: ObjectProtection[O],
    )(
      implicit
      enc: Encoder[O],
    ): Either[UploadError, ObjectWithETag[O]] =
      obj match {
        case _: ObjectWithETag[O] if p == lockPath && hits.getAndIncrement() < raceTimes =>
          // A zombie committed a higher offset with a fresh eTag just before our bump: overwrite
          // the stored lock (using its current eTag) then reject our If-Match write with a 412.
          val currentETag = super.getBlobAsObject[IndexFile](b, p)(IndexFile.indexFileDecoder).toOption.get.eTag
          super.writeBlobToFile[IndexFile](
            b,
            p,
            ObjectWithETag(IndexFile("zombie", Some(Offset(raceOffset)), None), currentETag),
          )(IndexFile.indexFileEncoder)
          FileCreateError(new IllegalStateException("If-Match failed (raced)"), p).asLeft
        case _ => super.writeBlobToFile(b, p, obj)
      }
  }

  test(
    "[Z] a bump that loses one race re-reads and adopts the racer's lock; exhausting the budget is non-fatal " +
      "to open() but leaves that lock unfenced-this-cycle",
  ) {
    // One race: re-read succeeds on retry, the lock is fenced and purged normally.
    locally {
      val st = new RaceOnBumpStorage(granularPath(keyOf("A")), raceOffset = 150, raceTimes = 1)
      storage = st
      seedMaster(99)
      writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(140)), None))
      val im = buildIndexManager(CommitMode.Batch, st)
      im.open(Set(tp)).value
      im.legacyPurged(tp) shouldBe true
      st.snapshot(bucket).keys should not contain granularPath(keyOf("A"))
      im.close()
    }

    // Never wins: three consecutive mismatches exhaust MaxLegacyBumpAttempts. This is now
    // NON-FATAL to open(): the TP simply is not purged this cycle, and the next open()
    // retries. Correctness does not depend on it, because the master-only floor never reads this
    // lock either way.
    locally {
      val st = new RaceOnBumpStorage(granularPath(keyOf("A")),
                                     raceOffset = 150,
                                     raceTimes  = IndexManagerV2.MaxLegacyBumpAttempts,
      )
      storage = st
      seedMaster(99)
      writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(140)), None))
      val im = buildIndexManager(CommitMode.Batch, st)
      im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(99)))
      im.legacyPurged(tp) shouldBe false
      st.snapshot(bucket).keys should contain(granularPath(keyOf("A")))
      im.close()
    }
  }

  test("[B] a legacy lock is purged immediately at open, independent of any batch commit or watermark") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val im = buildIndexManager(CommitMode.Batch)
    // Purge happens as part of open() itself -- no writer, no commit, no watermark involved.
    im.open(Set(tp)).value

    im.legacyPurged(tp) shouldBe true
    storage.snapshot(bucket).keys should not contain granularPath(keyOf("A"))
    im.close()
  }

  test("[NL] a never-seen key's dedup floor is the master offset alone, with no per-key GET at all") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val gets = new AtomicInteger(0)
    val counting = new InMemoryStorageInterface() {
      override def getBlobAsObject[O](
        b: String,
        p: String,
      )(
        implicit
        d: io.circe.Decoder[O],
      ): Either[io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError, ObjectWithETag[O]] = {
        if (p.endsWith(".lock") && !p.endsWith("0.lock")) gets.incrementAndGet()
        super.getBlobAsObject(b, p)
      }
    }
    storage = counting
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val im = buildIndexManager(CommitMode.Batch, counting)
    im.open(Set(tp)).value
    im.legacyPurged(tp) shouldBe true

    // batchDedupFloor never reads a per-key lock (there is no such lock to read any more): the
    // floor for a never-seen key Z is exactly the master offset, with zero additional GETs.
    val getsBefore = gets.get()
    im.batchDedupFloor(tp).value shouldBe im.getSeekedOffsetForTopicPartition(tp)
    gets.get() shouldBe getsBefore

    im.close()
  }

  test("[B] a purge that fails leaves the lock in place; the next open() retries and succeeds") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))
    storage.arm(FailDeleteAt(bucket, granularPath(keyOf("A"))))

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value

    // Purge failed: not purged, lock still present (already fenced by the bump though).
    im.legacyPurged(tp) shouldBe false
    storage.snapshot(bucket).keys should contain(granularPath(keyOf("A")))

    // Next open() (e.g. a rebalance revoke+reassign within the same task) re-lists, re-fences
    // (a harmless no-op bump), and retries the purge -- the one-shot hook was already consumed.
    im.open(Set(tp)).value
    im.legacyPurged(tp) shouldBe true
    storage.snapshot(bucket).keys should not contain granularPath(keyOf("A"))

    im.close()
  }

  test(
    "[B] a legacy-lock LIST failure leaves the TP unresolved but open() still succeeds; retried on the next open()",
  ) {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val listFailing = new InMemoryStorageInterface() {
      override def listFileMetaRecursive(
        b:      String,
        prefix: Option[String],
      ): Either[FileListError, Option[ListOfMetadataResponse[FakeFileMetadata]]] =
        if (prefix.exists(_.contains(".locks/")))
          FileListError(new RuntimeException("boom"), b, prefix).asLeft
        else super.listFileMetaRecursive(b, prefix)
    }
    storage = listFailing
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))

    val im = buildIndexManager(CommitMode.Batch, listFailing)
    // A LIST failure must NOT fail open(): correctness no longer depends on legacy-lock
    // fencing succeeding -- batchDedupFloor is master-only regardless.
    im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(99)))
    im.legacyPurged(tp) shouldBe false
    storage.snapshot(bucket).keys should contain(granularPath(keyOf("A")))

    // Next open() (storage healthy again) retries and succeeds.
    storage = new InMemoryStorageInterface()
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(150)), None))
    val im2 = buildIndexManager(CommitMode.Batch)
    im2.open(Set(tp)).value
    im2.legacyPurged(tp) shouldBe true

    im.close(); im2.close()
  }

  test("[B] purge chunks deletes by gcBatchSize, purging every lock across multiple delete calls") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(120)), None))
    writeIndex(granularPath(keyOf("B")), IndexFile("prev", Some(Offset(130)), None))
    writeIndex(granularPath(keyOf("C")), IndexFile("prev", Some(Offset(140)), None))

    val deleteCallSizes = scala.collection.mutable.ListBuffer.empty[Int]
    val countingDeletes = new InMemoryStorageInterface() {
      override def deleteFiles(
        b:     String,
        files: Seq[String],
      ): Either[io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError, Unit] = {
        deleteCallSizes += files.size
        super.deleteFiles(b, files)
      }
    }
    storage = countingDeletes
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(120)), None))
    writeIndex(granularPath(keyOf("B")), IndexFile("prev", Some(Offset(130)), None))
    writeIndex(granularPath(keyOf("C")), IndexFile("prev", Some(Offset(140)), None))

    implicit val si: InMemoryStorageInterface = countingDeletes
    val im = new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(countingDeletes),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcBatchSize                 = 2, // 3 locks + 1 marker = 4 paths -> chunks of [2, 2]
      gcSweepIntervalSeconds      = Int.MaxValue,
      gcSweepMinAgeSeconds        = Int.MaxValue,
      gcSweepEnabled              = false,
      commitMode                  = CommitMode.Batch,
    )(si, connectorTaskId)

    im.open(Set(tp)).value
    im.legacyPurged(tp) shouldBe true
    storage.snapshot(bucket).keys should not contain granularPath(keyOf("A"))
    storage.snapshot(bucket).keys should not contain granularPath(keyOf("B"))
    storage.snapshot(bucket).keys should not contain granularPath(keyOf("C"))
    // Every chunk is bounded by gcBatchSize; more than one call was needed for 4 paths.
    deleteCallSizes.toList.forall(_ <= 2) shouldBe true
    deleteCallSizes.sum shouldBe 4
    deleteCallSizes.size should be >= 2

    im.close()
  }

  test("[B] a purged TP does not consult a key that appears afterwards; the record is written, not skipped") {
    seedMaster(99)
    // No legacy locks at open time.
    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value
    im.legacyPurged(tp) shouldBe true // empty listing: nothing to fence

    // A lock for K2 appears in storage after the snapshot (should not happen post-switch under
    // the stop-first procedure, but batchDedupFloor's master-only design does not depend on that
    // procedure being followed: it never consults per-key locks in the first place).
    writeIndex(granularPath(keyOf("K2")), IndexFile("prev", Some(Offset(130)), None))
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("K2"), new TogglePolicy().policy)
    wm.write(tp.withOffset(Offset(120)), message(120)).value
    metrics.getRecordsWrittenTotal shouldBe 1L
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    wm.close(); im.close()
  }

  test("[Z] bump-first: the new owner's open fences a granular zombie that opened earlier") {
    seedMaster(99)
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(120)), None))

    // A granular zombie opened before the new owner and cached the granular lock's eTag.
    val zombie = buildIndexManager(CommitMode.Granular)
    zombie.open(Set(tp)).value
    zombie.ensureGranularLock(tp, keyOf("A")).value

    // The new batch owner opens and eager-bumps the granular lock.
    val owner = buildIndexManager(CommitMode.Batch)
    owner.open(Set(tp)).value

    // The zombie's cached eTag is now stale: its granular write is fenced.
    zombie.updateForPartitionKey(tp, keyOf("A"), Some(Offset(160)), None).left.value shouldBe a[FatalCloudSinkError]
    // No object reached a final path from the zombie.
    storage.keysUnder(bucket, "data/") shouldBe empty

    // The new owner replays those offsets once.
    val metrics = new CloudSinkMetrics()
    val policy  = new TogglePolicy()
    val wm      = buildWriterManager(owner, metrics, pv("A"), policy.policy)
    (121L to 130L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    policy.flush.set(true)
    wm.commitFlushableWriters().value
    metrics.getRecordsWrittenTotal shouldBe 10L
    storage.keysUnder(bucket, "data/") should have size 1

    wm.close(); owner.close(); zombie.close()
  }

  test(
    "[NL] zombie-first: the new owner resolves a chain a zombie already completed and purges it; replay " +
      "re-writes (a duplicate object), never loses",
  ) {
    seedMaster(99)
    // The zombie already performed its copy and recorded a Delete-only pending state.
    val temp   = ".temp-upload/legacy/orders/0/uuid/data/orders/0/z.json"
    val final_ = "data/orders/0/z.json"
    storage.writeStringToFile(bucket,
                              final_,
                              io.lenses.streamreactor.connect.cloud.common.model.UploadableString("zombie payload"),
    ).value
    writeIndex(
      granularPath(keyOf("A")),
      IndexFile(
        "zombie",
        Some(Offset(100)),
        Some(PendingState(Offset(140), NonEmptyList.of(DeleteOperation(bucket, temp, "z-etag")))),
      ),
    )

    val owner = buildIndexManager(CommitMode.Batch)
    owner.open(Set(tp)).value

    // The Delete resolved (temp was already gone -> idempotent) and the lock was purged.
    owner.legacyPurged(tp) shouldBe true
    storage.keysUnder(bucket, "data/orders/0/z") should have size 1

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(owner, metrics, pv("A"), new TogglePolicy().policy)
    // Master floor is still 99: none of these are skipped, even though the zombie's resolved
    // chain already made offsets 100..140's payload durable under "z.json" -- on an actual flush
    // (not exercised by this harness, which only buffers) the object key builder would derive a
    // fresh path from the replay's own offsets, producing a duplicate object, never a loss.
    (100L to 140L).foreach(o => wm.write(tp.withOffset(Offset(o)), message(o)).value)
    metrics.getRecordsWrittenTotal shouldBe 41L
    metrics.getDuplicateRecordsSkippedTotal shouldBe 0L
    wm.close(); owner.close()
  }

  // ── batch -> granular rollback ──────────────────────────────────────────────────────

  test("[ND] rollback: a stale legacy lock below the master floor cannot re-write a committed record") {
    // Batch mode committed W = 250 and left a stale legacy K.lock = 200 (crash before purge).
    seedMasterAt250()
    writeIndex(granularPath(keyOf("A")), IndexFile("prev", Some(Offset(200)), None))

    val im = buildIndexManager(CommitMode.Granular)
    im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(250)))
    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy)

    // The granular fallback floors at max(K.lock=200, master=250) = 250, so 250 is
    // skipped and 251 written — the `orElse` behaviour would have re-written 201..250.
    wm.write(tp.withOffset(Offset(250)), message(250)).value
    metrics.getDuplicateRecordsSkippedTotal shouldBe 1L
    wm.write(tp.withOffset(Offset(251)), message(251)).value
    metrics.getRecordsWrittenTotal shouldBe 1L

    wm.close(); im.close()
  }

  test("[NL] rollback with a pending batch chain: granular open completes the copies and skips <= P") {
    // Master carries a batch PendingState of two copies; granular-mode open must drive it to
    // completion (Copy returns no eTag so updateEtag is a no-op; the last Copy sets committed = P).
    val tempA = ".temp-upload/switch-test/orders/0/uuid/data/orders/0/roll-a.json"
    val tempB = ".temp-upload/switch-test/orders/0/uuid/data/orders/0/roll-b.json"
    storage.writeStringToFile(bucket,
                              tempA,
                              io.lenses.streamreactor.connect.cloud.common.model.UploadableString("a"),
    ).value
    storage.writeStringToFile(bucket,
                              tempB,
                              io.lenses.streamreactor.connect.cloud.common.model.UploadableString("b"),
    ).value
    val eA = storage.snapshot(bucket)(tempA).eTag
    val eB = storage.snapshot(bucket)(tempB).eTag
    writeIndex(
      masterPath,
      IndexFile(
        "prev",
        Some(Offset(99)),
        Some(
          PendingState(
            Offset(250),
            NonEmptyList.of(
              CopyOperation(bucket, tempA, "data/orders/0/roll-a.json", eA),
              CopyOperation(bucket, tempB, "data/orders/0/roll-b.json", eB),
            ),
          ),
        ),
      ),
    )

    val im = buildIndexManager(CommitMode.Granular)
    im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(250)))
    storage.keysUnder(bucket, "data/orders/0/roll-a") should have size 1
    storage.keysUnder(bucket, "data/orders/0/roll-b") should have size 1
    storage.getBlobAsObject[IndexFile](bucket, masterPath).value.wrappedObject.pendingState shouldBe None

    val metrics = new CloudSinkMetrics()
    val wm      = buildWriterManager(im, metrics, pv("A"), new TogglePolicy().policy)
    wm.write(tp.withOffset(Offset(250)), message(250)).value
    metrics.getDuplicateRecordsSkippedTotal shouldBe 1L
    wm.write(tp.withOffset(Offset(251)), message(251)).value
    metrics.getRecordsWrittenTotal shouldBe 1L
    wm.close(); im.close()
  }

  // A test for "rollback before any batch commit" was removed: it never constructed a batch
  // component or exercised any rollback path, only plain granular-mode behaviour already
  // covered by GranularLockScenarioTest.

  private def seedMasterAt250(): Unit = {
    val _ = writeIndex(masterPath, IndexFile("prev-owner", Some(Offset(250)), None))
  }

  test(
    "[ND] a batch Copy chain whose source is already gone (dest present) resolves as idempotent success",
  ) {
    // Models a crash between a completed mvFile and the lock rewrite: the source is gone (the
    // move already happened) but the destination is present. `InMemoryStorageInterface.mvFile`
    // treats missing-source + present-dest as success, and open() must resolve the chain rather
    // than fail.
    val finalPath = "data/orders/0/batch-a.json"
    storage.writeStringToFile(
      bucket,
      finalPath,
      io.lenses.streamreactor.connect.cloud.common.model.UploadableString("already-moved"),
    ).value
    writeIndex(
      masterPath,
      IndexFile(
        "prev-owner",
        Some(Offset(99)),
        Some(
          PendingState(
            Offset(150),
            NonEmptyList.of(CopyOperation(bucket, "gone-temp-path", finalPath, "does-not-matter")),
          ),
        ),
      ),
    )

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(150)))
    storage.keysUnder(bucket, "data/orders/0/batch-a") should have size 1
    storage.getBlobAsObject[IndexFile](bucket, masterPath).value.wrappedObject.pendingState shouldBe None
    im.close()
  }

  test("[NL] a batch Copy chain whose source AND destination are both missing fails (not silently dropped)") {
    writeIndex(
      masterPath,
      IndexFile(
        "prev-owner",
        Some(Offset(99)),
        Some(
          PendingState(
            Offset(150),
            NonEmptyList.of(CopyOperation(bucket, "gone-temp-path", "data/orders/0/never-arrived.json", "x")),
          ),
        ),
      ),
    )

    val im = buildIndexManager(CommitMode.Batch)
    im.open(Set(tp)).isLeft shouldBe true
    storage.keysUnder(bucket, "data/orders/0/never-arrived") shouldBe empty
    im.close()
  }
}
