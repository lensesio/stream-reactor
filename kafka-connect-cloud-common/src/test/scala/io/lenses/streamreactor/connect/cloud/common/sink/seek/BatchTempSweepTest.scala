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
import io.circe.Decoder
import io.circe.Encoder
import io.lenses.streamreactor.connect.cloud.common.config.ConnectorTaskId
import io.lenses.streamreactor.connect.cloud.common.model.Offset
import io.lenses.streamreactor.connect.cloud.common.model.Topic
import io.lenses.streamreactor.connect.cloud.common.model.TopicPartition
import io.lenses.streamreactor.connect.cloud.common.model.UploadableString
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocation
import io.lenses.streamreactor.connect.cloud.common.model.location.CloudLocationValidator
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.storage.FileDeleteError
import io.lenses.streamreactor.connect.cloud.common.storage.FileListError
import io.lenses.streamreactor.connect.cloud.common.storage.FileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.GeneralFileLoadError
import io.lenses.streamreactor.connect.cloud.common.storage.ListOfMetadataResponse
import io.lenses.streamreactor.connect.cloud.common.testing.FakeFileMetadata
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface
import io.lenses.streamreactor.connect.cloud.common.testing.InMemoryStorageInterface.FailMoveAt
import org.scalatest.BeforeAndAfterEach
import org.scalatest.EitherValues
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.LockSupport
import scala.jdk.CollectionConverters._

/**
 * The batch-mode `.temp-upload` orphan sweep. It runs at the end of batch-mode `open()`,
 * strictly after master resolution, the ownership bump and the legacy snapshot, and reaps aged
 * temp objects under the connector-scoped prefix without ever touching anything else.
 */
class BatchTempSweepTest extends AnyFunSuiteLike with Matchers with EitherValues with BeforeAndAfterEach {

  private implicit val connectorTaskId: ConnectorTaskId = ConnectorTaskId("sweep-test", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator =
    (location: CloudLocation) => Validated.valid(location)
  private implicit val indexFileEncoder: Encoder[IndexFile] = IndexFile.indexFileEncoder

  private val bucket        = "test-bucket"
  private val directoryName = ".indexes"
  private val tp            = Topic("orders").withPartition(0)
  private val ageSeconds    = 3600

  private def scopedPrefix = s".temp-upload/${connectorTaskId.name}/${tp.topic}/${tp.partition}/"
  private def masterPath   = IndexManagerV2.generateLockFilePath(connectorTaskId, tp, directoryName)

  private var storage: InMemoryStorageInterface = _

  override def beforeEach(): Unit = storage = new InMemoryStorageInterface()

  private def bucketAndPrefix(topicPartition: TopicPartition): Either[SinkError, CloudLocation] =
    CloudLocation(bucket, Some(s"data/${topicPartition.topic.value}/${topicPartition.partition}/")).asRight

  private def buildIndexManager(st: InMemoryStorageInterface = storage): IndexManagerV2 = {
    implicit val si: InMemoryStorageInterface = st
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(st),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcSweepIntervalSeconds      = ageSeconds,
      gcSweepMinAgeSeconds        = ageSeconds,
      gcSweepEnabled              = false,
      commitMode                  = CommitMode.Batch,
    )(si, connectorTaskId)
  }

  private def seedMaster(committed: Long, pending: Option[PendingState] = None): Unit = {
    val _ = storage.writeBlobToFile(
      bucket,
      masterPath,
      NoOverwriteExistingObject(IndexFile("prev-owner", Some(Offset(committed)), pending)),
    ).value
  }

  private def writeTemp(path: String, ageSecondsAgo: Int): Unit = {
    storage.writeStringToFile(bucket, path, UploadableString("payload")).value
    val _ = storage.setLastModified(bucket, path, Instant.now().minusSeconds(ageSecondsAgo.toLong))
  }

  test("[B] an aged unreferenced temp under the scoped prefix is swept; a fresh one is kept") {
    seedMaster(99)
    val old   = scopedPrefix + "uuid-old/data/orders/0/old.json"
    val fresh = scopedPrefix + "uuid-fresh/data/orders/0/fresh.json"
    writeTemp(old, ageSecondsAgo   = ageSeconds + 60)
    writeTemp(fresh, ageSecondsAgo = 0)

    val im = buildIndexManager()
    try {
      im.open(Set(tp)).value
      storage.snapshot(bucket).keys should not contain old
      storage.snapshot(bucket).keys should contain(fresh)
    } finally im.close()
  }

  test("[B] temps under another connector or the granular layout are never touched") {
    seedMaster(99)
    val otherConnector = s".temp-upload/other-connector/${tp.topic}/${tp.partition}/uuid/data/x.json"
    val granularLayout = s".temp-upload/${tp.topic}/${tp.partition}/uuid/data/y.json"
    writeTemp(otherConnector, ageSecondsAgo = ageSeconds + 60)
    writeTemp(granularLayout, ageSecondsAgo = ageSeconds + 60)

    val im = buildIndexManager()
    try {
      im.open(Set(tp)).value
      storage.snapshot(bucket).keys should contain(otherConnector)
      storage.snapshot(bucket).keys should contain(granularLayout)
    } finally im.close()
  }

  test("[NL] a temp referenced by a resolvable pending Copy is completed by open, not swept first") {
    val temp   = scopedPrefix + "uuid/data/orders/0/pending.json"
    val final_ = "data/orders/0/pending.json"
    storage.writeStringToFile(bucket, temp, UploadableString("pending payload")).value
    storage.setLastModified(bucket, temp, Instant.now().minusSeconds((ageSeconds + 600).toLong)) shouldBe true
    val tempETag = storage.snapshot(bucket)(temp).eTag
    seedMaster(
      99,
      Some(PendingState(Offset(150), NonEmptyList.of(CopyOperation(bucket, temp, final_, tempETag)))),
    )

    val im = buildIndexManager()
    try {
      im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(150)))
      // The copy completed: the final object exists. The sweep did not delete the temp before
      // resolution (it ran after, and the resolved pending state is None so nothing lingered).
      storage.keysUnder(bucket, "data/orders/0/pending") should have size 1
    } finally im.close()
  }

  test("[NL] a temp referenced by a pending state that open could not resolve is not deleted") {
    val temp   = scopedPrefix + "uuid/data/orders/0/stuck.json"
    val final_ = "data/orders/0/stuck.json"
    storage.writeStringToFile(bucket, temp, UploadableString("stuck payload")).value
    storage.setLastModified(bucket, temp, Instant.now().minusSeconds((ageSeconds + 600).toLong)) shouldBe true
    val tempETag = storage.snapshot(bucket)(temp).eTag
    seedMaster(
      99,
      Some(PendingState(Offset(150), NonEmptyList.of(CopyOperation(bucket, temp, final_, tempETag)))),
    )
    // The copy fails, so open() returns Left before the sweep would ever run.
    storage.arm(FailMoveAt(bucket, temp))

    val im = buildIndexManager()
    try {
      // open returns Left (the copy failed); a single-op [Copy] chain is a last-op NonFatal.
      im.open(Set(tp)).isLeft shouldBe true
      // The temp survives: open short-circuited on the failed resolution, so the sweep never ran.
      storage.snapshot(bucket).keys should contain(temp)
    } finally im.close()
  }

  test("[B] a sweep LIST failure leaves open() successful") {
    seedMaster(99)
    val listFailing = new InMemoryStorageInterface() {
      override def listFileMetaRecursive(
        b:      String,
        prefix: Option[String],
      ): Either[FileListError, Option[ListOfMetadataResponse[FakeFileMetadata]]] =
        if (prefix.exists(_.startsWith(".temp-upload/")))
          FileListError(new RuntimeException("boom"), b, prefix).asLeft
        else super.listFileMetaRecursive(b, prefix)
    }
    storage = listFailing
    seedMaster(99)

    val im = buildIndexManager(listFailing)
    try {
      im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(99)))
    } finally im.close()
  }

  /**
   * Storage whose master-lock GET can be turned into a transient failure while LIST and every
   * other operation keeps working -- a 5xx or timeout on the single read the sweep uses to learn
   * which temps are still referenced by an in-flight batch.
   *
   * `failFromCall` fails from the Nth master-lock GET onwards (so `open()`'s own read can
   * succeed and the sweep's read fail); `failMasterGet` fails every one until cleared.
   */
  private class FlakyMasterGetStorage(failFromCall: Int = Int.MaxValue) extends InMemoryStorageInterface {
    private val masterGets = new AtomicInteger(0)
    @volatile var failMasterGet: Boolean = false

    override def getBlobAsObject[O](
      b: String,
      p: String,
    )(
      implicit
      d: Decoder[O],
    ): Either[FileLoadError, ObjectWithETag[O]] =
      if (p == masterPath && (failMasterGet || masterGets.incrementAndGet() >= failFromCall))
        GeneralFileLoadError(new RuntimeException("boom"), p).asLeft
      else super.getBlobAsObject(b, p)
  }

  test("[NL] an unreadable master lock skips the sweep rather than treating everything as orphaned") {
    val flaky = new FlakyMasterGetStorage(failFromCall = 2)
    storage = flaky
    seedMaster(99)
    val old = scopedPrefix + "uuid-old/data/orders/0/old.json"
    writeTemp(old, ageSecondsAgo = ageSeconds + 60)

    val im = buildIndexManager(flaky)
    try {
      // open()'s own master read is the first GET and succeeds; the sweep's is the second and fails.
      im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(99)))
      // Fail closed: without a readable lock the sweep cannot tell an orphan from an in-flight
      // Copy source, so it defers to the next cycle instead of deleting.
      storage.snapshot(bucket).keys should contain(old)
    } finally im.close()
  }

  test("[B] the periodic sweep reaps aged orphans but keeps a live batch's referenced temps") {
    seedMaster(99)
    val orphan    = scopedPrefix + "uuid-orphan/data/orders/0/orphan.json"
    val inFlight  = scopedPrefix + "uuid-live/data/orders/0/live.json"
    val finalPath = "data/orders/0/live.json"

    val im = buildIndexManager()
    try {
      im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(99)))
      // Staged after open() so the at-open sweep does not reap them first: this models a
      // long-running task whose batch has been in flight longer than gcSweepMinAgeSeconds.
      writeTemp(orphan, ageSecondsAgo   = ageSeconds + 60)
      writeTemp(inFlight, ageSecondsAgo = ageSeconds + 60)
      val liveETag = storage.snapshot(bucket)(inFlight).eTag
      // A live batch commit: CAS a PendingState referencing the in-flight temp, exactly as
      // `WriterCommitManager.commitBatch` does before driving the copy chain.
      im.update(
        tp,
        Some(Offset(99)),
        Some(PendingState(Offset(150), NonEmptyList.of(CopyOperation(bucket, inFlight, finalPath, liveETag)))),
      ).value

      im.sweepAllBatchTemps()

      storage.snapshot(bucket).keys should contain(inFlight)
      storage.snapshot(bucket).keys should not contain orphan
    } finally im.close()
  }

  test("[NL] the periodic sweep does not delete a live batch's referenced temp when the master read fails") {
    val flaky = new FlakyMasterGetStorage()
    storage = flaky
    seedMaster(99)
    val inFlight  = scopedPrefix + "uuid-live/data/orders/0/live.json"
    val finalPath = "data/orders/0/live.json"

    val im = buildIndexManager(flaky)
    try {
      im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(99)))
      writeTemp(inFlight, ageSecondsAgo = ageSeconds + 60)
      val liveETag = storage.snapshot(bucket)(inFlight).eTag
      im.update(
        tp,
        Some(Offset(99)),
        Some(PendingState(Offset(150), NonEmptyList.of(CopyOperation(bucket, inFlight, finalPath, liveETag)))),
      ).value

      // The lock read now fails transiently. Reading that as "no pending operations" would make
      // the sweep delete the source of a Copy the commit chain is about to run, after which
      // `mvFile` finds neither source nor destination and every later open() fails fatally on
      // the same PendingState.
      flaky.failMasterGet = true
      im.sweepAllBatchTemps()

      storage.snapshot(bucket).keys should contain(inFlight)
    } finally im.close()
  }

  // ---------------------------------------------------------------------------------------------
  // LC-349 M': the sweep's deletes are serialised with commits per partition (per-chunk lock,
  // fresh master read, owner fence). Deterministic: latches and storage hooks, no sleeps; bounded
  // waits are deadlock guards only.
  // ---------------------------------------------------------------------------------------------

  /**
   * Storage with seams for the M' scenarios:
   *  - `afterTempList` runs once, right after the next LIST of a `.temp-upload/` prefix returns,
   *    i.e. between the sweep's LIST and its first delete chunk;
   *  - `onTempDelete` runs inside every `deleteFiles` of temp keys, before the delete is applied;
   *  - `failMasterGet` turns every master-lock GET into a transient failure.
   */
  private class HookedStorage extends InMemoryStorageInterface {
    val tempDeletes = new ConcurrentLinkedQueue[Seq[String]]()
    val deleting    = new AtomicBoolean(false)
    val afterTempList:           AtomicReference[() => Unit]          = new AtomicReference(() => ())
    val onTempDelete:            AtomicReference[Seq[String] => Unit] = new AtomicReference(_ => ())
    @volatile var failMasterGet: Boolean                              = false

    override def listFileMetaRecursive(
      b:      String,
      prefix: Option[String],
    ): Either[FileListError, Option[ListOfMetadataResponse[FakeFileMetadata]]] = {
      val result = super.listFileMetaRecursive(b, prefix)
      if (prefix.exists(_.startsWith(".temp-upload/"))) afterTempList.getAndSet(() => ())()
      result
    }

    override def deleteFiles(b: String, files: Seq[String]): Either[FileDeleteError, Unit] =
      if (files.exists(_.startsWith(".temp-upload/"))) {
        deleting.set(true)
        try {
          tempDeletes.add(files)
          onTempDelete.get()(files)
          super.deleteFiles(b, files)
        } finally deleting.set(false)
      } else super.deleteFiles(b, files)

    override def getBlobAsObject[O](
      b: String,
      p: String,
    )(
      implicit
      d: Decoder[O],
    ): Either[FileLoadError, ObjectWithETag[O]] =
      if (p == masterPath && failMasterGet) GeneralFileLoadError(new RuntimeException("boom"), p).asLeft
      else super.getBlobAsObject(b, p)

    def tempDeleteCalls: List[Seq[String]] = tempDeletes.asScala.toList
  }

  private def buildLockedIndexManager(
    st:             InMemoryStorageInterface,
    locks:          BatchCommitLocks,
    taskId:         ConnectorTaskId = connectorTaskId,
    batchSize:      Int             = IndexManagerV2.DefaultGcBatchSize,
    sweepEnabled:   Boolean         = false,
    sweepIntervalS: Int             = ageSeconds,
    minAgeS:        Int             = ageSeconds,
  ): IndexManagerV2 = {
    implicit val si: InMemoryStorageInterface = st
    new IndexManagerV2(
      bucketAndPrefixFn           = bucketAndPrefix,
      pendingOperationsProcessors = new PendingOperationsProcessors(st),
      directoryFileName           = directoryName,
      gcIntervalSeconds           = Int.MaxValue,
      gcBatchSize                 = batchSize,
      gcSweepIntervalSeconds      = sweepIntervalS,
      gcSweepMinAgeSeconds        = minAgeS,
      gcSweepEnabled              = sweepEnabled,
      commitMode                  = CommitMode.Batch,
      batchCommitLocks            = locks,
    )(si, taskId)
  }

  /** Deadlock guard: waits for `cond` without sleeping, failing after a generous bound. */
  private def awaitCondition(what: String)(cond: => Boolean): Unit = {
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
    while (!cond) {
      if (System.nanoTime() > deadline) fail(s"deadlock guard: timed out waiting for $what")
      LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100))
    }
  }

  /** Runs one sweep pass on its own thread and records how it ended. */
  private class SweepThread(im: IndexManagerV2) extends Thread("m-prime-sweep") {
    private val done = new CountDownLatch(1)
    val escaped:                  AtomicReference[Option[Throwable]] = new AtomicReference(None)
    val interruptedFlagAfterPass: AtomicBoolean                      = new AtomicBoolean(false)

    override def run(): Unit =
      try {
        im.sweepAllBatchTemps()
        interruptedFlagAfterPass.set(Thread.currentThread().isInterrupted)
      } catch {
        case t: Throwable => escaped.set(Some(t))
      } finally done.countDown()

    def finished: Boolean = done.getCount == 0

    def awaitFinished(): Unit = {
      done.await(10, TimeUnit.SECONDS) shouldBe true
      val _ = escaped.get() shouldBe None
    }
  }

  private def copyOf(temp: String, finalPath: String, st: InMemoryStorageInterface): PendingState =
    PendingState(Offset(150), NonEmptyList.of(CopyOperation(bucket, temp, finalPath, st.snapshot(bucket)(temp).eTag)))

  private def warningsDuring[A](f: => A): (A, List[String]) = {
    // Resolve the logger through the logback context, never by casting `getLogger`'s result: while
    // a suite on another thread is still initialising SLF4J, `getLogger` hands out a
    // `SubstituteLogger`. That initialisation runs under `LoggerFactory`'s class monitor, so taking
    // the monitor waits for it to finish and the factory is then the bound `LoggerContext`.
    val context =
      classOf[LoggerFactory].synchronized(LoggerFactory.getILoggerFactory).asInstanceOf[LoggerContext]
    val logger   = context.getLogger(classOf[IndexManagerV2])
    val appender = new ListAppender[ILoggingEvent]()
    appender.start()
    logger.addAppender(appender)
    try {
      val a = f
      (a, appender.list.asScala.toList.filter(_.getLevel == Level.WARN).map(_.getFormattedMessage))
    } finally {
      val _ = logger.detachAppender(appender)
    }
  }

  private val orphan   = scopedPrefix + "uuid-orphan/data/orders/0/orphan.json"
  private val inFlight = scopedPrefix + "uuid-live/data/orders/0/live.json"
  private val liveDest = "data/orders/0/live.json"

  test("[M'1] a commit holding the partition lock makes the sweep's delete wait, then spares what it referenced") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    val locks = BatchCommitLocks()
    val im    = buildLockedIndexManager(st, locks)
    try {
      im.open(Set(tp)).value
      writeTemp(orphan, ageSecondsAgo   = ageSeconds + 60)
      writeTemp(inFlight, ageSecondsAgo = ageSeconds + 60)

      val lock  = locks.lockFor(tp)
      val sweep = new SweepThread(im)
      lock.lock() // a commit in flight on tp
      try {
        sweep.start()
        awaitCondition("the sweep to queue on the partition lock or finish")(lock.hasQueuedThreads || sweep.finished)
        st.tempDeleteCalls shouldBe empty
        // The commit CASes a PendingState referencing a temp the sweep has already listed.
        im.update(tp, Some(Offset(99)), Some(copyOf(inFlight, liveDest, st))).value
      } finally lock.unlock()
      sweep.awaitFinished()

      st.snapshot(bucket).keys should contain(inFlight)
      st.snapshot(bucket).keys should not contain orphan
    } finally im.close()
  }

  test("[M'2] a CAS that lands between the sweep's LIST and its delete spares the referenced temp") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      im.open(Set(tp)).value
      writeTemp(orphan, ageSecondsAgo   = ageSeconds + 60)
      writeTemp(inFlight, ageSecondsAgo = ageSeconds + 60)
      st.afterTempList.set { () =>
        val _ = im.update(tp, Some(Offset(99)), Some(copyOf(inFlight, liveDest, st))).value
      }

      im.sweepAllBatchTemps()

      st.snapshot(bucket).keys should contain(inFlight)
      st.snapshot(bucket).keys should not contain orphan
    } finally im.close()
  }

  // Proves chunk-granular interleaving only: the sweep releases the partition lock between chunks,
  // so a commit that queued during one chunk runs before the remaining chunks and never overlaps a
  // delete. It does not prove fairness (a non-fair lock also passes here); that the lock is fair is
  // pinned by "the lock is fair" in BatchCommitLocksTest.
  test("[M'4] with gcBatchSize=1 the sweep releases the lock between chunks, so a queued commit runs in between") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    val locks = BatchCommitLocks()
    val im    = buildLockedIndexManager(st, locks, batchSize = 1)
    try {
      im.open(Set(tp)).value
      val orphans = (1 to 3).map(i => scopedPrefix + s"uuid-$i/data/orders/0/o$i.json")
      orphans.foreach(writeTemp(_, ageSecondsAgo = ageSeconds + 60))

      val lock          = locks.lockFor(tp)
      val events        = new ConcurrentLinkedQueue[String]()
      val firstChunkIn  = new CountDownLatch(1)
      val commitDone    = new AtomicBoolean(false)
      val commitOverlap = new AtomicBoolean(false)
      val commit = new Thread(() => {
        if (firstChunkIn.await(10, TimeUnit.SECONDS)) {
          locks.withLock(tp) {
            commitOverlap.set(st.deleting.get())
            events.add("commit")
          }
        }
        commitDone.set(true)
      })
      val first = new AtomicBoolean(true)
      st.onTempDelete.set { files =>
        events.add(s"delete:${files.mkString(",")}")
        if (first.getAndSet(false)) {
          firstChunkIn.countDown()
          awaitCondition("the commit to queue on the partition lock")(lock.hasQueuedThreads || commitDone.get())
        }
      }
      commit.start()

      im.sweepAllBatchTemps()
      commit.join(TimeUnit.SECONDS.toMillis(10))

      commitOverlap.get() shouldBe false
      events.asScala.toList shouldBe List(
        s"delete:${orphans(0)}",
        "commit",
        s"delete:${orphans(1)}",
        s"delete:${orphans(2)}",
      )
    } finally im.close()
  }

  test("[M'5] a sweep by an instance that no longer owns the master lock deletes nothing and warns") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    // A zombie / stale instance of the same connector task: same name, so the same temp prefix,
    // but its own lockUuid. It opened first; the current owner's open() then bumped the owner.
    val zombieTaskId = ConnectorTaskId(connectorTaskId.name, connectorTaskId.maxTasks, connectorTaskId.taskNo)
    zombieTaskId.lockUuid should not be connectorTaskId.lockUuid
    val zombie = buildLockedIndexManager(st, BatchCommitLocks(), taskId = zombieTaskId)
    val owner  = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      zombie.open(Set(tp)).value
      owner.open(Set(tp)).value
      writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)

      val (_, warnings) = warningsDuring(zombie.sweepAllBatchTemps())

      st.tempDeleteCalls shouldBe empty
      st.snapshot(bucket).keys should contain(orphan)
      warnings.exists(_.contains("lock owned by another instance; batch temp sweep skipped")) shouldBe true

      // The owner's own sweep passes the fence.
      owner.sweepAllBatchTemps()
      st.snapshot(bucket).keys should not contain orphan
    } finally {
      zombie.close()
      owner.close()
    }
  }

  test("[M'6] a periodic sweep deletes nothing once the partition is closed or its state cleared, before or mid-pass") {
    val cases: List[(String, (IndexManagerV2, HookedStorage) => Unit)] = List(
      "closed before the pass" -> ((im, _) => im.closePartitions(Set(tp))),
      "closed between LIST and delete" ->
        ((im, st) => st.afterTempList.set(() => im.closePartitions(Set(tp)))),
      "state cleared between LIST and delete" ->
        ((im, st) => st.afterTempList.set(() => im.clearTopicPartitionState(tp))),
    )
    cases.foreach {
      case (label, arrange) =>
        withClue(label) {
          val st = new HookedStorage
          storage = st
          seedMaster(99)
          val im = buildLockedIndexManager(st, BatchCommitLocks())
          try {
            im.open(Set(tp)).value
            writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)
            arrange(im, st)

            im.sweepAllBatchTemps()

            st.tempDeleteCalls shouldBe empty
            st.snapshot(bucket).keys should contain(orphan)
          } finally im.close()
        }
    }
  }

  test("[M'6b] a partition closed before the pass costs the periodic sweep no master read and no LIST") {
    val masterGets = new ConcurrentLinkedQueue[String]()
    val tempLists  = new ConcurrentLinkedQueue[String]()
    val st = new HookedStorage {
      override def listFileMetaRecursive(
        b:      String,
        prefix: Option[String],
      ): Either[FileListError, Option[ListOfMetadataResponse[FakeFileMetadata]]] = {
        prefix.filter(_.startsWith(".temp-upload/")).foreach(tempLists.add)
        super.listFileMetaRecursive(b, prefix)
      }

      override def getBlobAsObject[O](
        b: String,
        p: String,
      )(
        implicit
        d: Decoder[O],
      ): Either[FileLoadError, ObjectWithETag[O]] = {
        if (p == masterPath) masterGets.add(p)
        super.getBlobAsObject(b, p)
      }
    }
    storage = st
    seedMaster(99)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      im.open(Set(tp)).value
      writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)
      im.closePartitions(Set(tp))
      masterGets.clear()
      tempLists.clear()

      im.sweepAllBatchTemps()

      masterGets shouldBe empty
      tempLists shouldBe empty
      st.tempDeleteCalls shouldBe empty
      st.snapshot(bucket).keys should contain(orphan)
    } finally im.close()
  }

  test("[M'7] an unreadable master lock on the per-chunk fresh read stops the pass") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      im.open(Set(tp)).value
      writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)
      // The pre-LIST read succeeds; the fresh read under the lock fails.
      st.afterTempList.set(() => st.failMasterGet = true)

      im.sweepAllBatchTemps()

      st.tempDeleteCalls shouldBe empty
      st.snapshot(bucket).keys should contain(orphan)
    } finally im.close()
  }

  test("[M'8a] an interrupt while waiting on the partition lock issues no delete and restores the flag") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    val locks = BatchCommitLocks()
    val im    = buildLockedIndexManager(st, locks)
    try {
      im.open(Set(tp)).value
      writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)
      val lock  = locks.lockFor(tp)
      val sweep = new SweepThread(im)
      lock.lock()
      try {
        sweep.start()
        awaitCondition("the sweep to queue on the partition lock or finish")(lock.hasQueuedThreads || sweep.finished)
        sweep.interrupt()
        sweep.awaitFinished()
      } finally lock.unlock()

      st.tempDeleteCalls shouldBe empty
      st.snapshot(bucket).keys should contain(orphan)
      sweep.interruptedFlagAfterPass.get() shouldBe true
    } finally im.close()
  }

  test("[M'8b] close() interrupts a scheduled sweep waiting on the partition lock; no delete is issued") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    val locks  = BatchCommitLocks()
    val im     = buildLockedIndexManager(st, locks, sweepEnabled = true, sweepIntervalS = 1, minAgeS = 1)
    val lock   = locks.lockFor(tp)
    var closed = false
    try {
      im.open(Set(tp)).value
      val exec = im.sweepExecutorOpt.getOrElse(fail("the periodic sweep executor was not started"))
      lock.lock()
      try {
        writeTemp(orphan, ageSecondsAgo = 60)
        awaitCondition("the scheduled sweep to queue on the partition lock or delete")(
          lock.hasQueuedThreads || !st.tempDeletes.isEmpty,
        )
        im.close() // shutdownNow interrupts the waiting sweep thread
        closed = true
        exec.isTerminated shouldBe true
      } finally lock.unlock()

      st.tempDeleteCalls shouldBe empty
      st.snapshot(bucket).keys should contain(orphan)
    } finally if (!closed) im.close()
  }

  test("[M'8c] an interrupt while waiting on one partition's lock stops the pass: the next partition gets no I/O") {
    val tp1        = Topic("orders").withPartition(1)
    val masterGets = new ConcurrentLinkedQueue[String]()
    val tempLists  = new ConcurrentLinkedQueue[String]()
    val st = new HookedStorage {
      override def listFileMetaRecursive(
        b:      String,
        prefix: Option[String],
      ): Either[FileListError, Option[ListOfMetadataResponse[FakeFileMetadata]]] = {
        prefix.filter(_.startsWith(".temp-upload/")).foreach(tempLists.add)
        super.listFileMetaRecursive(b, prefix)
      }

      override def getBlobAsObject[O](
        b: String,
        p: String,
      )(
        implicit
        d: Decoder[O],
      ): Either[FileLoadError, ObjectWithETag[O]] = {
        if (p.endsWith(".lock")) masterGets.add(p)
        super.getBlobAsObject(b, p)
      }
    }
    storage = st
    val partitions = List(tp, tp1)
    def lockPath(t:   TopicPartition) = IndexManagerV2.generateLockFilePath(connectorTaskId, t, directoryName)
    def tempPrefix(t: TopicPartition) = s".temp-upload/${connectorTaskId.name}/${t.topic}/${t.partition}/"
    partitions.foreach { t =>
      val _ = storage.writeBlobToFile(
        bucket,
        lockPath(t),
        NoOverwriteExistingObject(IndexFile("prev-owner", Some(Offset(99)), None)),
      ).value
    }
    val locks = BatchCommitLocks()
    val im    = buildLockedIndexManager(st, locks)
    try {
      im.open(partitions.toSet).value
      partitions.foreach(t => writeTemp(tempPrefix(t) + s"uuid-o/data/orders/${t.partition}/o.json", ageSeconds + 60))
      masterGets.clear()
      tempLists.clear()

      // Hold both partitions' locks: the pass visits partitions in an unspecified order, and
      // whichever comes first must block on its lock.
      val partitionLocks = partitions.map(t => t -> locks.lockFor(t))
      val sweep          = new SweepThread(im)
      partitionLocks.foreach(_._2.lock())
      val first =
        try {
          sweep.start()
          awaitCondition("the sweep to queue on a partition lock")(partitionLocks.exists(_._2.hasQueuedThreads))
          val queuedOn = partitionLocks.collectFirst { case (t, l) if l.hasQueuedThreads => t }.get
          sweep.interrupt()
          sweep.awaitFinished()
          queuedOn
        } finally partitionLocks.foreach(_._2.unlock())
      val next = partitions.filterNot(_ == first).head

      // The first partition was read and listed, then the interrupt stopped the pass before the
      // next partition issued its pre-LIST master read or its LIST.
      masterGets.asScala.toList shouldBe List(lockPath(first))
      tempLists.asScala.toList shouldBe List(tempPrefix(first))
      masterGets.asScala.toList should not contain lockPath(next)
      tempLists.asScala.toList should not contain tempPrefix(next)
      st.tempDeleteCalls shouldBe empty
      sweep.interruptedFlagAfterPass.get() shouldBe true
    } finally im.close()
  }

  // ---------------------------------------------------------------------------------------------
  // LC-349 S2b: the per-chunk ownership check is an explicit owned-partitions set. `open(tp)` adds
  // tp before recovery and the open() sweep; `closePartitions` (close(partitions)) and close()
  // remove it, and open() never does. The same check applies to the open() and periodic sweeps.
  // ---------------------------------------------------------------------------------------------

  test("[S2b-1] a partition closed mid-pass is not swept by the open() sweep") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      // close(partitions) on the task releases tp between the open() sweep's LIST and its delete.
      st.afterTempList.set(() => im.closePartitions(Set(tp)))

      im.open(Set(tp)).value shouldBe Map(tp -> Some(Offset(99)))

      st.tempDeleteCalls shouldBe empty
      st.snapshot(bucket).keys should contain(orphan)
    } finally im.close()
  }

  test("[S2b-2] a closed partition is not swept by the periodic sweep, even while work is accepted") {
    val st    = new HookedStorage
    val other = Topic("orders").withPartition(1)
    storage = st
    seedMaster(99)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      im.open(Set(tp, other)).value
      writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)
      // A cooperative revoke of tp alone: the task keeps `other`, so work stays accepted.
      im.closePartitions(Set(tp))
      im.acceptingWork shouldBe true
      im.ownedPartitions.asScala.toSet shouldBe Set(other)
      im.getSeekedOffsetForTopicPartition(tp) shouldBe None

      im.sweepAllBatchTemps()

      st.tempDeleteCalls shouldBe empty
      st.snapshot(bucket).keys should contain(orphan)
    } finally im.close()
  }

  test("[S2b-3] a brand-new partition with no committed offset is swept during its own open() on a fresh instance") {
    val st = new HookedStorage
    storage = st
    // No master lock at all: open() creates it. The partition never committed, so it is not in
    // seekedOffsets, and acceptingWork is false throughout open().
    writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      im.acceptingWork shouldBe false
      im.open(Set(tp)).value shouldBe Map(tp -> None)

      st.snapshot(bucket).keys should not contain orphan
      st.tempDeleteCalls should have size 1
    } finally im.close()
  }

  test("[S2b-4] revoke then reassign makes the partition sweepable again") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      im.open(Set(tp)).value
      writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)
      im.closePartitions(Set(tp))
      im.sweepAllBatchTemps()
      st.snapshot(bucket).keys should contain(orphan)

      // Reassigned: the open() sweep reaps it...
      im.open(Set(tp)).value
      st.snapshot(bucket).keys should not contain orphan

      // ...and so does the periodic sweep afterwards.
      val later = scopedPrefix + "uuid-later/data/orders/0/later.json"
      writeTemp(later, ageSecondsAgo = ageSeconds + 60)
      im.sweepAllBatchTemps()
      st.snapshot(bucket).keys should not contain later
    } finally im.close()
  }

  test("[S2b-5] only closePartitions removes a partition from the owned set; open() of another set does not") {
    val st    = new HookedStorage
    val other = Topic("orders").withPartition(1)
    storage = st
    seedMaster(99)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      im.open(Set(tp)).value
      im.ownedPartitions.asScala.toSet shouldBe Set(tp)
      // A cooperative rebalance that adds `other` passes open() only the added partition: tp is kept.
      im.open(Set(other)).value
      im.ownedPartitions.asScala.toSet shouldBe Set(tp, other)
      // close(partitions) is the only source of truth for a revoke.
      im.closePartitions(Set(tp))
      im.ownedPartitions.asScala.toSet shouldBe Set(other)
    } finally im.close()
  }

  test("[S2b-6] close() clears the owned set: a sweep running after close deletes nothing") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    im.open(Set(tp)).value
    writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)

    im.close()
    im.ownedPartitions shouldBe empty
    // A sweep thread that outlived close() (close waits only 5 s) runs its next chunk.
    im.sweepAllBatchTemps()

    st.tempDeleteCalls shouldBe empty
    st.snapshot(bucket).keys should contain(orphan)
  }

  test("[S2b-7] a missing master lock on the fresh read still deletes the chunk (unreferenced temps are orphans)") {
    val st = new HookedStorage
    storage = st
    seedMaster(99)
    val im = buildLockedIndexManager(st, BatchCommitLocks())
    try {
      im.open(Set(tp)).value
      writeTemp(orphan, ageSecondsAgo = ageSeconds + 60)
      st.afterTempList.set { () =>
        val _ = st.deleteFiles(bucket, Seq(masterPath))
      }

      im.sweepAllBatchTemps()

      st.snapshot(bucket).keys should not contain orphan
    } finally im.close()
  }
}
