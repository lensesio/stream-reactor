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
import io.lenses.streamreactor.connect.cloud.common.sink.SinkError
import io.lenses.streamreactor.connect.cloud.common.sink.commit.CommitPolicy
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.config.PartitionNamePath
import io.lenses.streamreactor.connect.cloud.common.sink.config.ValuePartitionField
import io.lenses.streamreactor.connect.cloud.common.sink.naming.KeyNamer
import io.lenses.streamreactor.connect.cloud.common.sink.naming.ObjectKeyBuilder
import io.lenses.streamreactor.connect.cloud.common.sink.seek.IndexManager
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingOperationsProcessors
import io.lenses.streamreactor.connect.cloud.common.sink.seek.PendingState
import io.lenses.streamreactor.connect.cloud.common.storage.FileMetadata
import io.lenses.streamreactor.connect.cloud.common.utils.SampleData
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.mockito.ArgumentMatchersSugar
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.immutable
import scala.collection.mutable

object WriterManagerOffsetInvariantsScenarioTest {

  sealed trait Op
  final case class Write(tp: TopicPartition, pk: Option[String], offset: Long) extends Op
  final case class Commit(tp: TopicPartition) extends Op
  final case class PreCommit(tp: TopicPartition, kafkaOffset: Long) extends Op
  case object Rebalance extends Op

  sealed trait ShadowState
  final case class ShadowNoWriter(committed: Option[Long]) extends ShadowState
  final case class ShadowWriting(committed: Option[Long], uncommitted: Long) extends ShadowState
}

/**
 * Scenario-based test for the offset contract WriterManager.preCommit exposes to Kafka Connect.
 *
 * Why
 *   preCommit is what tells Kafka "you can forget records below this offset -- I have them
 *   safe." If that number regresses, or gets ahead of what is durably persisted in the
 *   IndexManager, the connector either duplicates or drops records on restart. The
 *   computation combines per-writer buffered offsets, per-writer committed offsets, a
 *   per-partition high watermark, and the master-lock offset read back from storage, and
 *   the interleavings of writes, commits, preCommits, and rebalances make it hard to
 *   reason about with single-call example tests alone.
 *
 * What
 *   Each test in this file is a named scenario: a short narrative comment, a list of
 *   Write / Commit / PreCommit / Rebalance ops, and an explicit assertion on the sequence
 *   of offsets preCommit returned. Two invariants are also checked automatically after
 *   every op, so the scenario's explicit expectations and the production math cannot
 *   silently drift apart:
 *
 *     1. Monotonicity. The offset K returned by preCommit for a given TopicPartition does
 *        not decrease within an assignment, and the first K after a rebalance is at least
 *        the durable offset at the moment the rebalance happened.
 *     2. No over-commit. K is never below M, the offset the IndexManager has durably
 *        recorded. We must never rewind Kafka below what storage considers committed,
 *        which would cause duplicate delivery on restart.
 *
 * How
 *   An Op DSL and a lightweight Harness drive a real WriterManager against an in-memory
 *   StubIndexManager. The stub mirrors the subset of IndexManagerV2 semantics preCommit
 *   depends on (master-lock reads and writes, granular writes). Each scenario fixes each
 *   TopicPartition to a single mode (PARTITIONBY or non-PARTITIONBY) so the setup reads
 *   as a realistic deployment state; the collection of scenarios covers both modes.
 *
 *   End-to-end verification that bytes actually land in cloud storage needs a storage
 *   fake and is out of scope here.
 */
class WriterManagerOffsetInvariantsScenarioTest
    extends AnyFunSuite
    with Matchers
    with MockitoSugar
    with ArgumentMatchersSugar {

  import WriterManagerOffsetInvariantsScenarioTest._

  private implicit val connectorTaskId:        ConnectorTaskId        = ConnectorTaskId("test-connector", 1, 0)
  private implicit val cloudLocationValidator: CloudLocationValidator = SampleData.cloudLocationValidator

  private val commitPolicy         = mock[CommitPolicy]
  private val formatWriter         = mock[FormatWriter]
  private val objectKeyBuilder     = mock[ObjectKeyBuilder]
  private val schemaChangeDetector = mock[SchemaChangeDetector]
  private val pendingOpsProcessors = mock[PendingOperationsProcessors]

  private val dateField: PartitionField = ValuePartitionField(PartitionNamePath("date"))

  private val tp0 = Topic("topic").withPartition(0)
  private val tp1 = Topic("topic").withPartition(1)

  // ---- Stub IndexManager ------------------------------------------------------

  /**
   * Minimal in-memory IndexManager. Only `getSeekedOffsetForTopicPartition` and
   * `updateMasterLock` drive `preCommit`'s offset math; the granular and lifecycle
   * methods are provided to satisfy the trait and to let rebalance scenarios survive
   * a fresh WriterManager instance.
   *
   * Semantics mirror `IndexManagerV2` on the success path only -- no eTag enforcement,
   * no lock files, no storage I/O.
   */
  final class StubIndexManager extends IndexManager {

    val master:   mutable.Map[TopicPartition, Long]           = mutable.Map.empty
    val granular: mutable.Map[(TopicPartition, String), Long] = mutable.Map.empty

    override def indexingEnabled: Boolean = true

    override def open(
      topicPartitions: Set[TopicPartition],
    ): Either[SinkError, Map[TopicPartition, Option[Offset]]] =
      Right(topicPartitions.iterator.map(tp => tp -> master.get(tp).map(Offset(_))).toMap)

    override def update(
      topicPartition:  TopicPartition,
      committedOffset: Option[Offset],
      pendingState:    Option[PendingState],
    ): Either[SinkError, Option[Offset]] = {
      val _ = pendingState
      committedOffset.foreach(o => master.put(topicPartition, o.value))
      Right(committedOffset)
    }

    override def getSeekedOffsetForTopicPartition(topicPartition: TopicPartition): Option[Offset] =
      master.get(topicPartition).map(Offset(_))

    override def getSeekedOffsetForPartitionKey(
      topicPartition: TopicPartition,
      partitionKey:   String,
    ): Either[SinkError, Option[Offset]] =
      Right(granular.get((topicPartition, partitionKey)).map(Offset(_)))

    override def updateForPartitionKey(
      topicPartition:  TopicPartition,
      partitionKey:    String,
      committedOffset: Option[Offset],
      pendingState:    Option[PendingState],
    ): Either[SinkError, Option[Offset]] = {
      val _ = pendingState
      committedOffset.foreach(o => granular.put((topicPartition, partitionKey), o.value))
      Right(committedOffset)
    }

    override def updateMasterLock(
      topicPartition:   TopicPartition,
      globalSafeOffset: Offset,
    ): Either[SinkError, Unit] = {
      // Mirrors IndexManagerV2: records `globalSafeOffset - 1` as the committedOffset.
      master.put(topicPartition, globalSafeOffset.value - 1)
      Right(())
    }

    override def cleanUpObsoleteLocks(
      topicPartition:      TopicPartition,
      globalSafeOffset:    Offset,
      activePartitionKeys: Set[String],
    ): Either[SinkError, Unit] = Right(())

    override def ensureGranularLock(
      topicPartition: TopicPartition,
      partitionKey:   String,
    ): Either[SinkError, Unit] = Right(())

    override def evictGranularLock(topicPartition:        TopicPartition, partitionKey: String): Unit = ()
    override def evictAllGranularLocks(topicPartition:    TopicPartition): Unit = ()
    override def clearTopicPartitionState(topicPartition: TopicPartition): Unit = ()
    override def suspendBackgroundWork(): Unit = ()
    override def close():                 Unit = ()

    def masterNextOffset(tp: TopicPartition): Long = master.get(tp).map(_ + 1L).getOrElse(0L)
  }

  // ---- Writer + WriterManager builders ---------------------------------------

  private def makeWriter(
    tp:              TopicPartition,
    committedOffset: Option[Offset],
    partitionKey:    Option[String],
  ): Writer[FileMetadata] = {
    val im = mock[IndexManager]
    when(im.indexingEnabled).thenReturn(true)
    new Writer[FileMetadata](
      tp,
      commitPolicy,
      im,
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
    partitionKey:    Option[String],
  ): Writer[FileMetadata] = {
    val w = makeWriter(tp, committedOffset, partitionKey)
    w.forceWriteState(NoWriter(CommitState(tp, committedOffset)))
    w
  }

  private def writerInWritingState(
    tp:                  TopicPartition,
    committedOffset:     Option[Offset],
    firstBufferedOffset: Offset,
    uncommittedOffset:   Offset,
    partitionKey:        Option[String],
  ): Writer[FileMetadata] = {
    val w = makeWriter(tp, committedOffset, partitionKey)
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

  private def buildWriterManager(im: IndexManager): WriterManager[FileMetadata] =
    new WriterManager[FileMetadata](
      commitPolicyFn              = _ => Right(commitPolicy),
      bucketAndPrefixFn           = _ => Right(CloudLocation("bucket", None)),
      keyNamerFn                  = _ => Right(mock[KeyNamer]),
      stagingFilenameFn           = (_, _) => Right(new File("test")),
      objKeyBuilderFn             = (_, _) => objectKeyBuilder,
      formatWriterFn              = (_, _) => Right(formatWriter),
      indexManager                = im,
      transformerF                = (m: MessageDetail) => Right(m),
      schemaChangeDetector        = schemaChangeDetector,
      skipNullValues              = false,
      pendingOperationsProcessors = pendingOpsProcessors,
    )

  private def partitionValuesFor(pk: Option[String]): immutable.Map[PartitionField, String] =
    pk match {
      case Some(v) => Map(dateField -> v)
      case None    => Map.empty
    }

  // ---- Harness ---------------------------------------------------------------

  /**
   * Drives a real WriterManager through an Op sequence. Invariants are asserted inline
   * after every `PreCommit` so a failure points directly at the offending op rather than
   * a post-run aggregate. Tests read `h.ks(tp)` for the sequence of offsets preCommit
   * returned.
   */
  private final class Harness(private var wm: WriterManager[FileMetadata], private val stub: StubIndexManager) {

    private val shadow: mutable.Map[(TopicPartition, Option[String]), ShadowState] = mutable.Map.empty

    /** All K values observed per tp across the whole run (test-readable). */
    val ks: mutable.Map[TopicPartition, mutable.ListBuffer[Long]] = mutable.Map.empty

    private val currentAssignmentLastK: mutable.Map[TopicPartition, Long] = mutable.Map.empty

    private val pendingRebalanceFloor: mutable.Map[TopicPartition, Long] = mutable.Map.empty

    def runOp(op: Op): Unit = op match {
      case Write(tp, pk, off) =>
        val keyShadow = (tp, pk)
        // Mirror what `createWriter` seeds on the real stack: an existing shadow entry
        // wins (carries committedOffset across re-opens); otherwise fall back to the stub
        // granular map, then the master map. This is what lets post-rebalance writers
        // still produce a committedOffset on their first preCommit so the
        // `committedOffsets.isEmpty` short-circuit in `getOffsetAndMeta` does not swallow
        // the offset we want to assert on.
        val shadowCommitted = shadow.get(keyShadow).collect {
          case ShadowNoWriter(c)   => c
          case ShadowWriting(c, _) => c
        }.flatten
        val seedCommitted = shadowCommitted.orElse {
          pk match {
            case Some(pkStr) => stub.granular.get((tp, pkStr)).orElse(stub.master.get(tp))
            case None        => stub.master.get(tp)
          }
        }
        val w = writerInWritingState(
          tp,
          committedOffset     = seedCommitted.map(Offset(_)),
          firstBufferedOffset = Offset(off),
          uncommittedOffset   = Offset(off),
          partitionKey        = pk,
        )
        wm.putWriter(MapKey(tp, partitionValuesFor(pk)), w)
        shadow.update(keyShadow, ShadowWriting(seedCommitted, off))

      case Commit(tp) =>
        // Drive every Writing writer for `tp` to NoWriter at committedOffset=uncommittedOffset,
        // mirroring the real `Writer.commit` -> IndexManager update call. Non-PARTITIONBY
        // writers write to the master map; PARTITIONBY writers write to the granular map.
        val toCommit = shadow.collect {
          case ((t, pk), ShadowWriting(_, unc)) if t == tp => (pk, unc)
        }.toList
        toCommit.foreach {
          case (pk, unc) =>
            val w = writerInNoWriterState(tp, Some(Offset(unc)), pk)
            wm.putWriter(MapKey(tp, partitionValuesFor(pk)), w)
            shadow.update((tp, pk), ShadowNoWriter(Some(unc)))
            pk match {
              case None =>
                val _ = stub.update(tp, Some(Offset(unc)), None)
              case Some(pkStr) =>
                val _ = stub.updateForPartitionKey(tp, pkStr, Some(Offset(unc)), None)
            }
        }

      case PreCommit(tp, kafkaOffset) =>
        val mPrev = stub.masterNextOffset(tp)
        val res   = wm.preCommit(Map(tp -> new OffsetAndMetadata(kafkaOffset)))
        res.get(tp).foreach { oam =>
          val k = oam.offset()
          withClue(s"no-over-commit (K >= M) violated at PreCommit($tp, $kafkaOffset): K=$k M=$mPrev. ") {
            k should be >= mPrev
          }
          currentAssignmentLastK.get(tp).foreach { prev =>
            withClue(
              s"monotonicity violated within assignment at PreCommit($tp, $kafkaOffset): K=$k previous=$prev. ",
            ) {
              k should be >= prev
            }
          }
          currentAssignmentLastK.update(tp, k)
          ks.getOrElseUpdate(tp, mutable.ListBuffer.empty).append(k)
          pendingRebalanceFloor.remove(tp).foreach { floor =>
            withClue(s"post-rebalance regression at PreCommit($tp, $kafkaOffset): K=$k M_at_rebalance=$floor. ") {
              k should be >= floor
            }
          }
        }

      case Rebalance =>
        val tps = shadow.keys.map(_._1).toSet
        tps.foreach(tp => pendingRebalanceFloor.update(tp, stub.masterNextOffset(tp)))
        wm.close()
        shadow.clear()
        currentAssignmentLastK.clear()
        wm = buildWriterManager(stub)
    }
  }

  /**
   * Run an Op sequence against a fresh WriterManager + StubIndexManager and return the
   * Harness so scenarios can read `h.ks(tp)`. Invariants are enforced inline by `runOp`.
   */
  private def run(ops: List[Op]): Harness = runWith(Map.empty)(ops)

  /**
   * Run an Op sequence against a StubIndexManager pre-seeded with master offsets.
   * Useful for scenarios that want to simulate an existing durable state from a previous
   * task lifetime (e.g. master=9 before any op in this run is issued).
   */
  private def runWith(masterSeed: Map[TopicPartition, Long])(ops: List[Op]): Harness = {
    val stub = new StubIndexManager
    masterSeed.foreach { case (tp, off) => stub.master.put(tp, off) }
    val h = new Harness(buildWriterManager(stub), stub)
    ops.foreach(h.runOp)
    h
  }

  // ---- Scenarios --------------------------------------------------------------

  test("PARTITIONBY HWM holds the line when a high-offset writer is replaced by a lower one") {
    // pk-a commits at 500. preCommit returns 501, writes master=500, HWM caches 501.
    // pk-a is then replaced in-place by a writer at offset 30 (simulating eviction + recreate
    // without partition revocation). calculatedSafeOffset would be 30, but HWM keeps the
    // returned offset at 501 so Kafka never sees a regression.
    val ops = List[Op](
      Write(tp0, Some("pk-a"), 500),
      Commit(tp0),
      PreCommit(tp0, 5000), // K = 501
      Write(tp0, Some("pk-a"), 30),
      PreCommit(tp0, 5000), // K = 501 (HWM protects)
    )
    val h = run(ops)
    h.ks(tp0).toList shouldBe List(501L, 501L)
  }

  test("non-PARTITIONBY HWM holds the line across eviction + recreate (no updateMasterLock)") {
    // Empty partition values -> non-PARTITIONBY branch of getOffsetAndMeta, which does NOT
    // call updateMasterLock. The only thing standing between a lower-offset replacement
    // writer and a Kafka offset regression is the in-memory HWM cache.
    val ops = List[Op](
      Write(tp0, None, 100),
      Commit(tp0),
      PreCommit(tp0, 1000), // K = 101, master advanced to 100 by the Commit op
      Write(tp0, None, 20),
      PreCommit(tp0, 1000), // K = 101 (HWM protects; master unchanged)
    )
    val h = run(ops)
    h.ks(tp0).toList shouldBe List(101L, 101L)
  }

  test("PARTITIONBY multi-commit climbing sequence advances K with max committed + 1") {
    // Three successive commits on the same pk-a with strictly increasing offsets. K tracks
    // max(committed) + 1 at each round and master advances with it.
    val ops = List[Op](
      Write(tp0, Some("pk-a"), 10),
      Commit(tp0),
      PreCommit(tp0, 1000), // K = 11
      Write(tp0, Some("pk-a"), 50),
      Commit(tp0),
      PreCommit(tp0, 1000), // K = 51
      Write(tp0, Some("pk-a"), 200),
      Commit(tp0),
      PreCommit(tp0, 1000), // K = 201
    )
    val h = run(ops)
    h.ks(tp0).toList shouldBe List(11L, 51L, 201L)
  }

  test("commit + preCommit interleave: K never drops when a mid-sequence writer has a lower offset") {
    // A high-offset writer commits and advances K to 201. A later writer for the same pk
    // arrives at firstBuffered=50 -- calculatedSafeOffset would be 50, which is below
    // master+1. The HWM cache keeps K at 201.
    val ops = List[Op](
      Write(tp0, Some("pk-a"), 100),
      Commit(tp0),
      PreCommit(tp0, 10000), // K = 101
      Write(tp0, Some("pk-a"), 200),
      Commit(tp0),
      PreCommit(tp0, 10000), // K = 201
      Write(tp0, Some("pk-a"), 50),
      PreCommit(tp0, 10000), // K = 201 (HWM protects)
    )
    val h = run(ops)
    h.ks(tp0).toList shouldBe List(101L, 201L, 201L)
  }

  test("rebalance preserves the durable master: first post-rebalance preCommit respects master + 1") {
    // pk-a commits at 100 and preCommit advances master to 100. On rebalance the in-memory
    // HWM is cleared (WriterManager.close clears safeOffsetHighWatermarks); the fresh
    // WriterManager re-seeds HWM from `getSeekedOffsetForTopicPartition` on its first
    // preCommit, so K must be >= master + 1 = 101 even when the new writer is buffering
    // from a much lower firstBufferedOffset.
    val ops = List[Op](
      Write(tp0, Some("pk-a"), 100),
      Commit(tp0),
      PreCommit(tp0, 500), // K = 101 pre-rebalance
      Rebalance,
      Write(tp0, Some("pk-a"), 10), // new writer buffering from offset 10
      PreCommit(tp0, 500),          // K = 101 post-rebalance (master-derived floor)
    )
    val h = run(ops)
    h.ks(tp0).toList shouldBe List(101L, 101L)
  }

  test("rebalance preserves granular state: per-pk committedOffset is re-seeded from the stub") {
    // Two pks commit at different offsets pre-rebalance. Post-rebalance, fresh writers for
    // those pks must inherit their granular committedOffsets (50 and 70) rather than
    // starting empty -- otherwise `committedOffsets.isEmpty` in getOffsetAndMeta would
    // short-circuit and no offset would be reported. We then verify K climbs above both.
    val ops = List[Op](
      Write(tp0, Some("pk-a"), 50),
      Write(tp0, Some("pk-b"), 70),
      Commit(tp0),
      PreCommit(tp0, 1000), // K = 71 (max(50, 70) + 1), master -> 70
      Rebalance,
      Write(tp0, Some("pk-a"), 100),
      Write(tp0, Some("pk-b"), 120),
      PreCommit(tp0, 1000), // K = 100 (min firstBuffered), floor = master + 1 = 71
    )
    val h = run(ops)
    h.ks(tp0).toList shouldBe List(71L, 100L)
  }

  test("two TopicPartitions progress independently: commits on tp0 do not move tp1's K") {
    val ops = List[Op](
      Write(tp0, Some("pk-a"), 50),
      Write(tp1, Some("pk-a"), 200),
      Commit(tp0),
      Commit(tp1),
      PreCommit(tp0, 1000), // K_tp0 = 51
      PreCommit(tp1, 5000), // K_tp1 = 201
      Write(tp0, Some("pk-a"), 60),
      Commit(tp0),
      PreCommit(tp0, 1000), // K_tp0 = 61 (climbs)
      PreCommit(tp1, 5000), // K_tp1 = 201 (unchanged; no new commits on tp1)
    )
    val h = run(ops)
    h.ks(tp0).toList shouldBe List(51L, 61L)
    h.ks(tp1).toList shouldBe List(201L, 201L)
  }

  test("long monotone sequence: K non-decreasing as committed offsets dance up and down") {
    // Long-running sequence where each round replaces the pk-a writer with a fresh idle one
    // at varying committed offsets. The HWM must keep K monotone non-decreasing even when
    // committed offsets dip. Reproduces the expected sequence [51, 121, 121, 201, 201, 201,
    // 301] from WriterManagerPreCommitTest's "preCommit maintains monotone non-decreasing
    // globalSafeOffset across a long sequence", but expressed via the Op DSL with master
    // seeded to 9 beforehand (simulating a prior task's durable state).
    val offsets = List(50L, 120L, 80L, 200L, 150L, 30L, 300L)
    val ops = offsets.flatMap { off =>
      List(
        Write(tp0, Some("pk-a"), off),
        Commit(tp0),
        PreCommit(tp0, 10000),
      )
    }
    val h = runWith(Map(tp0 -> 9L))(ops)
    h.ks(tp0).toList shouldBe List(51L, 121L, 121L, 201L, 201L, 201L, 301L)
  }
}
