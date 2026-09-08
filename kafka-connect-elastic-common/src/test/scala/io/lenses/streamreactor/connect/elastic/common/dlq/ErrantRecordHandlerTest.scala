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
package io.lenses.streamreactor.connect.elastic.common.dlq

import ch.qos.logback.classic.{ Level => LogbackLevel }
import ch.qos.logback.classic.{ Logger => LogbackLogger }
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemError
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemErrorClassifier
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.LoneElement
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.util.concurrent.CompletableFuture
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Unit tests for [[ErrantRecordHandler]] in isolation from [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter]].
 *
 * Covers the correctness guarantees the handler is responsible for:
 *  - refusing to map a [[BulkItemError]] whose position is out of range or whose index/id does
 *    not match the op at that position (all-or-nothing per call — never partially divert)
 *  - deduplicating diverts within the tracker passed in
 *  - catching a throw from `report()` itself (the `errors.tolerance=none` case, [[ItemErrorMode.Dlq]] only)
 *  - waiting on the returned future and treating both a timeout and an execution failure as
 *    "not diverted" rather than silently skipping ([[ItemErrorMode.Dlq]] only)
 *  - [[ItemErrorMode.Drop]] diverting without ever touching the reporter
 *
 * Most tests here exercise [[ItemErrorMode.Dlq]] (the DLQ-reporting path, which shares the
 * position/id resolution and dedup logic with [[ItemErrorMode.Drop]]); the "drop mode" section
 * near the bottom covers what is unique to [[ItemErrorMode.Drop]].
 *
 * [[ErrantRecordHandler.divertWriterStage]] gets its own set of tests mirroring [[divertPermanent]]'s
 * (shared deadline across N failures, dedup, one summary WARN per call under `drop`) since it is
 * the batching entry point `JsonBulkWriter.write` calls ONCE per `put()` for every writer-stage
 * failure collected across the whole call — see its own scaladoc for why per-record diversion
 * would be unsafe.
 */
class ErrantRecordHandlerTest extends AnyFunSuite with Matchers with LoneElement {

  private val json = JsonNodeFactory.instance.objectNode().put("field", "value")

  private def record(topic: String = "t", partition: Int = 0, offset: Long = 0L): SinkRecord =
    new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "k", Schema.STRING_SCHEMA, "v", offset)

  private def op(index: String = "idx", id: String = "1"): InsertOp =
    InsertOp(index = index, id = id, json = json, pipeline = None)

  sealed trait ReporterBehaviour
  object ReporterBehaviour {
    case object Succeed          extends ReporterBehaviour
    case object ThrowImmediately extends ReporterBehaviour
    case object FutureFails      extends ReporterBehaviour
    case object NeverCompletes   extends ReporterBehaviour
    case object Cancelled        extends ReporterBehaviour
  }

  /** Records every (record, cause) it was asked to report, then behaves per [[ReporterBehaviour]]. */
  class FakeReporter(behaviour: ReporterBehaviour = ReporterBehaviour.Succeed) extends ErrantRecordReporter {
    val received: mutable.Buffer[(SinkRecord, Throwable)] = mutable.Buffer.empty

    override def report(record: SinkRecord, error: Throwable): java.util.concurrent.Future[Void] = {
      received += (record -> error)
      behaviour match {
        case ReporterBehaviour.Succeed =>
          CompletableFuture.completedFuture[Void](null)
        case ReporterBehaviour.ThrowImmediately =>
          throw new RuntimeException("Tolerance exceeded in error handler")
        case ReporterBehaviour.FutureFails =>
          val f = new CompletableFuture[Void]()
          f.completeExceptionally(new RuntimeException("producer send failed"))
          f
        case ReporterBehaviour.NeverCompletes =>
          new CompletableFuture[Void]() // deliberately never completed — forces the caller's get(timeout) to time out
        case ReporterBehaviour.Cancelled =>
          val f = new CompletableFuture[Void]()
          f.cancel(true) // Future.get() on a cancelled future throws CancellationException directly,
          // NOT wrapped in ExecutionException — a distinct case from FutureFails above.
          f
      }
    }
  }

  private def gateWith(reporter: ErrantRecordReporter): DlqGate =
    DlqGate(Some(reporter), dlqTopicConfigured = true)

  test("divertPermanent(Dlq) maps a permanent error to its record by position and reports it") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record(offset = 42L)
    val ops      = IndexedSeq(op(index = "idx", id = "1") -> r)
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)
    val tracked  = mutable.Map.empty[DlqKey, SinkRecord]

    val outcome = handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, tracked)

    outcome shouldBe DivertOutcome.Diverted
    reporter.received should have size 1
    reporter.received.head._1 shouldBe r
    tracked.values.toSet shouldBe Set(r)
  }

  test("divertPermanent(Dlq) with an empty error list is trivially Diverted without calling the reporter") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val outcome  = handler.divertPermanent(Seq.empty, IndexedSeq.empty, ItemErrorMode.Dlq, mutable.Map.empty)

    outcome shouldBe DivertOutcome.Diverted
    reporter.received shouldBe empty
  }

  test("divertPermanent(Dlq) returns NotDiverted when no reporter is present (gate disabled)") {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    val r       = record()
    val ops     = IndexedSeq(op() -> r)
    val error   = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
  }

  test("divertPermanent(Fail) always returns NotDiverted defensively, even with a working reporter") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Fail, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
    reporter.received shouldBe empty
  }

  test("divertPermanent(Dlq) refuses the whole call when a position is out of range") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 5) // only position 0 exists

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
    reporter.received shouldBe empty
  }

  test("divertPermanent(Dlq) refuses the whole call when position = -1 (unknown / disabled upstream)") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = -1)

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
    reporter.received shouldBe empty
  }

  test(
    "divertPermanent(Drop) also refuses the whole call when position = -1 — untrustworthy responses are never dropped either",
  ) {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    val ops     = IndexedSeq(op() -> record())
    val error   = BulkItemError(index = "idx", id = "1", reason = "boom", position = -1)
    val tracked = mutable.Map.empty[DlqKey, SinkRecord]

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Drop, tracked) shouldBe DivertOutcome.NotDiverted
    tracked shouldBe empty
  }

  test(
    "divertPermanent(Dlq) refuses the whole call when the response item's id does not match the op at that position",
  ) {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op(index = "idx-a", id = "1") -> record())
    // Same position, but a DIFFERENT id — simulates the Bulk API not returning items in request
    // order (or a stale mapping); must never be trusted. Index is left matching here to isolate
    // the id check from the alias-tolerance case covered below.
    val error = BulkItemError(index = "idx-a", id = "2", reason = "boom", position = 0)

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
    reporter.received shouldBe empty
  }

  test(
    "divertPermanent(Dlq) trusts the mapping when only the index differs but the id matches (write-alias resolved to its concrete backing index)",
  ) {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record(offset = 5L)
    // The request op targets the alias "logs"; the response reports the CONCRETE backing index
    // "logs-000001" for the same id — exactly what a real Elasticsearch/OpenSearch cluster does
    // when the KCQL target is a write alias (see the ElasticBulkErrorsIT / OpenSearchBulkErrorsIT
    // "ALIAS" tests, confirmed against real clusters). The id match alone must be enough to trust
    // the position-based mapping; the index must NOT be compared.
    val ops   = IndexedSeq(op(index = "logs", id = "42") -> r)
    val error = BulkItemError(index = "logs-000001", id = "42", reason = "boom", position = 0)

    val outcome = handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty)

    outcome shouldBe DivertOutcome.Diverted
    reporter.received should have size 1
    reporter.received.head._1 shouldBe r
  }

  test(
    "divertPermanent(Dlq) is all-or-nothing: one unresolvable error blocks the whole batch, even if others resolve fine",
  ) {
    val reporter  = new FakeReporter()
    val handler   = new ErrantRecordHandler(gateWith(reporter))
    val r0        = record(offset = 0L)
    val r1        = record(offset = 1L)
    val ops       = IndexedSeq(op(id = "0") -> r0, op(id = "1") -> r1)
    val goodError = BulkItemError(index = "idx", id = "0", reason = "boom", position = 0)
    val badError  = BulkItemError(index = "idx", id = "1", reason = "boom", position = 9) // out of range

    handler.divertPermanent(
      Seq(goodError, badError),
      ops,
      ItemErrorMode.Dlq,
      mutable.Map.empty,
    ) shouldBe DivertOutcome.NotDiverted
    // Neither record should have been sent to the DLQ — a partial report would advance offsets
    // for r0 while the exception for badError still fails the batch, double-counting r0.
    reporter.received shouldBe empty
  }

  test("divertPermanent(Dlq) does not re-report a key already present in the tracker") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record(offset = 7L)
    val ops      = IndexedSeq(op(index = "idx", id = "1") -> r)
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)
    val tracked  = mutable.Map(DlqKey.forOp(op(index = "idx", id = "1"), r) -> r)

    val outcome = handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, tracked)

    outcome shouldBe DivertOutcome.Diverted
    reporter.received shouldBe empty // already reported earlier this put() — not sent again
  }

  test("divertPermanent(Dlq) catches a throw from report() and returns NotDiverted") {
    val reporter = new FakeReporter(ReporterBehaviour.ThrowImmediately)
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
  }

  test("divertPermanent(Dlq) returns NotDiverted when the DLQ future resolves exceptionally") {
    val reporter = new FakeReporter(ReporterBehaviour.FutureFails)
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
  }

  test("divertPermanent(Dlq) returns NotDiverted when the DLQ future times out") {
    val reporter = new FakeReporter(ReporterBehaviour.NeverCompletes)
    // Short timeout so the test runs quickly rather than waiting the production 30s default.
    val handler = new ErrantRecordHandler(gateWith(reporter), reportTimeoutMs = 100L)
    val ops     = IndexedSeq(op() -> record())
    val error   = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
  }

  test("divertPermanent(Dlq) returns NotDiverted when the DLQ future was cancelled") {
    val reporter = new FakeReporter(ReporterBehaviour.Cancelled)
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
  }

  test(
    "divertPermanent(Dlq) returns NotDiverted and restores the interrupt flag when interrupted while awaiting the DLQ future",
  ) {
    val reporter = new FakeReporter(ReporterBehaviour.NeverCompletes)
    // A short bound, not the production 30s default: the interrupt is set BEFORE the blocking
    // call, so `Future.get` normally throws InterruptedException immediately without blocking
    // at all. This timeout only matters as a worst-case cap if awaitAck ever regresses and stops
    // honouring the interrupt — capped low so that regression fails fast instead of stalling
    // the suite for the old 10s bound.
    val handler = new ErrantRecordHandler(gateWith(reporter), reportTimeoutMs = 2000L)
    val ops     = IndexedSeq(op() -> record())
    val error   = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    Thread.currentThread().interrupt()
    try {
      handler.divertPermanent(Seq(error), ops, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.NotDiverted
    } finally {
      // Thread.interrupted() checks AND clears the flag — asserting true here proves awaitAck
      // restored it (interruption must never be silently swallowed), and clears it so it
      // doesn't leak into later tests in the suite either way.
      val _ = Thread.interrupted() shouldBe true
    }
  }

  /** Behaves per-call according to `behaviourFor(callIndex)` (0-based, in `report()` call order). */
  class SelectiveReporter(behaviourFor: Int => ReporterBehaviour) extends ErrantRecordReporter {
    val received: mutable.Buffer[(SinkRecord, Throwable)] = mutable.Buffer.empty
    private var callCount = 0

    override def report(record: SinkRecord, error: Throwable): java.util.concurrent.Future[Void] = {
      val idx = callCount
      callCount += 1
      behaviourFor(idx) match {
        case ReporterBehaviour.ThrowImmediately =>
          throw new RuntimeException("Tolerance exceeded in error handler")
        case other =>
          received += (record -> error)
          other match {
            case ReporterBehaviour.Succeed => CompletableFuture.completedFuture[Void](null)
            case ReporterBehaviour.FutureFails =>
              val f = new CompletableFuture[Void]()
              f.completeExceptionally(new RuntimeException("producer send failed"))
              f
            case ReporterBehaviour.NeverCompletes => new CompletableFuture[Void]()
            case ReporterBehaviour.Cancelled =>
              val f = new CompletableFuture[Void]()
              f.cancel(true)
              f
            case ReporterBehaviour.ThrowImmediately => throw new IllegalStateException("unreachable")
          }
      }
    }
  }

  test(
    "divertPermanent(Dlq) with N records: one future timing out makes the WHOLE call NotDiverted, even though the others acked",
  ) {
    // report() is issued for every record (index 2's future just never completes) — a PARTIAL
    // ACK, not a partial issue. futures.forall short-circuits AWAITING at the first failure, but
    // by then all three report() calls already happened.
    val reporter =
      new SelectiveReporter(idx => if (idx == 2) ReporterBehaviour.NeverCompletes else ReporterBehaviour.Succeed)
    val handler = new ErrantRecordHandler(gateWith(reporter), reportTimeoutMs = 100L)
    val r0      = record(offset = 0L)
    val r1      = record(offset = 1L)
    val r2      = record(offset = 2L)
    val ops     = IndexedSeq(op(id = "0") -> r0, op(id = "1") -> r1, op(id = "2") -> r2)
    val errors = Seq(
      BulkItemError(index = "idx", id = "0", reason = "boom", position = 0),
      BulkItemError(index = "idx", id = "1", reason = "boom", position = 1),
      BulkItemError(index = "idx", id = "2", reason = "boom", position = 2),
    )
    val tracked = mutable.Map.empty[DlqKey, SinkRecord]

    handler.divertPermanent(errors, ops, ItemErrorMode.Dlq, tracked) shouldBe DivertOutcome.NotDiverted

    reporter.received.map(_._1) should contain theSameElementsAs Seq(r0, r1, r2)
    // Nothing is tracked as diverted — a partial ack must not partially advance offsets. The
    // batch throws and JsonBulkWriter's redelivery (a fresh tracker per put()) re-attempts all
    // three; r0/r1 landing on the DLQ twice is a harmless duplicate, unlike losing r2.
    tracked shouldBe empty
  }

  /** Throws on the `throwAtIndex`-th `report()` call (0-based); every earlier call succeeds normally. */
  class ThrowsOnNthCallReporter(throwAtIndex: Int) extends ErrantRecordReporter {
    val received: mutable.Buffer[(SinkRecord, Throwable)] = mutable.Buffer.empty
    private var callCount = 0

    override def report(record: SinkRecord, error: Throwable): java.util.concurrent.Future[Void] = {
      val idx = callCount
      callCount += 1
      if (idx == throwAtIndex) throw new RuntimeException("Tolerance exceeded in error handler")
      received += (record -> error)
      CompletableFuture.completedFuture[Void](null)
    }
  }

  test(
    "divertPermanent(Dlq) with N records: report() throwing on the SECOND call aborts issuing the rest and returns NotDiverted",
  ) {
    val reporter = new ThrowsOnNthCallReporter(throwAtIndex = 1) // throws on the 2nd call (0-based)
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r0       = record(offset = 0L)
    val r1       = record(offset = 1L)
    val r2       = record(offset = 2L)
    val ops      = IndexedSeq(op(id = "0") -> r0, op(id = "1") -> r1, op(id = "2") -> r2)
    val errors = Seq(
      BulkItemError(index = "idx", id = "0", reason = "boom", position = 0),
      BulkItemError(index = "idx", id = "1", reason = "boom", position = 1),
      BulkItemError(index = "idx", id = "2", reason = "boom", position = 2),
    )
    val tracked = mutable.Map.empty[DlqKey, SinkRecord]

    handler.divertPermanent(errors, ops, ItemErrorMode.Dlq, tracked) shouldBe DivertOutcome.NotDiverted

    // r0's report() call succeeded — the record may already be durably on the DLQ before the
    // throw on r1 (see ErrantRecordHandler.reportAndAwait's own scaladoc on this). r2's report()
    // is never even attempted: the underlying `toSend.map` aborts at the exception on r1.
    reporter.received.map(_._1) shouldBe Seq(r0)
    // Not tracked despite r0 (probably) having reached the DLQ — the batch throws and is
    // redelivered, so r0 is diverted again on retry: a harmless duplicate, never a loss.
    tracked shouldBe empty
  }

  test("divertWriterStage(Dlq) reports a single writer-stage failure and tracks it") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record(offset = 3L)
    val cause    = new IllegalStateException("KCQL mapping is configured to fail on tombstone records")
    val tracked  = mutable.Map.empty[DlqKey, SinkRecord]

    val outcome = handler.divertWriterStage(Seq((r, "my-kcql-target", cause)), ItemErrorMode.Dlq, tracked)

    outcome shouldBe DivertOutcome.Diverted
    reporter.received should have size 1
    // The reported cause is wrapped (record coordinates + target prepended) rather than the raw
    // cause itself — see ContextualWriterStageFailure — but the original is preserved as its
    // cause and its message is still present verbatim.
    reporter.received.head._1 shouldBe r
    reporter.received.head._2.getCause shouldBe cause
    reporter.received.head._2.getMessage should include(cause.getMessage)
    reporter.received.head._2.getMessage should include("my-kcql-target")
    tracked.values.toSet shouldBe Set(r)
  }

  // F1: unlike divertPermanent's silent-on-success dlq path, divertWriterStage(Dlq) DOES log a
  // summary WARN — a writer-stage DLQ report is much more likely to mean the whole KCQL is wrong
  // for the topic than a single cluster-rejected document is, so it must stay visible even
  // though dlq mode keeps the task running.
  test("divertWriterStage(Dlq) emits ONE summary WARN per call on success, not one per record") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val failures = writerStageDropScenario(12)
    val tracked  = mutable.Map.empty[DlqKey, SinkRecord]

    val warnings = captureLogs(LogbackLevel.WARN) {
      handler.divertWriterStage(failures, ItemErrorMode.Dlq, tracked) shouldBe DivertOutcome.Diverted
    }

    warnings should have size 1
    warnings.head should include("reported 12 writer-stage record(s)")
    warnings.head should include("dead-letter queue")
    tracked should have size 12
  }

  test("divertWriterStage(Dlq) caps the WARN detail and reports the remaining count") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val failures = writerStageDropScenario(25)

    val warnings = captureLogs(LogbackLevel.WARN) {
      handler.divertWriterStage(failures, ItemErrorMode.Dlq, mutable.Map.empty)
    }

    val warning = warnings.loneElement
    warning should include("reason=boom-0")
    warning should include(s"reason=boom-${BulkItemErrorClassifier.MaxErrorsInMessage - 1}")
    warning should not include s"reason=boom-${BulkItemErrorClassifier.MaxErrorsInMessage}"
    warning should include(s"and ${25 - BulkItemErrorClassifier.MaxErrorsInMessage} more")
  }

  test("divertWriterStage(Dlq) logs nothing when the call is refused (NotDiverted) — no false success signal") {
    val reporter = new FakeReporter(ReporterBehaviour.ThrowImmediately)
    val handler  = new ErrantRecordHandler(gateWith(reporter))

    val warnings = captureLogs(LogbackLevel.WARN) {
      handler.divertWriterStage(
        Seq((record(), "target", new RuntimeException("x"))),
        ItemErrorMode.Dlq,
        mutable.Map.empty,
      ) shouldBe DivertOutcome.NotDiverted
    }

    warnings shouldBe empty
  }

  test("divertWriterStage(Dlq) with an empty failure list is trivially Diverted without calling the reporter") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))

    handler.divertWriterStage(Seq.empty, ItemErrorMode.Dlq, mutable.Map.empty) shouldBe DivertOutcome.Diverted
    reporter.received shouldBe empty
  }

  test("divertWriterStage(Dlq) returns NotDiverted when no reporter is present") {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    val r       = record()
    handler.divertWriterStage(
      Seq((r, "target", new RuntimeException("x"))),
      ItemErrorMode.Dlq,
      mutable.Map.empty,
    ) shouldBe DivertOutcome.NotDiverted
  }

  test("divertWriterStage(Fail) always returns NotDiverted defensively, even with a working reporter") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record()
    handler.divertWriterStage(
      Seq((r, "target", new RuntimeException("x"))),
      ItemErrorMode.Fail,
      mutable.Map.empty,
    ) shouldBe DivertOutcome.NotDiverted
    reporter.received shouldBe empty
  }

  test("divertWriterStage(Dlq) does not re-report a key already present in the tracker") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record(offset = 9L)
    val key      = DlqKey.forTarget(r, "target")
    val tracked  = mutable.Map(key -> r)

    val outcome = handler.divertWriterStage(Seq((r, "target", new RuntimeException("x"))), ItemErrorMode.Dlq, tracked)

    outcome shouldBe DivertOutcome.Diverted
    reporter.received shouldBe empty // already reported earlier this put() — not sent again
  }

  test("divertWriterStage(Dlq) returns NotDiverted when report() throws") {
    val reporter = new FakeReporter(ReporterBehaviour.ThrowImmediately)
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    handler.divertWriterStage(
      Seq((record(), "target", new RuntimeException("x"))),
      ItemErrorMode.Dlq,
      mutable.Map.empty,
    ) shouldBe DivertOutcome.NotDiverted
  }

  test(
    "divertWriterStage(Dlq) with N failures: one future timing out makes the WHOLE call NotDiverted, even though the others acked",
  ) {
    // Mirrors the equivalent divertPermanent test: report() is issued for every record (index 2's
    // future just never completes) — a PARTIAL ACK, not a partial issue. This is exactly the
    // shared-deadline guarantee divertWriterStage's scaladoc relies on: with writer-stage
    // diversion now happening for a whole write() at once, N could be every record in a put().
    val reporter =
      new SelectiveReporter(idx => if (idx == 2) ReporterBehaviour.NeverCompletes else ReporterBehaviour.Succeed)
    val handler = new ErrantRecordHandler(gateWith(reporter), reportTimeoutMs = 100L)
    val r0      = record(offset = 0L)
    val r1      = record(offset = 1L)
    val r2      = record(offset = 2L)
    val failures = Seq(
      (r0, "target", new RuntimeException("boom-0")),
      (r1, "target", new RuntimeException("boom-1")),
      (r2, "target", new RuntimeException("boom-2")),
    )
    val tracked = mutable.Map.empty[DlqKey, SinkRecord]

    handler.divertWriterStage(failures, ItemErrorMode.Dlq, tracked) shouldBe DivertOutcome.NotDiverted

    reporter.received.map(_._1) should contain theSameElementsAs Seq(r0, r1, r2)
    // Nothing is tracked as diverted — a partial ack must not partially advance offsets; the
    // batch throws (WriterStageDiversionFailedException) and the redelivered put() re-attempts
    // all three, a harmless duplicate for r0/r1, unlike losing r2.
    tracked shouldBe empty
  }

  // Drop never touches dlqGate/the reporter at all — these tests deliberately use a disabled
  // gate (no reporter, no DLQ topic) to prove diversion works without any DLQ machinery.

  test("divertPermanent(Drop) diverts a permanent error without ever calling a reporter, using a disabled gate") {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    val r       = record(offset = 11L)
    val ops     = IndexedSeq(op(index = "idx", id = "1") -> r)
    val error   = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)
    val tracked = mutable.Map.empty[DlqKey, SinkRecord]

    val outcome = handler.divertPermanent(Seq(error), ops, ItemErrorMode.Drop, tracked)

    outcome shouldBe DivertOutcome.Diverted
    tracked.values.toSet shouldBe Set(r)
  }

  test("divertPermanent(Drop) does not re-drop a key already present in the tracker") {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    val r       = record(offset = 12L)
    val ops     = IndexedSeq(op(index = "idx", id = "1") -> r)
    val error   = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)
    val key     = DlqKey.forOp(op(index = "idx", id = "1"), r)
    val tracked = mutable.Map(key -> r)

    handler.divertPermanent(Seq(error), ops, ItemErrorMode.Drop, tracked) shouldBe DivertOutcome.Diverted
    tracked.size shouldBe 1
  }

  test(
    "divertWriterStage(Drop) diverts a single writer-stage failure without ever calling a reporter, using a disabled gate",
  ) {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    val r       = record(offset = 13L)
    val cause   = new IllegalStateException("KCQL mapping is configured to fail on tombstone records")
    val tracked = mutable.Map.empty[DlqKey, SinkRecord]

    val outcome = handler.divertWriterStage(Seq((r, "my-kcql-target", cause)), ItemErrorMode.Drop, tracked)

    outcome shouldBe DivertOutcome.Diverted
    tracked.values.toSet shouldBe Set(r)
  }

  test("divertWriterStage(Drop) does not re-track a key already present in the tracker") {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    val r       = record(offset = 14L)
    val key     = DlqKey.forTarget(r, "target")
    val tracked = mutable.Map(key -> r)

    handler.divertWriterStage(
      Seq((r, "target", new RuntimeException("x"))),
      ItemErrorMode.Drop,
      tracked,
    ) shouldBe DivertOutcome.Diverted
    tracked.size shouldBe 1
  }

  /** `count` distinct writer-stage failures, one per record. */
  private def writerStageDropScenario(count: Int): Seq[(SinkRecord, String, Throwable)] =
    (0 until count).map(i => (record(offset = i.toLong), "target", new RuntimeException(s"boom-$i")))

  test("divertWriterStage(Drop) emits ONE summary WARN per call, not one per record") {
    val handler  = new ErrantRecordHandler(DlqGate.disabled)
    val failures = writerStageDropScenario(12)
    val tracked  = mutable.Map.empty[DlqKey, SinkRecord]

    val warnings = captureLogs(LogbackLevel.WARN) {
      handler.divertWriterStage(failures, ItemErrorMode.Drop, tracked) shouldBe DivertOutcome.Diverted
    }

    warnings should have size 1
    warnings.head should include("discarding 12 writer-stage record(s)")
    tracked should have size 12
  }

  test("divertWriterStage(Drop) caps the WARN detail and reports the remaining count") {
    val handler  = new ErrantRecordHandler(DlqGate.disabled)
    val failures = writerStageDropScenario(25)

    val warnings = captureLogs(LogbackLevel.WARN) {
      handler.divertWriterStage(failures, ItemErrorMode.Drop, mutable.Map.empty)
    }

    val warning = warnings.loneElement
    warning should include("reason=boom-0")
    warning should include(s"reason=boom-${BulkItemErrorClassifier.MaxErrorsInMessage - 1}")
    warning should not include s"reason=boom-${BulkItemErrorClassifier.MaxErrorsInMessage}"
    warning should include(s"and ${25 - BulkItemErrorClassifier.MaxErrorsInMessage} more")
  }

  test("divertWriterStage(Drop) logs the failures omitted from the WARN individually at DEBUG") {
    val handler  = new ErrantRecordHandler(DlqGate.disabled)
    val failures = writerStageDropScenario(25)

    val debugs = captureLogs(LogbackLevel.DEBUG) {
      handler.divertWriterStage(failures, ItemErrorMode.Drop, mutable.Map.empty)
    }

    debugs.size shouldBe (25 - BulkItemErrorClassifier.MaxErrorsInMessage)
    debugs.head should include(s"reason=boom-${BulkItemErrorClassifier.MaxErrorsInMessage}")
    debugs.last should include("reason=boom-24")
  }

  // A batch-wide cause (e.g. a field type change) makes every document in the batch a 400, so
  // per-record WARN logging would emit up to batch.size lines per bulk call, each carrying a
  // document preview in `reason`.

  private def captureLogs(level: LogbackLevel)(body: => Any): Seq[String] = {
    val handlerLogger = LoggerFactory.getLogger(classOf[ErrantRecordHandler]).asInstanceOf[LogbackLogger]
    val previousLevel = handlerLogger.getLevel
    val listAppender  = new ListAppender[ILoggingEvent]()
    listAppender.start()
    handlerLogger.addAppender(listAppender)
    handlerLogger.setLevel(LogbackLevel.DEBUG)
    try {
      body
      listAppender.list.asScala.filter(_.getLevel == level).map(_.getFormattedMessage).toSeq
    } finally {
      handlerLogger.setLevel(previousLevel)
      val _ = handlerLogger.detachAppender(listAppender)
    }
  }

  /** `count` records, each rejected with its own 400, correlated by position. */
  private def dropScenario(count: Int): (Seq[BulkItemError], IndexedSeq[(BulkOp, SinkRecord)]) = {
    val ops = (0 until count).map { i =>
      op(index = "idx", id = i.toString) -> record(offset = i.toLong)
    }.toIndexedSeq
    val errors = (0 until count).map { i =>
      BulkItemError(index = "idx", id = i.toString, reason = s"boom-$i", position = i)
    }
    (errors, ops)
  }

  test("divertPermanent(Drop) emits ONE summary WARN per batch, not one per record") {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    // 12 is enough to prove "one summary line, not N" — it only needs to exceed 1; it doesn't
    // need to be anywhere near a realistic batch size to make that point.
    val (errors, ops) = dropScenario(12)
    val tracked       = mutable.Map.empty[DlqKey, SinkRecord]

    val warnings = captureLogs(LogbackLevel.WARN) {
      handler.divertPermanent(errors, ops, ItemErrorMode.Drop, tracked) shouldBe DivertOutcome.Diverted
    }

    warnings should have size 1
    warnings.head should include("discarding 12 record(s)")
    tracked should have size 12
  }

  test("divertPermanent(Drop) caps the WARN detail and reports the remaining count") {
    val handler       = new ErrantRecordHandler(DlqGate.disabled)
    val (errors, ops) = dropScenario(25)

    val warnings = captureLogs(LogbackLevel.WARN) {
      handler.divertPermanent(errors, ops, ItemErrorMode.Drop, mutable.Map.empty)
    }

    val warning = warnings.loneElement
    // MaxErrorsInMessage (10) records of detail, then a count of the rest.
    warning should include("reason=boom-0")
    warning should include(s"reason=boom-${BulkItemErrorClassifier.MaxErrorsInMessage - 1}")
    warning should not include s"reason=boom-${BulkItemErrorClassifier.MaxErrorsInMessage}"
    warning should include(s"and ${25 - BulkItemErrorClassifier.MaxErrorsInMessage} more")
  }

  test("divertPermanent(Drop) logs the records omitted from the WARN individually at DEBUG") {
    val handler       = new ErrantRecordHandler(DlqGate.disabled)
    val (errors, ops) = dropScenario(25)

    val debugs = captureLogs(LogbackLevel.DEBUG) {
      handler.divertPermanent(errors, ops, ItemErrorMode.Drop, mutable.Map.empty)
    }

    debugs.size shouldBe (25 - BulkItemErrorClassifier.MaxErrorsInMessage)
    debugs.head should include(s"reason=boom-${BulkItemErrorClassifier.MaxErrorsInMessage}")
    debugs.last should include("reason=boom-24")
  }

  test("divertPermanent(Drop) emits no DEBUG remainder when the batch fits within the WARN cap") {
    val handler       = new ErrantRecordHandler(DlqGate.disabled)
    val (errors, ops) = dropScenario(BulkItemErrorClassifier.MaxErrorsInMessage)

    val debugs = captureLogs(LogbackLevel.DEBUG) {
      handler.divertPermanent(errors, ops, ItemErrorMode.Drop, mutable.Map.empty)
    }

    debugs shouldBe empty
  }

  test("divertWriterStage(Drop) emits no DEBUG remainder when the batch fits within the WARN cap") {
    val handler  = new ErrantRecordHandler(DlqGate.disabled)
    val failures = writerStageDropScenario(BulkItemErrorClassifier.MaxErrorsInMessage)

    val debugs = captureLogs(LogbackLevel.DEBUG) {
      handler.divertWriterStage(failures, ItemErrorMode.Drop, mutable.Map.empty)
    }

    debugs shouldBe empty
  }
}
