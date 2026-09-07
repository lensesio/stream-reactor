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

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemError
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.util.concurrent.CompletableFuture
import scala.collection.mutable

/**
 * Unit tests for [[ErrantRecordHandler]] in isolation from [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter]].
 *
 * Covers the correctness guarantees the handler is responsible for:
 *  - refusing to map a [[BulkItemError]] whose position is out of range or whose index/id does
 *    not match the op at that position (all-or-nothing per call — never partially report)
 *  - deduplicating reports within the tracker passed in
 *  - catching a throw from `report()` itself (the `errors.tolerance=none` case)
 *  - waiting on the returned future and treating both a timeout and an execution failure as
 *    "not reported" rather than silently skipping
 */
class ErrantRecordHandlerTest extends AnyFunSuite with Matchers {

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

  // ---- reportPermanent: happy path ------------------------------------------------------

  test("reportPermanent maps a permanent error to its record by position and reports it") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record(offset = 42L)
    val ops      = IndexedSeq(op(index = "idx", id = "1") -> r)
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)
    val tracked  = mutable.Map.empty[DlqKey, SinkRecord]

    val outcome = handler.reportPermanent(Seq(error), ops, tracked)

    outcome shouldBe ReportOutcome.Reported
    reporter.received should have size 1
    reporter.received.head._1 shouldBe r
    tracked.values.toSet shouldBe Set(r)
  }

  test("reportPermanent with an empty error list is trivially Reported without calling the reporter") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val outcome  = handler.reportPermanent(Seq.empty, IndexedSeq.empty, mutable.Map.empty)

    outcome shouldBe ReportOutcome.Reported
    reporter.received shouldBe empty
  }

  test("reportPermanent returns NotReported when no reporter is present (gate disabled)") {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    val r       = record()
    val ops     = IndexedSeq(op() -> r)
    val error   = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.reportPermanent(Seq(error), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
  }

  // ---- position / identity verification -------------------------------------------------

  test("reportPermanent refuses the whole call when a position is out of range") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 5) // only position 0 exists

    handler.reportPermanent(Seq(error), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
    reporter.received shouldBe empty
  }

  test("reportPermanent refuses the whole call when position = -1 (unknown / disabled upstream)") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = -1)

    handler.reportPermanent(Seq(error), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
    reporter.received shouldBe empty
  }

  test("reportPermanent refuses the whole call when the response item's id does not match the op at that position") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op(index = "idx-a", id = "1") -> record())
    // Same position, but a DIFFERENT id — simulates the Bulk API not returning items in request
    // order (or a stale mapping); must never be trusted. Index is left matching here to isolate
    // the id check from the alias-tolerance case covered below.
    val error = BulkItemError(index = "idx-a", id = "2", reason = "boom", position = 0)

    handler.reportPermanent(Seq(error), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
    reporter.received shouldBe empty
  }

  test(
    "reportPermanent trusts the mapping when only the index differs but the id matches (write-alias resolved to its concrete backing index)",
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

    val outcome = handler.reportPermanent(Seq(error), ops, mutable.Map.empty)

    outcome shouldBe ReportOutcome.Reported
    reporter.received should have size 1
    reporter.received.head._1 shouldBe r
  }

  test(
    "reportPermanent is all-or-nothing: one unresolvable error blocks the whole batch, even if others resolve fine",
  ) {
    val reporter  = new FakeReporter()
    val handler   = new ErrantRecordHandler(gateWith(reporter))
    val r0        = record(offset = 0L)
    val r1        = record(offset = 1L)
    val ops       = IndexedSeq(op(id = "0") -> r0, op(id = "1") -> r1)
    val goodError = BulkItemError(index = "idx", id = "0", reason = "boom", position = 0)
    val badError  = BulkItemError(index = "idx", id = "1", reason = "boom", position = 9) // out of range

    handler.reportPermanent(Seq(goodError, badError), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
    // Neither record should have been sent to the DLQ — a partial report would advance offsets
    // for r0 while the exception for badError still fails the batch, double-counting r0.
    reporter.received shouldBe empty
  }

  // ---- dedup ------------------------------------------------------------------------------

  test("reportPermanent does not re-report a key already present in the tracker") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record(offset = 7L)
    val ops      = IndexedSeq(op(index = "idx", id = "1") -> r)
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)
    val tracked  = mutable.Map(DlqKey.forOp(op(index = "idx", id = "1"), r) -> r)

    val outcome = handler.reportPermanent(Seq(error), ops, tracked)

    outcome shouldBe ReportOutcome.Reported
    reporter.received shouldBe empty // already reported earlier this put() — not sent again
  }

  // ---- report() throwing (errors.tolerance=none) -----------------------------------------

  test("reportPermanent catches a throw from report() and returns NotReported") {
    val reporter = new FakeReporter(ReporterBehaviour.ThrowImmediately)
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.reportPermanent(Seq(error), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
  }

  // ---- future acknowledgement -------------------------------------------------------------

  test("reportPermanent returns NotReported when the DLQ future resolves exceptionally") {
    val reporter = new FakeReporter(ReporterBehaviour.FutureFails)
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.reportPermanent(Seq(error), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
  }

  test("reportPermanent returns NotReported when the DLQ future times out") {
    val reporter = new FakeReporter(ReporterBehaviour.NeverCompletes)
    // Short timeout so the test runs quickly rather than waiting the production 30s default.
    val handler = new ErrantRecordHandler(gateWith(reporter), reportTimeoutMs = 100L)
    val ops     = IndexedSeq(op() -> record())
    val error   = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.reportPermanent(Seq(error), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
  }

  test("reportPermanent returns NotReported when the DLQ future was cancelled") {
    val reporter = new FakeReporter(ReporterBehaviour.Cancelled)
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    handler.reportPermanent(Seq(error), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
  }

  test(
    "reportPermanent returns NotReported and restores the interrupt flag when interrupted while awaiting the DLQ future",
  ) {
    val reporter = new FakeReporter(ReporterBehaviour.NeverCompletes)
    val handler  = new ErrantRecordHandler(gateWith(reporter), reportTimeoutMs = 10000L)
    val ops      = IndexedSeq(op() -> record())
    val error    = BulkItemError(index = "idx", id = "1", reason = "boom", position = 0)

    Thread.currentThread().interrupt()
    try {
      handler.reportPermanent(Seq(error), ops, mutable.Map.empty) shouldBe ReportOutcome.NotReported
    } finally {
      // Thread.interrupted() checks AND clears the flag — asserting true here proves awaitAck
      // restored it (interruption must never be silently swallowed), and clears it so it
      // doesn't leak into later tests in the suite either way.
      val _ = Thread.interrupted() shouldBe true
    }
  }

  // ---- reportOne (writer-stage failures) --------------------------------------------------

  test("reportOne reports a single writer-stage failure and tracks it") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record(offset = 3L)
    val cause    = new IllegalStateException("KCQL mapping is configured to fail on tombstone records")
    val tracked  = mutable.Map.empty[DlqKey, SinkRecord]

    handler.reportOne(r, "my-kcql-target", cause, tracked) shouldBe true
    reporter.received should have size 1
    reporter.received.head shouldBe (r -> cause)
    tracked.values.toSet shouldBe Set(r)
  }

  test("reportOne returns false when no reporter is present") {
    val handler = new ErrantRecordHandler(DlqGate.disabled)
    val r       = record()
    handler.reportOne(r, "target", new RuntimeException("x"), mutable.Map.empty) shouldBe false
  }

  test("reportOne short-circuits to true without re-invoking the reporter when already tracked") {
    val reporter = new FakeReporter()
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    val r        = record(offset = 9L)
    val key      = DlqKey.forTarget(r, "target")
    val tracked  = mutable.Map(key -> r)

    handler.reportOne(r, "target", new RuntimeException("x"), tracked) shouldBe true
    reporter.received shouldBe empty
  }

  test("reportOne returns false when report() throws") {
    val reporter = new FakeReporter(ReporterBehaviour.ThrowImmediately)
    val handler  = new ErrantRecordHandler(gateWith(reporter))
    handler.reportOne(record(), "target", new RuntimeException("x"), mutable.Map.empty) shouldBe false
  }
}
