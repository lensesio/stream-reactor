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

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemError
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemErrorClassifier
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import org.apache.kafka.connect.sink.ErrantRecordReporter
import org.apache.kafka.connect.sink.SinkRecord

import java.util.concurrent.CancellationException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.{ Future => JFuture }
import scala.collection.mutable
import scala.util.control.NonFatal

/**
 * Reports permanently-failed bulk items (and writer-stage failures) to Kafka Connect's
 * dead-letter queue via [[ErrantRecordReporter]], modelled on the in-repo
 * `com.wepay.kafka.connect.bigquery.ErrantRecordHandler` (kcbq-connector) but hardened:
 *
 *  - Maps a [[BulkItemError]] to a record by `position`, refusing the mapping unless `position`
 *    is in range AND the response item's `id` matches the op at that position. A length
 *    mismatch already disables positions upstream (`position = -1`); an id mismatch at an
 *    in-range position means the Bulk API did not return items in request order, and guessing
 *    would silently DLQ the wrong Kafka record while committing the actually-bad one as
 *    successful — the only way this feature could lose data. On any doubt the whole call is
 *    refused ([[ReportOutcome.NotReported]]) rather than partially reported.
 *  - Deliberately does NOT also require the response item's `index` to match the request op's
 *    `index`. When the KCQL target is a write alias, Elasticsearch/OpenSearch resolve it to the
 *    concrete backing index before writing and echo that concrete index in the response — never
 *    the alias that was actually sent — so an index equality check would refuse every batch
 *    routed through an alias. `id` alone is sufficient corroboration that the response is in
 *    request order; `DlqKey.forOp` still keys the DLQ report on the op's (KCQL-resolved) index,
 *    so this does not change dedup semantics or misattribute a Kafka record.
 *  - Waits on every `Future[Void]` returned by `report()` under a short bound before treating a
 *    batch as reported. This is a correctness requirement, not an optimisation: Kafka Connect
 *    provides no other signal that a DLQ write failed while `errors.tolerance=all` —
 *    `WorkerErrantRecordReporter.maybeThrowAsyncError()` only throws outside tolerance limits,
 *    so it is inert in exactly the mode `skip` requires.
 *  - Catches throws from `report()` itself. Under `errors.tolerance=none`,
 *    `RetryWithToleranceOperator.executeFailed` issues the report and THEN throws
 *    (`ConnectException("Tolerance exceeded in error handler", ...)`), so the record still
 *    reaches the DLQ even though the call throws. The throw is caught here and surfaced as
 *    [[ReportOutcome.NotReported]] so the writer falls back to its own classified exception
 *    (carrying index/id/type/status/reason) instead of a generic tolerance message — net effect
 *    under `none`: the record is in the DLQ AND the task fails fast, which is correct, since
 *    `skip` requires `errors.tolerance=all` to have any effect.
 */
class ErrantRecordHandler(
  dlqGate: DlqGate,
  // Bounded wait for DLQ producer acknowledgement. Deliberately far shorter than
  // connect.elastic.write.timeout (default 300000ms/5min) — blocking that long inside put()
  // risks exceeding max.poll.interval.ms and having the task revoked mid-flight, after the
  // Elasticsearch/OpenSearch write has already happened. Exposed as a constructor param (rather
  // than a hardcoded constant) purely so tests can exercise the timeout path quickly.
  reportTimeoutMs: Long = 30000L,
) extends StrictLogging {

  /**
   * Reports permanent (non-retriable) item errors, mapping each back to its originating
   * `SinkRecord` by position. `errors` must already be filtered to permanent errors only —
   * retriable errors (429/409) are never reported here; they are left on the retry path.
   */
  def reportPermanent(
    errors:          Seq[BulkItemError],
    opsWithRecords:  IndexedSeq[(BulkOp, SinkRecord)],
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): ReportOutcome =
    // Gated on dlqGate.enabled, NOT dlqGate.reporter alone — a non-null reporter with no DLQ
    // topic configured (errors.log.enable=true on its own, see DlqGate's scaladoc) must behave
    // identically to no reporter at all. JsonBulkWriter already enforces this via
    // skipModeActive before ever calling this method, but the handler must not rely on that:
    // it is public API and must be safe on its own for any future caller.
    (dlqGate.enabled, dlqGate.reporter) match {
      case _ if errors.isEmpty => ReportOutcome.Reported
      case (true, Some(reporter)) =>
        resolvePositions(errors, opsWithRecords) match {
          case None => ReportOutcome.NotReported
          case Some(mapped) =>
            val toSend = mapped
              .filterNot { case (key, _, _) => reportedThisPut.contains(key) }
              .map { case (key, record, error) => (key, record, BulkItemErrorClassifier.exceptionFor(Seq(error))) }
            reportAndAwait(reporter, toSend, reportedThisPut)
        }
      case _ => ReportOutcome.NotReported
    }

  /**
   * Reports a single writer-stage failure — a record that never made it into a `BulkOp` (e.g. a
   * `NullValueBehavior.FAIL` tombstone, a primary-key extraction failure, or an unsupported write
   * mode). There is no resolved index to correlate against here, so `target` (typically the KCQL
   * target pattern) stands in for dedup purposes only.
   */
  def reportOne(
    record:          SinkRecord,
    target:          String,
    cause:           Throwable,
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): Boolean =
    // See reportPermanent — gated on dlqGate.enabled, not dlqGate.reporter alone.
    (dlqGate.enabled, dlqGate.reporter) match {
      case (true, Some(reporter)) =>
        val key = DlqKey.forTarget(record, target)
        if (reportedThisPut.contains(key)) {
          true
        } else {
          reportAndAwait(reporter, Vector((key, record, cause)), reportedThisPut) == ReportOutcome.Reported
        }
      case _ => false
    }

  private def resolvePositions(
    errors:         Seq[BulkItemError],
    opsWithRecords: IndexedSeq[(BulkOp, SinkRecord)],
  ): Option[Seq[(DlqKey, SinkRecord, BulkItemError)]] = {
    val mapped: Seq[Option[(DlqKey, SinkRecord, BulkItemError)]] = errors.map { error =>
      if (error.position < 0 || error.position >= opsWithRecords.size) {
        logger.error(
          s"Bulk item error at position ${error.position} is out of range for a request of " +
            s"${opsWithRecords.size} op(s) (index=${error.index} id=${error.id}); " +
            "refusing to report to the DLQ as the position cannot be trusted.",
        )
        None
      } else {
        val (op, record) = opsWithRecords(error.position)
        // Corroborate the position-based mapping with `id` only — NOT `index`. A write alias
        // resolves to its concrete backing index by the time Elasticsearch/OpenSearch echo
        // `_index` in the bulk response, so op.index (the alias we sent) never equals
        // error.index (the concrete index) even though the mapping is entirely correct. `id` is
        // sufficient corroboration that the Bulk API returned items in request order: a reorder
        // would flip id too, and DlqKey.forOp still keys on op.index (the KCQL-resolved target),
        // so dedup semantics are unaffected by an alias/concrete-index difference.
        if (op.id == error.id) {
          if (op.index != error.index) {
            logger.debug(
              s"Bulk response item at position ${error.position} reported index '${error.index}' " +
                s"but the request op targeted '${op.index}' (id=${op.id}); treating this as alias " +
                "resolution (the target is a write alias resolved to its concrete backing index) " +
                "and mapping by id. The DLQ report will use the KCQL-resolved target, not the " +
                "concrete index reported by the cluster.",
            )
          }
          Some((DlqKey.forOp(op, record), record, error))
        } else {
          logger.error(
            s"Bulk response item at position ${error.position} (id=${error.id}) does not match the " +
              s"request op at that position (id=${op.id}); the Bulk API may not have returned items " +
              "in request order. Refusing to report to the DLQ for this batch as the mapping cannot " +
              "be trusted.",
          )
          None
        }
      }
    }
    if (mapped.exists(_.isEmpty)) None else Some(mapped.flatten)
  }

  private def reportAndAwait(
    reporter:        ErrantRecordReporter,
    toSend:          Seq[(DlqKey, SinkRecord, Throwable)],
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): ReportOutcome =
    if (toSend.isEmpty) {
      ReportOutcome.Reported
    } else {
      val issued: Option[Seq[(DlqKey, SinkRecord, JFuture[Void])]] =
        try {
          Some(toSend.map { case (key, record, cause) => (key, record, reporter.report(record, cause)) })
        } catch {
          case NonFatal(e) =>
            logger.error(
              s"ErrantRecordReporter.report threw while issuing ${toSend.size} record(s) to the DLQ " +
                s"(likely errors.tolerance=none — the record(s) may still have reached the DLQ before " +
                s"the throw); falling back to failing the batch: ${e.getMessage}",
              e,
            )
            None
        }

      issued match {
        case None => ReportOutcome.NotReported
        case Some(futures) =>
          val allAcknowledged = futures.forall { case (_, _, future) => awaitAck(future) }
          if (allAcknowledged) {
            futures.foreach { case (key, record, _) => reportedThisPut.put(key, record) }
            ReportOutcome.Reported
          } else {
            ReportOutcome.NotReported
          }
      }
    }

  private def awaitAck(future: JFuture[Void]): Boolean =
    try {
      future.get(reportTimeoutMs, TimeUnit.MILLISECONDS)
      true
    } catch {
      case _: TimeoutException =>
        logger.error(s"Timed out after ${reportTimeoutMs}ms waiting for a DLQ write to be acknowledged.")
        false
      case e: ExecutionException =>
        logger.error(s"DLQ write failed: ${Option(e.getCause).map(_.getMessage).getOrElse(e.getMessage)}", e.getCause)
        false
      case e: CancellationException =>
        // Future.get throws this (a RuntimeException, NOT a subtype of ExecutionException) if
        // the future was cancelled — treat it the same as any other unacknowledged write.
        logger.error("DLQ write future was cancelled before it could be acknowledged.", e)
        false
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        logger.error("Interrupted while waiting for a DLQ write to be acknowledged.", e)
        false
    }
}
