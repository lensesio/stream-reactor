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
import io.lenses.streamreactor.connect.elastic.common.config.ItemErrorMode
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
 * A record that failed before becoming a bulk operation (e.g. a missing header, an unresolvable
 * PK path, a tombstone under `FAIL`).
 */
final case class WriterStageFailure(record: SinkRecord, target: String, cause: Throwable)

/**
 * Prepends `topic-partition-offset (target=…)` to a writer-stage failure's message string
 * before it is reported to the DLQ. The original cause is preserved as `getCause`.
 * Suppression and stack trace capture are disabled (`writableStackTrace = false`): this
 * exception is never thrown or logged, and on a fully-poison topic it is constructed once per
 * record, so `fillInStackTrace` would be wasted work.
 */
final class ContextualWriterStageFailure(message: String, cause: Throwable)
    extends RuntimeException(message, cause, false, false)

/**
 * Diverts permanently-failed bulk items (and writer-stage failures): drops them
 * (`bulk.item.errors=drop`) or reports them to Kafka Connect's DLQ via [[ErrantRecordReporter]]
 * (`bulk.item.errors=dlq`).
 *
 *  - Maps a [[BulkItemError]] to a record by `position`, requiring both that `position` is in
 *    range and that the response item's `id` matches the op at that position; any mismatch
 *    refuses the whole call ([[DivertOutcome.NotDiverted]]), for both `drop` and `dlq`.
 *  - Does not compare `index`: a write alias resolves to a concrete backing index in the
 *    response, so `op.index` (alias) never equals `error.index` (concrete) even on a correct
 *    mapping. `id` alone corroborates ordering; `DlqKey.forOp` keys on the op's index.
 *  - `dlq`: awaits every `report()` future issued by one [[divertPermanent]]/[[divertWriterStage]]
 *    call against a single shared deadline (`reportTimeoutMs` from the start of that call, not
 *    restarted per future), bounding the whole call at `reportTimeoutMs` regardless of how many
 *    records it reports. [[divertWriterStage]] in particular is called ONCE for an entire
 *    `JsonBulkWriter.write()` (every topic/KCQL/sub-batch), not once per sub-batch or per record
 *    — see its own scaladoc for why that matters.
 *  - `dlq`: catches throws from `report()` itself — under `errors.tolerance=none`,
 *    `RetryWithToleranceOperator.executeFailed` reports then throws
 *    (`ConnectException("Tolerance exceeded in error handler", ...)`) — and surfaces
 *    [[DivertOutcome.NotDiverted]] so the writer's own classified exception propagates instead
 *    of the generic tolerance message.
 *  - `drop`: no reporter, no ack wait, no tolerance interaction. Logged as one summary WARN per
 *    batch, capped at [[BulkItemErrorClassifier.MaxErrorsInMessage]] records of detail with the
 *    remainder at DEBUG — the log is the only trace of a discarded record.
 *  - `dlq`: [[divertPermanent]] logs nothing itself on success — a bulk-item DLQ report is
 *    usually just one cluster-rejected document, so per-bulk-call WARN would be noise.
 *    [[divertWriterStage]] is the exception: it DOES log a summary WARN on success, capped the
 *    same way, because a writer-stage DLQ report is much more likely to indicate a KCQL that is
 *    wrong for the whole topic — see its own scaladoc.
 */
class ErrantRecordHandler(
  dlqGate: DlqGate,
  // Bounded wait for DLQ producer ack. Shorter than connect.elastic.write.timeout (default
  // 300000ms) to avoid exceeding max.poll.interval.ms. Constructor param so tests can use a
  // short timeout.
  reportTimeoutMs: Long = 30000L,
) extends StrictLogging {

  /**
   * Diverts permanent item errors, mapping each to its `SinkRecord` by position. `errors` must
   * already be filtered to permanent, divertible errors — retriable (429/409) are never
   * diverted here.
   *
   * `mode = Fail` returns [[DivertOutcome.NotDiverted]] (defensive default for callers that
   * invoke this while diversion is inactive).
   */
  def divertPermanent(
    errors:          Seq[BulkItemError],
    opsWithRecords:  IndexedSeq[(BulkOp, SinkRecord)],
    mode:            ItemErrorMode,
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): DivertOutcome =
    if (errors.isEmpty) {
      DivertOutcome.Diverted
    } else {
      mode match {
        case ItemErrorMode.Drop =>
          resolvePositions(errors, opsWithRecords) match {
            case None => DivertOutcome.NotDiverted
            case Some(mapped) =>
              dropAll(mapped, reportedThisPut)
              DivertOutcome.Diverted
          }
        case ItemErrorMode.Dlq =>
          // Gated on dlqGate.enabled, not dlqGate.reporter alone: a reporter with no DLQ topic
          // (errors.log.enable=true alone) must behave like no reporter. Must be safe standalone
          // even though JsonBulkWriter also gates on divertActive.
          (dlqGate.enabled, dlqGate.reporter) match {
            case (true, Some(reporter)) =>
              resolvePositions(errors, opsWithRecords) match {
                case None => DivertOutcome.NotDiverted
                case Some(mapped) =>
                  val toSend = mapped
                    .filterNot { case (key, _, _) => reportedThisPut.contains(key) }
                    .map { case (key, record, error) =>
                      (key, record, BulkItemErrorClassifier.exceptionFor(Seq(error)))
                    }
                  reportAndAwait(reporter, toSend, reportedThisPut)
              }
            case _ => DivertOutcome.NotDiverted
          }
        case ItemErrorMode.Fail => DivertOutcome.NotDiverted
      }
    }

  /**
   * Diverts a batch of writer-stage failures — records that never became a `BulkOp` (e.g. a
   * `NullValueBehavior.FAIL` tombstone, or any PK/index-target extraction or conversion failure).
   * `target` (the KCQL target) stands in for dedup; there is no resolved index here.
   *
   * Called ONCE per `JsonBulkWriter.write()` (i.e. once per `put()`), covering every topic/KCQL/
   * sub-batch's writer-stage failures together — never per record and never per sub-batch. Two
   * reasons this matters:
   *
   *  - `dlq`: [[reportAndAwait]] shares ONE deadline (`reportTimeoutMs`) across every future it
   *    issues in a single call. Diverting per record would mean a KCQL no record on the topic can
   *    satisfy (e.g. a renamed PK header) pays up to one FULL `reportTimeoutMs` wait PER RECORD
   *    inside a single `put()` — for a batch of hundreds or thousands of records, easily blowing
   *    `max.poll.interval.ms` and evicting the task from its consumer group. One call for the
   *    whole `put()` bounds it at `reportTimeoutMs` total, matching [[divertPermanent]].
   *  - Diverting per sub-batch (`settings.batchSize` records at a time) is not enough either: a
   *    later sub-batch's bulk call can still fail and force the whole `put()` to be redelivered,
   *    after an earlier sub-batch's writer-stage failures were already reported — duplicating
   *    those DLQ reports (or drop log lines) on every redelivery attempt while the later failure
   *    persists, the exact unbounded-amplification failure mode [[divertPermanent]]'s deferred-
   *    diversion-on-a-retriable-remainder already avoids for bulk-item errors (see its scaladoc).
   *    `JsonBulkWriter.write` therefore defers calling this until every bulk call in the `put()`
   *    has already succeeded.
   */
  def divertWriterStage(
    failures:        Seq[WriterStageFailure],
    mode:            ItemErrorMode,
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): DivertOutcome =
    if (failures.isEmpty) {
      DivertOutcome.Diverted
    } else {
      mode match {
        case ItemErrorMode.Drop =>
          dropAllWriterStage(failures, reportedThisPut)
          DivertOutcome.Diverted
        case ItemErrorMode.Dlq =>
          // See divertPermanent — gated on dlqGate.enabled, not dlqGate.reporter alone.
          (dlqGate.enabled, dlqGate.reporter) match {
            case (true, Some(reporter)) =>
              // Filter FIRST, enrich SECOND (mirrors divertPermanent's order): a
              // ContextualWriterStageFailure is only ever allocated for a record that is
              // actually about to be reported, not for one about to be discarded as
              // already-reported this put().
              val toReport = failures
                .map(f => (DlqKey.forTarget(f.record, f.target), f))
                .filterNot { case (key, _) => reportedThisPut.contains(key) }
              val toSend = toReport.map {
                case (key, f) =>
                  val enriched = new ContextualWriterStageFailure(
                    s"${f.record.topic()}-${f.record.kafkaPartition()}-${f.record.kafkaOffset()} " +
                      s"(target=${f.target}): ${f.cause.getMessage}",
                    f.cause,
                  )
                  (key, f.record, enriched: Throwable)
              }
              val outcome = reportAndAwait(reporter, toSend, reportedThisPut)
              if (outcome == DivertOutcome.Diverted) {
                // reportAndAwait already marked these in reportedThisPut on success; this is
                // purely the log line — see logWriterStageDlqReported's scaladoc for why dlq
                // gets a summary WARN here, unlike divertPermanent's silent dlq path.
                logWriterStageDlqReported(toReport.map { case (key, f) => (key, f.record, f.cause) })
              }
              outcome
            case _ => DivertOutcome.NotDiverted
          }
        case ItemErrorMode.Fail => DivertOutcome.NotDiverted
      }
    }

  /**
   * Tracks and logs a batch of dropped records.
   *
   * One WARN per call, not per record: a batch-wide cause (e.g. a field type change that makes
   * every document in the topic a mapper conflict) drops up to `batch.size` records per bulk
   * call, and each `reason` embeds a preview of the rejected document. Detail is capped at
   * [[BulkItemErrorClassifier.MaxErrorsInMessage]]; the remainder is logged at DEBUG.
   */
  private def dropAll(
    mapped:          Seq[(DlqKey, SinkRecord, BulkItemError)],
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): Unit = {
    val toDrop = mapped.filterNot { case (key, _, _) => reportedThisPut.contains(key) }
    toDrop.foreach { case (key, record, _) => reportedThisPut.put(key, record) }

    if (toDrop.nonEmpty) {
      val shown = toDrop.take(BulkItemErrorClassifier.MaxErrorsInMessage)
      val extra = toDrop.size - shown.size
      val more  = if (extra > 0) s", ... and $extra more (logged individually at DEBUG)" else ""
      logger.warn(
        s"${toDrop.size} record(s) were permanently rejected by Elasticsearch/OpenSearch and have been " +
          s"discarded (bulk.item.errors=drop). These records will not be retried and are not recoverable. " +
          s"Check the rejection reasons below and fix the data or mapping if this is unexpected. " +
          s"Details: ${shown.map(describeDrop).mkString(", ")}$more",
      )
      toDrop.drop(shown.size).foreach(e => logger.debug(s"Discarded record: ${describeDrop(e)}"))
    }
  }

  private def describeDrop(entry: (DlqKey, SinkRecord, BulkItemError)): String = {
    val (_, record, error) = entry
    s"[${record.topic()}-${record.kafkaPartition()}-${record.kafkaOffset()} index=${error.index} " +
      s"id=${error.id} status=${error.status} reason=${error.reason}]"
  }

  /**
   * Tracks and logs a batch of dropped WRITER-STAGE failures (records that never became a
   * `BulkOp`) — the writer-stage counterpart of [[dropAll]]. One summary WARN per
   * [[divertWriterStage]] call (i.e. per `put()`), not per record, for the same reason `dropAll`
   * caps its own: a KCQL no record on the topic can satisfy would otherwise emit one WARN line
   * per record in the batch.
   */
  private def dropAllWriterStage(
    failures:        Seq[WriterStageFailure],
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): Unit = {
    val toDrop = failures
      .map(f => (DlqKey.forTarget(f.record, f.target), f.record, f.cause))
      .filterNot { case (key, _, _) => reportedThisPut.contains(key) }
    toDrop.foreach { case (key, record, _) => reportedThisPut.put(key, record) }

    if (toDrop.nonEmpty) {
      val shown = toDrop.take(BulkItemErrorClassifier.MaxErrorsInMessage)
      val extra = toDrop.size - shown.size
      val more  = if (extra > 0) s", ... and $extra more (logged individually at DEBUG)" else ""
      logger.warn(
        s"${toDrop.size} record(s) could not be sent to Elasticsearch/OpenSearch because required " +
          s"fields could not be extracted (e.g. a missing header, an unresolvable primary key, or an " +
          s"invalid index target). They have been discarded (bulk.item.errors=drop) and will not be " +
          s"retried. Check your KCQL configuration and record schema. " +
          s"Details: ${shown.map(describeWriterStageFailure).mkString(", ")}$more",
      )
      toDrop.drop(shown.size).foreach(e =>
        logger.debug(s"Discarded record (extraction failure): ${describeWriterStageFailure(e)}"),
      )
    }
  }

  /**
   * One summary WARN per `divertWriterStage(Dlq)` call, deliberately UNLIKE `divertPermanent`'s
   * DLQ path, which logs nothing on success (see the class scaladoc: a bulk-item DLQ report
   * usually just means the cluster rejected one otherwise-fine document, so a WARN per bulk call
   * would be noise). A writer-stage DLQ report is a different signal: it means a record never
   * even reached the cluster, and — now that writer-stage diversion isn't restricted to the rare
   * `FAIL`-tombstone case any more — it is frequently caused by a KCQL that is wrong for the
   * WHOLE topic (e.g. a renamed PK header), which is exactly the kind of misconfiguration an
   * operator needs to see even though `dlq` mode keeps the task running. Mirrors
   * `dropAllWriterStage`'s cap/remainder shape so `drop` and `dlq` are equally visible in logs.
   */
  private def logWriterStageDlqReported(reported: Seq[(DlqKey, SinkRecord, Throwable)]): Unit =
    if (reported.nonEmpty) {
      val shown = reported.take(BulkItemErrorClassifier.MaxErrorsInMessage)
      val extra = reported.size - shown.size
      val more  = if (extra > 0) s", ... and $extra more (logged individually at DEBUG)" else ""
      logger.warn(
        s"${reported.size} record(s) could not be sent to Elasticsearch/OpenSearch because required " +
          s"fields could not be extracted (e.g. a missing header, an unresolvable primary key, or an " +
          s"invalid index target). They have been sent to the dead-letter queue (bulk.item.errors=dlq) " +
          s"and will not be retried. Check your KCQL configuration and record schema. " +
          s"Details: ${shown.map(describeWriterStageFailure).mkString(", ")}$more",
      )
      reported.drop(shown.size).foreach(e =>
        logger.debug(s"Sent to dead-letter queue (extraction failure): ${describeWriterStageFailure(e)}"),
      )
    }

  private def describeWriterStageFailure(entry: (DlqKey, SinkRecord, Throwable)): String = {
    val (key, record, cause) = entry
    s"[${record.topic()}-${record.kafkaPartition()}-${record.kafkaOffset()} target=${key.target} " +
      s"reason=${cause.getMessage}]"
  }

  private def resolvePositions(
    errors:         Seq[BulkItemError],
    opsWithRecords: IndexedSeq[(BulkOp, SinkRecord)],
  ): Option[Seq[(DlqKey, SinkRecord, BulkItemError)]] = {
    val mapped: Seq[Option[(DlqKey, SinkRecord, BulkItemError)]] = errors.map { error =>
      if (error.position < 0 || error.position >= opsWithRecords.size) {
        logger.error(
          s"Could not identify which record caused a rejection from Elasticsearch/OpenSearch: " +
            s"response position ${error.position} is out of range (request had ${opsWithRecords.size} record(s); " +
            s"rejected: index=${error.index} id=${error.id}). The entire batch will be retried rather " +
            s"than risk discarding or DLQ-ing the wrong record.",
        )
        None
      } else {
        val (op, record) = opsWithRecords(error.position)
        // Corroborate by `id` only, not `index` (see class scaladoc: alias resolution).
        if (op.id == error.id) {
          if (op.index != error.index) {
            logger.debug(
              s"Bulk response item at position ${error.position} reported index '${error.index}' " +
                s"but the request op targeted '${op.index}' (id=${op.id}); treating as alias " +
                "resolution and mapping by id — the divert will use the KCQL-resolved target.",
            )
          }
          Some((DlqKey.forOp(op, record), record, error))
        } else {
          logger.error(
            s"The Elasticsearch/OpenSearch bulk response is out of order: response position ${error.position} " +
              s"has id='${error.id}' but the record sent at that position has id='${op.id}'. " +
              s"The entire batch will be retried to avoid discarding or DLQ-ing the wrong record.",
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
  ): DivertOutcome =
    if (toSend.isEmpty) {
      DivertOutcome.Diverted
    } else {
      val issued: Option[Seq[(DlqKey, SinkRecord, JFuture[Void])]] =
        try {
          Some(toSend.map { case (key, record, cause) => (key, record, reporter.report(record, cause)) })
        } catch {
          case NonFatal(e) =>
            logger.error(
              s"Failed to send ${toSend.size} record(s) to the dead-letter queue: ${e.getMessage}. " +
                s"If errors.tolerance=none is set (the default), Kafka Connect rejects DLQ writes — " +
                s"set errors.tolerance=all for dlq mode to work. The batch will be retried.",
              e,
            )
            None
        }

      issued match {
        case None          => DivertOutcome.NotDiverted
        case Some(futures) =>
          // One deadline for the whole call, computed once (not restarted per future): bounds
          // the call at reportTimeoutMs regardless of how many records are in `toSend`, instead
          // of up to N * reportTimeoutMs for N sequentially-awaited futures.
          val deadlineNanos   = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(reportTimeoutMs)
          val allAcknowledged = futures.forall { case (_, _, future) => awaitAck(future, deadlineNanos) }
          if (allAcknowledged) {
            futures.foreach { case (key, record, _) => reportedThisPut.put(key, record) }
            DivertOutcome.Diverted
          } else {
            DivertOutcome.NotDiverted
          }
      }
    }

  private def awaitAck(future: JFuture[Void], deadlineNanos: Long): Boolean = {
    // Never short-circuit on an exhausted deadline: a future that already completed must still count
    // as acknowledged. A zero timeout makes get() return immediately for completed futures.
    val remainingMs = math.max(TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime()), 0L)
    try {
      future.get(remainingMs, TimeUnit.MILLISECONDS)
      true
    } catch {
      case _: TimeoutException =>
        logger.error(
          s"Timed out waiting for the dead-letter queue to confirm a write (limit: ${reportTimeoutMs}ms, shared across this batch). " +
            s"The batch will be retried. Consider increasing bulk.item.errors.dlq.timeout.ms or checking dead-letter queue topic health.",
        )
        false
      case e: ExecutionException =>
        logger.error(
          s"The dead-letter queue rejected a write: ${Option(e.getCause).map(_.getMessage).getOrElse(e.getMessage)}. " +
            s"The batch will be retried.",
          e.getCause,
        )
        false
      case e: CancellationException =>
        logger.error("A dead-letter queue write was cancelled before it completed. The batch will be retried.", e)
        false
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        logger.error(
          "Interrupted while waiting for the dead-letter queue to confirm a write. The batch will be retried.",
          e,
        )
        false
    }
  }
}
