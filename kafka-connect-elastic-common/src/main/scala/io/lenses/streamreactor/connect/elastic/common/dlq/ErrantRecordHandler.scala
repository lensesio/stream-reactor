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
 *  - `dlq`: awaits every `report()` future issued by one [[divertPermanent]]/[[divertOne]] call
 *    against a single shared deadline (`reportTimeoutMs` from the start of that call, not
 *    restarted per future), bounding the whole call at `reportTimeoutMs` regardless of how many
 *    records it reports.
 *  - `dlq`: catches throws from `report()` itself — under `errors.tolerance=none`,
 *    `RetryWithToleranceOperator.executeFailed` reports then throws
 *    (`ConnectException("Tolerance exceeded in error handler", ...)`) — and surfaces
 *    [[DivertOutcome.NotDiverted]] so the writer's own classified exception propagates instead
 *    of the generic tolerance message.
 *  - `drop`: no reporter, no ack wait, no tolerance interaction. Logged as one summary WARN per
 *    batch, capped at [[BulkItemErrorClassifier.MaxErrorsInMessage]] records of detail with the
 *    remainder at DEBUG — the log is the only trace of a discarded record.
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
   * Diverts a single writer-stage failure — a record that never became a `BulkOp` (e.g. a
   * `NullValueBehavior.FAIL` tombstone, PK extraction failure, unsupported write mode).
   * `target` (the KCQL target) stands in for dedup; there is no resolved index here.
   */
  def divertOne(
    record:          SinkRecord,
    target:          String,
    cause:           Throwable,
    mode:            ItemErrorMode,
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): Boolean =
    mode match {
      case ItemErrorMode.Drop =>
        // Not logged here: JsonBulkWriter.insert already logs one WARN per diverted
        // writer-stage record, for both drop and dlq.
        reportedThisPut.put(DlqKey.forTarget(record, target), record)
        true
      case ItemErrorMode.Dlq =>
        // See divertPermanent — gated on dlqGate.enabled, not dlqGate.reporter alone.
        (dlqGate.enabled, dlqGate.reporter) match {
          case (true, Some(reporter)) =>
            val key = DlqKey.forTarget(record, target)
            if (reportedThisPut.contains(key)) {
              true
            } else {
              reportAndAwait(reporter, Vector((key, record, cause)), reportedThisPut) == DivertOutcome.Diverted
            }
          case _ => false
        }
      case ItemErrorMode.Fail => false
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
        s"bulk.item.errors=drop: discarding ${toDrop.size} record(s); offsets will advance past them " +
          s"and they are not sent anywhere else: ${shown.map(describeDrop).mkString(", ")}$more",
      )
      toDrop.drop(shown.size).foreach(e => logger.debug(s"bulk.item.errors=drop: discarding ${describeDrop(e)}"))
    }
  }

  private def describeDrop(entry: (DlqKey, SinkRecord, BulkItemError)): String = {
    val (_, record, error) = entry
    s"[${record.topic()}-${record.kafkaPartition()}-${record.kafkaOffset()} index=${error.index} " +
      s"id=${error.id} status=${error.status} reason=${error.reason}]"
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
            "refusing to divert as the position cannot be trusted.",
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
            s"Bulk response item at position ${error.position} (id=${error.id}) does not match the " +
              s"request op at that position (id=${op.id}); the Bulk API may not have returned items " +
              "in request order. Refusing to divert for this batch as the mapping cannot be trusted.",
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
              s"ErrantRecordReporter.report threw while issuing ${toSend.size} record(s) to the DLQ " +
                s"(likely errors.tolerance=none — the record(s) may still have reached the DLQ before " +
                s"the throw); falling back to failing the batch: ${e.getMessage}",
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
    val remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime())
    if (remainingMs <= 0) {
      // The shared deadline was already exhausted by earlier futures in this same call.
      logger.error(
        s"Timed out after ${reportTimeoutMs}ms (shared across this report call) waiting for a DLQ write to be acknowledged.",
      )
      false
    } else {
      try {
        future.get(remainingMs, TimeUnit.MILLISECONDS)
        true
      } catch {
        case _: TimeoutException =>
          logger.error(s"Timed out after ${reportTimeoutMs}ms waiting for a DLQ write to be acknowledged.")
          false
        case e: ExecutionException =>
          logger.error(
            s"DLQ write failed: ${Option(e.getCause).map(_.getMessage).getOrElse(e.getMessage)}",
            e.getCause,
          )
          false
        case e: CancellationException =>
          // Not a subtype of ExecutionException. Treated as unacknowledged.
          logger.error("DLQ write future was cancelled before it could be acknowledged.", e)
          false
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          logger.error("Interrupted while waiting for a DLQ write to be acknowledged.", e)
          false
      }
    }
  }
}
