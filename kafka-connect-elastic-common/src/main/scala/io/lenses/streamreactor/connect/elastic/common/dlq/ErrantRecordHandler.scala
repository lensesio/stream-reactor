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
 *  - Maps a [[BulkItemError]] to a record by `position`; refuses the mapping unless `position`
 *    is in range AND the response item's `id` matches the op at that position. Refuses the
 *    whole call ([[DivertOutcome.NotDiverted]]) on any mismatch — applies to both `drop` and
 *    `dlq`.
 *  - Does not compare `index`: a write alias resolves to a concrete backing index in the
 *    response, so `op.index` (alias) never equals `error.index` (concrete) even when the
 *    mapping is correct. `id` alone corroborates ordering; `DlqKey.forOp` keys on the op's
 *    (KCQL-resolved) index.
 *  - `dlq`: awaits every `report()` future under `reportTimeoutMs` before treating a batch as
 *    diverted. `WorkerErrantRecordReporter.maybeThrowAsyncError()` only throws outside
 *    tolerance limits, so it gives no signal under `errors.tolerance=all`.
 *  - `dlq`: catches throws from `report()` itself. Under `errors.tolerance=none`,
 *    `RetryWithToleranceOperator.executeFailed` reports, then throws
 *    (`ConnectException("Tolerance exceeded in error handler", ...)`). Caught here and surfaced
 *    as [[DivertOutcome.NotDiverted]] so the writer's own classified exception propagates
 *    instead of the generic tolerance message.
 *  - `drop`: no reporter, no ack wait, no tolerance interaction. Each drop is logged
 *    individually at WARN with the error detail — its only trace.
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
        val key = DlqKey.forTarget(record, target)
        if (!reportedThisPut.contains(key)) {
          dropOne(key, record, target, cause, reportedThisPut)
        }
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

  private def dropAll(
    mapped:          Seq[(DlqKey, SinkRecord, BulkItemError)],
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): Unit =
    mapped
      .filterNot { case (key, _, _) => reportedThisPut.contains(key) }
      .foreach {
        case (key, record, error) =>
          reportedThisPut.put(key, record)
          logger.warn(
            s"bulk.item.errors=drop: discarding ${record.topic()}-${record.kafkaPartition()}-${record.kafkaOffset()} " +
              s"(index=${error.index} id=${error.id} status=${error.status} reason=${error.reason}); " +
              "offsets will advance past this record and it is not sent anywhere else.",
          )
      }

  private def dropOne(
    key:             DlqKey,
    record:          SinkRecord,
    target:          String,
    cause:           Throwable,
    reportedThisPut: mutable.Map[DlqKey, SinkRecord],
  ): Unit = {
    reportedThisPut.put(key, record)
    logger.warn(
      s"bulk.item.errors=drop: discarding ${record.topic()}-${record.kafkaPartition()}-${record.kafkaOffset()} " +
        s"(target=$target) after a writer-stage failure: ${cause.getMessage}; " +
        "offsets will advance past this record and it is not sent anywhere else.",
    )
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
        // Corroborate by `id` only, not `index`: a write alias resolves to its concrete backing
        // index in the response, so op.index (alias) != error.index (concrete) even when
        // correct. DlqKey.forOp keys on op.index (the KCQL-resolved target) regardless.
        if (op.id == error.id) {
          if (op.index != error.index) {
            logger.debug(
              s"Bulk response item at position ${error.position} reported index '${error.index}' " +
                s"but the request op targeted '${op.index}' (id=${op.id}); treating this as alias " +
                "resolution (the target is a write alias resolved to its concrete backing index) " +
                "and mapping by id. The divert will use the KCQL-resolved target, not the " +
                "concrete index reported by the cluster.",
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
        case None => DivertOutcome.NotDiverted
        case Some(futures) =>
          val allAcknowledged = futures.forall { case (_, _, future) => awaitAck(future) }
          if (allAcknowledged) {
            futures.foreach { case (key, record, _) => reportedThisPut.put(key, record) }
            DivertOutcome.Diverted
          } else {
            DivertOutcome.NotDiverted
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
        // Not a subtype of ExecutionException. Treated as unacknowledged.
        logger.error("DLQ write future was cancelled before it could be acknowledged.", e)
        false
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        logger.error("Interrupted while waiting for a DLQ write to be acknowledged.", e)
        false
    }
}
