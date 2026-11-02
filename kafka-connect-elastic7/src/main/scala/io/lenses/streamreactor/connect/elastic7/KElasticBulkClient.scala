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
package io.lenses.streamreactor.connect.elastic7

import com.sksamuel.elastic4s.ElasticDsl
import com.sksamuel.elastic4s.Index
import com.sksamuel.elastic4s.requests.bulk.BulkResponseItem
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemError
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkItemErrorClassifier
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkResult
import io.lenses.streamreactor.connect.elastic.common.bulk.DeleteOp
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.elastic.common.bulk.KBulkClient
import io.lenses.streamreactor.connect.elastic.common.bulk.UpsertOp
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/**
 * Adapts the elastic4s-based [[KElasticClient]] to the [[KBulkClient]] trait used by the shared
 * [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter]].
 *
 * HTTP-transport errors are surfaced via the returned `Try`. Per-item bulk errors are controlled
 * by [[strictItemErrors]] (config `connect.elastic.bulk.strict.item.errors`, default true):
 *  - true: `BulkResult.errors=true` so JsonBulkWriter classifies 429 vs mapper errors
 *  - false: logged at WARN and dropped (legacy tolerant mode)
 *
 * @param writeTimeoutMillis timeout in **milliseconds** for `Await.result` on the bulk Future.
 *                           The same value is applied as the HTTP connect/socket timeout on
 *                           [[KElasticClient.createHttpClient]]. Default 300000 = 5 minutes.
 */
class KElasticBulkClient(
  client:             KElasticClient,
  writeTimeoutMillis: Int,
  strictItemErrors:   Boolean = true,
) extends KBulkClient
    with StrictLogging {

  /**
   * Validates structural constraints (empty index/id, an id over the 512-byte hard id limit)
   * locally rather than letting them surface as a bare `RuntimeException` from deeper in the
   * request path, which
   * [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter.handleTransportFailure]]
   * would reclassify as retriable (pointless: identical input fails identically on retry) and
   * unreportable to the DLQ. Returned as ordinary [[BulkItemError]]s instead, tagged with
   * [[BulkItemErrorClassifier.ConnectorValidationErrorType]] — never divertible (see
   * [[BulkItemErrorClassifier.isDivertible]]) and always surfaced even under
   * `bulk.strict.item.errors=false` (see tolerant-mode handling below), since these are systemic
   * rather than a one-off cluster rejection. Mirrors
   * [[io.lenses.streamreactor.connect.opensearch.KOpenSearchClient.validateOp]].
   */
  private def validateOp(op: BulkOp, position: Int): Either[BulkItemError, Unit] = {
    def invalid(reason: String): Either[BulkItemError, Unit] =
      Left(
        BulkItemError(
          index     = op.index,
          id        = op.id,
          reason    = reason,
          position  = position,
          errorType = BulkItemErrorClassifier.ConnectorValidationErrorType,
          status    = 400,
        ),
      )

    if (op.index.isEmpty) {
      invalid(s"BulkOp index must be non-empty (id=${op.id})")
    } else if (op.id.isEmpty) {
      invalid("BulkOp id must be non-empty")
    } else {
      val idBytes = op.id.getBytes(StandardCharsets.UTF_8).length
      if (idBytes > 512) {
        invalid(
          s"BulkOp id exceeds Elasticsearch's 512-byte hard limit (got $idBytes bytes); " +
            s"reduce topic name length, shorten PK fields, or set a smaller pk.separator. " +
            s"Offending id prefix: ${op.id.take(64)}",
        )
      } else {
        Right(())
      }
    }
  }

  private def toElasticRequest(op: BulkOp) = {
    import ElasticDsl._
    op match {
      case InsertOp(index, id, json, pipeline, _) =>
        indexInto(new Index(index))
          .id(id)
          .pipeline(pipeline.orNull)
          .source(json.toString)

      case UpsertOp(index, id, json, _) =>
        updateById(new Index(index), id)
          .docAsUpsert(json.toString)
          .retryOnConflict(ElasticCommonConfigConstants.UPSERT_RETRY_ON_CONFLICT)

      case DeleteOp(index, id, _) =>
        deleteById(new Index(index), id)
    }
  }

  /**
   * Local-validation outcome for one `bulk` call: `validOpsWithIndex` retains each surviving
   * op's position in the ORIGINAL request, since only a subset may end up sent to the cluster.
   */
  private case class ValidatedOps(
    validationErrors:  Seq[BulkItemError],
    validOpsWithIndex: IndexedSeq[(BulkOp, Int)],
  )

  // Structurally identical to
  // [[io.lenses.streamreactor.connect.opensearch.KOpenSearchClient.bulk]] — same four stages
  // (validate, send, correlate, merge) — but kept independent rather than shared: the two
  // diverge in where exactly the cluster-error scan happens and what the completion log
  // reports, and unifying those would force a behavioural choice neither side asked for.
  override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = Try {
    val validated                       = partitionValidated(ops)
    val (tookMillis, clusterItemErrors) = executeValidOps(validated, ops.size)
    buildResult(tookMillis, validated.validationErrors, clusterItemErrors)
  }

  private def partitionValidated(ops: Seq[BulkOp]): ValidatedOps = {
    // Single pass over `ops` (vs. validate-all-into-Either then collect twice).
    val validationErrorsBuilder  = mutable.ListBuffer.empty[BulkItemError]
    val validOpsWithIndexBuilder = IndexedSeq.newBuilder[(BulkOp, Int)]
    validationErrorsBuilder.sizeHint(ops.size)
    validOpsWithIndexBuilder.sizeHint(ops.size)
    // Lazy iterator: no intermediate collection of (op, idx) pairs.
    ops.iterator.zipWithIndex.foreach {
      case (op, idx) =>
        // `idx` is the position in the ORIGINAL request, so DLQ correlation stays correct even
        // when only a subset of ops is actually sent to the cluster.
        validateOp(op, idx) match {
          case Left(error) => validationErrorsBuilder += error
          case Right(())   => validOpsWithIndexBuilder += op -> idx
        }
    }
    ValidatedOps(validationErrorsBuilder.toSeq, validOpsWithIndexBuilder.result())
  }

  private def executeValidOps(validated: ValidatedOps, totalOps: Int): (Long, Seq[BulkItemError]) = {
    val validOpsWithIndex = validated.validOpsWithIndex
    if (validOpsWithIndex.isEmpty) {
      // Every op failed local validation — nothing sent to the cluster. `took=0` doesn't mean
      // a 0ms round trip; logged distinctly rather than folded into the "Bulk write completed"
      // line below.
      logger.info(
        s"All $totalOps op(s) failed local validation; no bulk request was sent to the cluster.",
      )
      (0L, Seq.empty[BulkItemError])
    } else {
      val validOps        = validOpsWithIndex.map(_._1)
      val elasticRequests = validOps.map(toElasticRequest)
      val response        = Await.result(client.execute(ElasticDsl.bulk(elasticRequests)), writeTimeoutMillis.millis)

      if (response.isError) {
        throw new RuntimeException(s"Elastic bulk transport error: ${response.error.reason}")
      }

      val result = response.result
      val took   = result.took
      // BulkResponse.items is a def that rebuilds its list on every access — capture once.
      val items = result.items

      val errs = itemErrorsFrom(items, validOpsWithIndex)
      logger.info(s"Bulk write completed: took=${took}ms, items=${items.size}")
      (took, errs)
    }
  }

  private def itemErrorsFrom(
    items:             Seq[BulkResponseItem],
    validOpsWithIndex: IndexedSeq[(BulkOp, Int)],
  ): Seq[BulkItemError] = {
    // elastic4s's own `itemId` is assigned positionally over this same decoded list, so a
    // length mismatch against the request (e.g. a decode step dropping an unrecognised item
    // shape) would shift every later itemId too. Guard here: trust positions only when the
    // response item count matches the request op count; otherwise disable position mapping
    // for the whole batch (position = -1) rather than risk mis-correlating an error.
    val positionsTrustworthy = items.size == validOpsWithIndex.size
    if (!positionsTrustworthy) {
      logger.warn(
        s"Bulk response returned ${items.size} item(s) for a request of ${validOpsWithIndex.size} op(s); " +
          "cannot reliably correlate item errors to their originating records. Per-record DLQ " +
          "mapping (if configured) is disabled for this batch.",
      )
    }

    items.zipWithIndex.collect {
      case (item, idx) if item.error.isDefined =>
        val err = item.error.get
        BulkItemError(
          index     = item.index,
          id        = item.id,
          reason    = err.reason,
          errorType = err.`type`,
          status    = item.status,
          // Map back to the position in the ORIGINAL request (validation failures may have
          // already removed ops from what was sent). Not `item.itemId`: derived from the
          // same response list, so a length mismatch would poison it too.
          position = if (positionsTrustworthy) validOpsWithIndex(idx)._2 else -1,
        )
    }
  }

  private def buildResult(
    tookMillis:         Long,
    validationErrors:   Seq[BulkItemError],
    responseItemErrors: Seq[BulkItemError],
  ): BulkResult = {
    // bulk.strict.item.errors=false (legacy tolerant mode: log and drop) applies only to genuine
    // cluster item errors, never to connector-side validation errors (see validateOp) — those
    // are never divertible and must always surface.
    if (responseItemErrors.nonEmpty && !strictItemErrors) {
      logger.warn(
        s"Bulk write completed with ${BulkItemErrorClassifier.formatItemErrors(responseItemErrors)} (tolerant mode)",
      )
    }
    val clusterErrorsToSurface = if (strictItemErrors) responseItemErrors else Seq.empty
    val itemErrors             = validationErrors ++ clusterErrorsToSurface

    if (itemErrors.isEmpty) {
      BulkResult(took = tookMillis, errors = false, itemErrors = Seq.empty)
    } else {
      logger.error(s"Bulk write completed with ${BulkItemErrorClassifier.formatItemErrors(itemErrors)}")
      BulkResult(took = tookMillis, errors = true, itemErrors = itemErrors)
    }
  }

  override def createIndex(name: String): Try[Unit] = Try {
    client.createIndex(name)
  }

  override def close(): Unit = client.close()
}

object KElasticBulkClient {
  def apply(client: KElasticClient, settings: ElasticCommonSettings): KElasticBulkClient =
    new KElasticBulkClient(client, settings.writeTimeout, settings.strictItemErrors)
}
