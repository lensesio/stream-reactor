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
   * Structural violations (empty index/id, an id over the cluster's 512-byte hard id limit) are
   * permanent, per-document faults, not transport failures. Left unvalidated, they'd surface as
   * a bare `RuntimeException`/`IllegalArgumentException` from deeper in the request path, which
   * [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter.handleTransportFailure]]
   * would reclassify as a *retriable* transport failure: pointless under `error.policy=RETRY`
   * (identical input, identical failure, every attempt) and unreportable to the DLQ. Returning
   * them as ordinary [[BulkItemError]]s instead routes them through the same strict/tolerant
   * machinery as every other per-item bulk error — mirrors
   * [[io.lenses.streamreactor.connect.opensearch.KOpenSearchClient.validateOp]] for ES7/OpenSearch
   * parity.
   *
   * Tagged with [[BulkItemErrorClassifier.ConnectorValidationErrorType]]: never divertible under
   * `bulk.item.errors=drop`/`dlq` (see [[BulkItemErrorClassifier.isDivertible]]), and always
   * surfaced even under `bulk.strict.item.errors=false` (see the tolerant-mode handling below) —
   * these are systemic (every record hitting the same misconfiguration fails identically), not a
   * one-off cluster rejection.
   */
  private def validateOp(op: BulkOp): Either[BulkItemError, Unit] = {
    def invalid(reason: String): Either[BulkItemError, Unit] =
      Left(
        BulkItemError(
          index     = op.index,
          id        = op.id,
          reason    = reason,
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

  override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = Try {
    // Single pass over `ops`, not validate-then-partition-then-partition-again: a
    // validate-all-into-Either, then `collect` the lefts, then `collect` the rights (each its
    // own traversal plus an intermediate `IndexedSeq[Either[...]]`) costs three traversals and
    // two throwaway collections on every call, for a validation failure that essentially never
    // fires in steady-state operation. Still calls validateOp exactly once per op either way —
    // the fast path is fewer traversals of the result, not fewer validations.
    val validationErrorsBuilder  = mutable.ListBuffer.empty[BulkItemError]
    val validOpsWithIndexBuilder = IndexedSeq.newBuilder[(BulkOp, Int)]
    ops.zipWithIndex.foreach {
      case (op, idx) =>
        validateOp(op) match {
          // Position set relative to the ORIGINAL request before any op is dropped, so DLQ
          // correlation (which trusts BulkItemError.position as an index into the original ops
          // list) stays correct even when only a subset of ops is actually sent to the cluster.
          case Left(error) => validationErrorsBuilder += error.copy(position = idx)
          case Right(())   => validOpsWithIndexBuilder += op -> idx
        }
    }
    val validationErrors: Seq[BulkItemError]         = validationErrorsBuilder.toSeq
    val validOpsWithIndex: IndexedSeq[(BulkOp, Int)] = validOpsWithIndexBuilder.result()

    val (tookMillis, responseItemErrors) =
      if (validOpsWithIndex.isEmpty) {
        // Every op failed local validation — nothing left to send to the cluster. `took=0` is
        // NOT "the cluster responded in 0ms" (no request was ever sent); logged distinctly
        // below rather than folded into the "Bulk write completed: took=Xms" line, which would
        // misleadingly imply a real round trip happened.
        logger.info(
          s"All ${ops.size} op(s) failed local validation; no bulk request was sent to the cluster.",
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
        // elastic4s's BulkResponse.items is a def that rebuilds (flatMap + zipWithIndex + map)
        // its list on every access — capture it once rather than recomputing it on each
        // reference below.
        val items = result.items

        // elastic4s assigns `itemId` positionally via zipWithIndex over its own decoded item
        // list (com.sksamuel.elastic4s.requests.bulk.BulkResponse.items). That list can differ
        // in length from our request (e.g. a decode step silently drops an item shape it
        // doesn't recognise), which would shift every later itemId. Guard it here: only trust
        // positions when the response item count matches the request op count. On mismatch,
        // disable position mapping for the whole batch (position = -1) rather than risk
        // correlating an error to the wrong Kafka record.
        val positionsTrustworthy = items.size == validOps.size
        if (!positionsTrustworthy) {
          logger.warn(
            s"Bulk response returned ${items.size} item(s) for a request of ${validOps.size} op(s); " +
              "cannot reliably correlate item errors to their originating records. Per-record DLQ " +
              "mapping (if configured) is disabled for this batch.",
          )
        }

        val errs: Seq[BulkItemError] = items.zipWithIndex.collect {
          case (item, idx) if item.error.isDefined =>
            val err = item.error.get
            BulkItemError(
              index     = item.index,
              id        = item.id,
              reason    = err.reason,
              errorType = err.`type`,
              status    = item.status,
              // Map the response-relative index back to the position in the ORIGINAL request,
              // since validation failures may have already been removed from what was actually
              // sent. `item.itemId` (elastic4s's own positional id, see above) is deliberately
              // NOT used here — it is derived the same way, from the same response list, so a
              // length mismatch would silently poison it too.
              position = if (positionsTrustworthy) validOpsWithIndex(idx)._2 else -1,
            )
        }
        logger.info(s"Bulk write completed: took=${took}ms, items=${items.size}")
        (took, errs)
      }

    // bulk.strict.item.errors=false only ever applied to genuine CLUSTER item errors (legacy
    // tolerant mode: log and drop). It must NOT swallow a connector-side validation error —
    // those are never divertible (see validateOp) and were an unconditional throw before
    // per-record diversion existed, so silently discarding them under tolerant mode would be a
    // new data-loss path with no way to opt out.
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
