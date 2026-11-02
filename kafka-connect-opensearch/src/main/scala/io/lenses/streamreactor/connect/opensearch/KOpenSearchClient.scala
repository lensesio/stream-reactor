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
package io.lenses.streamreactor.connect.opensearch

import com.fasterxml.jackson.databind.JsonNode
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
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.apache.kafka.connect.errors.ConnectException
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch._types.OpenSearchException
import org.opensearch.client.opensearch.core.BulkRequest
import org.opensearch.client.opensearch.core.bulk.BulkOperation
import org.opensearch.client.opensearch.core.bulk.BulkResponseItem
import org.opensearch.client.opensearch.core.bulk.DeleteOperation
import org.opensearch.client.opensearch.core.bulk.IndexOperation
import org.opensearch.client.opensearch.core.bulk.UpdateOperation

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * [[KBulkClient]] implementation backed by the native `opensearch-java` 2.x client.
 *
 * Key behaviours:
 * - Probes the cluster's distribution and version eagerly at writer construction via [[start()]].
 * - Applies `strictItemErrors` to decide how to handle per-item bulk failures.
 * - Uses the synchronous `OpenSearchClient` (no `Future.sequence` / `Await`).
 */
class KOpenSearchClient(client: OpenSearchClient, settings: OpenSearchSettings) extends KBulkClient with StrictLogging {

  private val closed = new AtomicBoolean(false)

  private def probeClusterCompatibility(): Unit = {
    val info =
      try client.info()
      catch {
        case e: Exception =>
          throw new ConnectException("Failed to probe OpenSearch cluster version", e)
      }

    val distribution = info.version().distribution()
    val version      = info.version().number()

    if (distribution == "elasticsearch") {
      throw new ConnectException(
        s"connect.opensearch.* is configured against an Elasticsearch cluster " +
          s"(distribution=$distribution, version=$version); " +
          s"this connector targets OpenSearch only. " +
          s"Use kafka-connect-elastic7 (or the future kafka-connect-elastic8 connector) instead.",
      )
    }

    if (version.startsWith("1.")) {
      throw new ConnectException(
        s"OpenSearch 1.x cluster (version $version) is not supported by this connector — " +
          s"bulk per-item error shape differs from 2.x. " +
          s"Use a 2.x cluster or pin to a connector version with 1.x support.",
      )
    }

    if (!version.startsWith("2.")) {
      logger.warn(
        s"OpenSearch cluster version is $version; this connector is tested against the 2.x series. " +
          s"1.x clusters are not supported (incompatible bulk response shape on per-item errors); " +
          s"3.x clusters may work but are unverified. " +
          s"Pin Dependencies.openSearchVersion to match the cluster's major if you encounter shape drift.",
      )
    }
  }

  /** Eagerly probes cluster compatibility at writer construction, surfacing misconfiguration at SinkTask.start. */
  override def start(): Try[Unit] = Try(probeClusterCompatibility())

  /**
   * Validates structural constraints (empty index/id, an id over the 512-byte hard limit)
   * locally — the moral equivalent of an HTTP 400 `illegal_argument_exception` — rather than
   * letting them surface as a bare `IllegalArgumentException` before the request is sent, which
   * [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter.handleTransportFailure]]
   * would reclassify as retriable (pointless: identical input fails identically on retry) and
   * unreportable to the DLQ. Returned as ordinary [[BulkItemError]]s instead, tagged with
   * [[BulkItemErrorClassifier.ConnectorValidationErrorType]] (not `illegal_argument_exception`):
   * never divertible (see [[BulkItemErrorClassifier.isDivertible]]) and always surfaced even
   * under `bulk.strict.item.errors=false` (see tolerant-mode handling in [[bulk]] below), since
   * these are systemic rather than a one-off cluster rejection.
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
          s"BulkOp id exceeds OpenSearch's 512-byte hard limit (got $idBytes bytes); " +
            s"reduce topic name length, shorten PK fields, or set a smaller pk.separator. " +
            s"Offending id prefix: ${op.id.take(64)}",
        )
      } else {
        Right(())
      }
    }
  }

  private def toBulkOperation(op: BulkOp): BulkOperation = op match {
    case InsertOp(index, id, json, pipeline, _) =>
      val indexOpBuilder = new IndexOperation.Builder[JsonNode]()
        .index(index)
        .id(id)
        .document(json)
      pipeline.foreach(p => indexOpBuilder.pipeline(p))
      new BulkOperation.Builder().index(indexOpBuilder.build()).build()

    case UpsertOp(index, id, json, _) =>
      val updateOp = new UpdateOperation.Builder[JsonNode]()
        .index(index)
        .id(id)
        .document(json)
        .docAsUpsert(true)
        .retryOnConflict(Int.box(ElasticCommonConfigConstants.UPSERT_RETRY_ON_CONFLICT))
        .build()
      new BulkOperation.Builder().update(updateOp).build()

    case DeleteOp(index, id, _) =>
      val deleteOp = new DeleteOperation.Builder()
        .index(index)
        .id(id)
        .build()
      new BulkOperation.Builder().delete(deleteOp).build()
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
  // [[io.lenses.streamreactor.connect.elastic7.KElasticBulkClient.bulk]] — same four stages
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
      // Every op failed local validation — nothing left to send to the cluster. `took=0` is
      // NOT "the cluster responded in 0ms" (no request was ever sent).
      logger.info(
        s"All $totalOps op(s) failed local validation; no bulk request was sent to the cluster.",
      )
      (0L, Seq.empty[BulkItemError])
    } else {
      val validOps = validOpsWithIndex.map(_._1)
      val bulkOps: java.util.List[BulkOperation] = validOps.map(toBulkOperation).asJava
      val request  = new BulkRequest.Builder().operations(bulkOps).build()
      val response = client.bulk(request)

      val took = response.took()
      logger.info(s"Bulk write completed: took=${took}ms, items=${bulkOps.size()}")

      if (!response.errors()) {
        (took, Seq.empty[BulkItemError])
      } else {
        val responseItems = response.items().asScala.toIndexedSeq
        (took, itemErrorsFrom(responseItems, validOpsWithIndex))
      }
    }
  }

  private def itemErrorsFrom(
    responseItems:     IndexedSeq[BulkResponseItem],
    validOpsWithIndex: IndexedSeq[(BulkOp, Int)],
  ): Seq[BulkItemError] = {
    // opensearch-java's BulkResponseItem has no ordinal field — position is the item's
    // index within response.items(), trusted only if it lines up 1:1 with the ops sent
    // (zipWithIndex must run before the filter, or a dropped success item shifts every
    // later index). On a length mismatch, disable position mapping for the whole batch
    // (position = -1) rather than risk mis-correlating an error.
    val positionsTrustworthy = responseItems.size == validOpsWithIndex.size
    if (!positionsTrustworthy) {
      logger.warn(
        s"Bulk response returned ${responseItems.size} item(s) for a request of ${validOpsWithIndex.size} op(s); " +
          "cannot reliably correlate item errors to their originating records. Per-record DLQ " +
          "mapping (if configured) is disabled for this batch.",
      )
    }

    responseItems.zipWithIndex
      .collect {
        case (item, idx) if item.error() != null =>
          BulkItemError(
            index     = Option(item.index()).getOrElse(""),
            id        = Option(item.id()).getOrElse(""),
            reason    = Option(item.error().reason()).getOrElse(""),
            errorType = Option(item.error().`type`()).getOrElse(""),
            status    = item.status(),
            // Map the response-relative index back to the position in the ORIGINAL
            // request, since validation failures may have already been removed from
            // what was actually sent.
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
    if (responseItemErrors.nonEmpty && !settings.strictItemErrors) {
      logger.warn(
        s"Bulk write completed with ${BulkItemErrorClassifier.formatItemErrors(responseItemErrors)} (tolerant mode)",
      )
    }
    val clusterErrorsToSurface = if (settings.strictItemErrors) responseItemErrors else Seq.empty
    val itemErrors             = validationErrors ++ clusterErrorsToSurface

    if (itemErrors.isEmpty) {
      BulkResult(took = tookMillis, errors = false, itemErrors = Seq.empty)
    } else {
      logger.error(s"Bulk write completed with ${BulkItemErrorClassifier.formatItemErrors(itemErrors)}")
      BulkResult(took = tookMillis, errors = true, itemErrors = itemErrors)
    }
  }

  override def createIndex(name: String): Try[Unit] =
    Try {
      client.indices().create(b => b.index(name))
      ()
    } match {
      case Failure(e: OpenSearchException)
          if Option(e.error()).flatMap(err => Option(err.`type`())).contains("resource_already_exists_exception") =>
        logger.debug(s"Index $name already exists — skipping auto-create (idempotent restart)")
        Success(())
      case Failure(e) =>
        logger.error(s"Failed to auto-create index $name: ${e.getMessage}")
        Failure(new ConnectException(s"Failed to auto-create index '$name'", e))
      case ok => ok
    }

  override def close(): Unit =
    if (closed.compareAndSet(false, true)) {
      try client._transport().close()
      catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          logger.warn(s"Interrupted while closing OpenSearch transport: ${e.getMessage}")
        case e: Exception =>
          logger.warn(s"Error closing OpenSearch transport: ${e.getMessage}")
      }
    }
}
