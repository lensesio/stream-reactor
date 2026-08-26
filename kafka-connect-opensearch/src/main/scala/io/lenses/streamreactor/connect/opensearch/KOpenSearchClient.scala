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
import org.opensearch.client.opensearch.core.bulk.DeleteOperation
import org.opensearch.client.opensearch.core.bulk.IndexOperation
import org.opensearch.client.opensearch.core.bulk.UpdateOperation

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicBoolean
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

  private def validateOp(op: BulkOp): Unit = {
    if (op.index.isEmpty)
      throw new IllegalArgumentException(s"BulkOp index must be non-empty (id=${op.id})")
    if (op.id.isEmpty)
      throw new IllegalArgumentException("BulkOp id must be non-empty")
    val idBytes = op.id.getBytes(StandardCharsets.UTF_8).length
    if (idBytes > 512)
      throw new IllegalArgumentException(
        s"BulkOp id exceeds OpenSearch's 512-byte hard limit (got $idBytes bytes); " +
          s"reduce topic name length, shorten PK fields, or set a smaller pk.separator. " +
          s"Offending id prefix: ${op.id.take(64)}",
      )
  }

  override def bulk(ops: Seq[BulkOp]): Try[BulkResult] = Try {
    ops.foreach(validateOp)

    val bulkOps: java.util.List[BulkOperation] = ops.map {
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
    }.asJava

    val request  = new BulkRequest.Builder().operations(bulkOps).build()
    val response = client.bulk(request)

    val tookMillis = response.took()
    logger.info(s"Bulk write completed: took=${tookMillis}ms, items=${bulkOps.size()}")

    if (!response.errors()) {
      BulkResult(took = tookMillis, errors = false, itemErrors = Seq.empty)
    } else {
      val itemErrors: Seq[BulkItemError] = response.items().asScala
        .filter(item => item.error() != null)
        .map(item =>
          BulkItemError(
            index     = Option(item.index()).getOrElse(""),
            id        = Option(item.id()).getOrElse(""),
            reason    = Option(item.error().reason()).getOrElse(""),
            errorType = Option(item.error().`type`()).getOrElse(""),
            status    = item.status(),
          ),
        )
        .toSeq

      if (settings.strictItemErrors) {
        logger.error(s"Bulk write completed with ${BulkItemErrorClassifier.formatItemErrors(itemErrors)}")
        BulkResult(took = tookMillis, errors = true, itemErrors = itemErrors)
      } else {
        logger.warn(
          s"Bulk write completed with ${BulkItemErrorClassifier.formatItemErrors(itemErrors)} (tolerant mode)",
        )
        BulkResult(took = tookMillis, errors = false, itemErrors = Seq.empty)
      }
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
