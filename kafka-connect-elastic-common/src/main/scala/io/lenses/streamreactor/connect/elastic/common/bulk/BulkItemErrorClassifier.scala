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
package io.lenses.streamreactor.connect.elastic.common.bulk

import io.lenses.streamreactor.common.errors.FatalConnectException
import io.lenses.streamreactor.common.errors.RetriableIntegrityException
import org.apache.kafka.connect.errors.ConnectException

import java.util.Locale

/**
 * Classifies per-item Elasticsearch/OpenSearch bulk failures.
 *
 *  - Uses HTTP `status` and `errorType`. `reason` is checked only when `errorType` is empty,
 *    as a prefix match (mapper errors embed the rejected document in `reason`).
 *  - Retriable: HTTP 429 (`es_rejected_execution_exception`), HTTP 409
 *    (`version_conflict_engine_exception`).
 *  - Permanent: mapping conflicts and other document-level rejections.
 *  - Mixed batch (permanent + retriable) is classified permanent.
 */
object BulkItemErrorClassifier {

  val MaxErrorsInMessage: Int = 10

  private val retriableTypes: Set[String] = Set(
    "es_rejected_execution_exception",
    "rejected_execution_exception",
    "circuit_breaking_exception",
    "too_many_requests",
    "too_many_requests_exception",
    "process_cluster_event_timeout_exception",
    "receive_timeout_transport_exception",
    "timeout_exception",
    "unavailable_shards_exception",
    "no_shard_available_action_exception",
    "not_master_exception",
    "master_not_discovered_exception",
    "node_not_connected_exception",
    "version_conflict_engine_exception",
  )

  private val retriableTypesCompact: Set[String] = retriableTypes.map(_.replace("_", ""))

  /** Prefixes matched against `reason` only when `errorType` is empty. */
  private val retriableReasonPrefixes: Seq[String] = Seq(
    "esrejectedexecution",
    "es_rejected_execution",
    "rejected execution",
    "rejected_execution",
    "too many requests",
    "too_many_requests",
    "circuit_breaking",
  )

  def isRetriable(error: BulkItemError): Boolean =
    if (error.status == 429 || error.status == 409) true
    else {
      val t = normalize(error.errorType)
      if (t.nonEmpty) typeIsRetriable(t)
      else reasonLooksRetriable(error.reason)
    }

  /**
   * Whether a permanent item error is divertible under `bulk.item.errors=drop`/`dlq`.
   *
   *  - HTTP 400 only. Per-document rejections (`mapper_parsing_exception`,
   *    `illegal_argument_exception`, `strict_dynamic_mapping_exception`, etc) are always 400.
   *  - Status-based, not a type allow-list: covers renamed/new document-level error types
   *    without a connector release.
   *  - Excludes 403 (`cluster_block_exception`, `security_exception`) and 404
   *    (`index_not_found_exception`) — batch-wide, not per-document.
   *  - Also excludes the HTTP 400 faults that are batch-wide despite the per-item status code
   *    (see [[isBatchWide]]): a closed index and a missing ingest pipeline. Every document
   *    routed to a closed index / through a missing pipeline fails identically, so diverting
   *    them would discard perfectly-good records while the task reported itself healthy — the
   *    exact silent-data-loss case the 403/404 exclusions guard against. The
   *    [[io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter.handleBulkResult]]
   *    backstop only trips when a *non*-divertible error is mixed in, so an all-400 response of
   *    these faults must be caught here rather than there.
   */
  def isDivertible(error: BulkItemError): Boolean =
    !isRetriable(error) && error.status == 400 && !isBatchWide(error)

  /**
   * HTTP 400 error types that are batch-wide (caused by index/cluster state, not the content of
   * any single document) and therefore must never be diverted:
   *
   *  - `index_closed_exception`: every write to a closed index fails; the fix is reopening the
   *    index, not dropping records.
   */
  private val batchWide400Types: Set[String] = Set(
    "index_closed_exception",
  )

  private val batchWide400TypesCompact: Set[String] = batchWide400Types.map(_.replace("_", ""))

  /**
   * Whether an HTTP 400 item error is really batch-wide. Detected two ways:
   *
   *  - by `errorType`, for types that are unambiguously batch-wide ([[batchWide400Types]]);
   *  - by `reason`, for a missing ingest pipeline, whose `errorType` (`illegal_argument_exception`)
   *    collides with genuine per-document rejections. Elasticsearch/OpenSearch report it as
   *    "pipeline with id [x] does not exist", which is batch-wide because the pipeline is absent
   *    for every document routed through it, not just this one.
   *
   * The reason check is a conservative fail-safe: at worst it pins an oddly-worded per-document
   * rejection to `fail` (never data loss).
   */
  private def isBatchWide(error: BulkItemError): Boolean = {
    val t = normalize(error.errorType)
    batchWide400Types.contains(t) ||
    batchWide400TypesCompact.contains(t.replace("_", "")) ||
    reasonLooksBatchWide(error.reason)
  }

  private def reasonLooksBatchWide(reason: String): Boolean = {
    val lower = normalize(reason)
    lower.contains("pipeline with id") && lower.contains("does not exist")
  }

  def formatItemErrors(itemErrors: Seq[BulkItemError]): String = {
    val shown = itemErrors.take(MaxErrorsInMessage)
    val body = shown.map { e =>
      s"[index=${e.index} id=${e.id} type=${e.errorType} status=${e.status} reason=${e.reason}]"
    }.mkString(", ")
    val extra = itemErrors.size - shown.size
    val more  = if (extra > 0) s", ... and $extra more" else ""
    s"${itemErrors.size} item-level error(s): $body$more"
  }

  def exceptionFor(itemErrors: Seq[BulkItemError]): ConnectException = {
    val msg = s"Bulk request had ${formatItemErrors(itemErrors)}"
    if (itemErrors.nonEmpty && itemErrors.forall(isRetriable)) {
      new RetriableIntegrityException(msg)
    } else {
      new FatalConnectException(msg)
    }
  }

  /**
   * `errorType` format differs by connector:
   *  - ES6/ES7 ([[io.lenses.streamreactor.connect.elastic7.KElasticBulkClient]]): snake_case,
   *    e.g. `es_rejected_execution_exception`.
   *  - OpenSearch ([[io.lenses.streamreactor.connect.opensearch.KOpenSearchClient]]): camelCase
   *    class names, e.g. `EsRejectedExecutionException`.
   *
   * Matched after [[normalize]] against both snake_case and the underscore-stripped form.
   */
  private def typeIsRetriable(normalizedType: String): Boolean =
    retriableTypes.contains(normalizedType) ||
      retriableTypesCompact.contains(normalizedType.replace("_", ""))

  private def reasonLooksRetriable(reason: String): Boolean = {
    val lower = Option(reason).getOrElse("").trim.toLowerCase(Locale.ROOT)
    retriableReasonPrefixes.exists(p => lower.startsWith(p))
  }

  private def normalize(s: String): String =
    Option(s).getOrElse("").trim.toLowerCase(Locale.ROOT)
}
