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
 * Classification uses HTTP `status` and `errorType` only. The item `reason` is consulted
 * solely when `errorType` is empty, and only as a prefix match — mapper errors embed a
 * preview of the rejected document in `reason`, so a substring match would treat a
 * permanent 400 as write-queue saturation.
 *
 * Write-queue saturation (HTTP 429 / `es_rejected_execution_exception`) and version
 * conflicts (HTTP 409 / `version_conflict_engine_exception`) are transient: the batch is
 * safe to re-deliver. Mapping conflicts and other document-level rejections are permanent:
 * retrying the same document cannot succeed, so they must fail the task rather than burn the
 * retry budget or silently drop records.
 *
 * Mixed batches (any permanent error alongside retriable ones) are treated as permanent — a
 * poison-pill document would otherwise loop until `max.retries` is exhausted.
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

  private def typeIsRetriable(normalizedType: String): Boolean =
    retriableTypes.contains(normalizedType) ||
      retriableTypesCompact.contains(normalizedType.replace("_", ""))

  private def reasonLooksRetriable(reason: String): Boolean = {
    val lower = Option(reason).getOrElse("").trim.toLowerCase(Locale.ROOT)
    retriableReasonPrefixes.exists(p => lower.startsWith(p))
  }

  private def normalize(s: String): String =
    Option(s).getOrElse("").toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_")
      .stripPrefix("_").stripSuffix("_")
}
