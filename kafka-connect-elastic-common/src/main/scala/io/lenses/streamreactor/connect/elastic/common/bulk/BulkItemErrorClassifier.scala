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

/**
 * Classifies per-item Elasticsearch/OpenSearch bulk failures.
 *
 * Write-queue saturation (HTTP 429 / `es_rejected_execution_exception`) is transient: the batch
 * is safe to re-deliver. Mapping conflicts and other document-level rejections are permanent:
 * retrying the same document cannot succeed, so they must fail the task rather than burn the
 * retry budget or silently drop records.
 *
 * Mixed batches (any permanent error alongside retriable ones) are treated as permanent — a
 * poison-pill document would otherwise loop until `max.retries` is exhausted.
 */
object BulkItemErrorClassifier {

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
  )

  private val retriableReasonFragments: Seq[String] = Seq(
    "es_rejected_execution",
    "rejected_execution",
    "too_many_requests",
    "circuit_breaking",
  )

  def isRetriable(error: BulkItemError): Boolean = {
    if (error.status == 429) true
    else {
      val t = normalize(error.errorType)
      val r = normalize(error.reason)
      retriableTypes.contains(t) ||
      retriableTypes.exists(rt => t.contains(rt) || r.contains(rt)) ||
      retriableReasonFragments.exists(f => t.contains(f) || r.contains(f))
    }
  }

  def exceptionFor(itemErrors: Seq[BulkItemError]): ConnectException = {
    val msg = s"Bulk request had ${itemErrors.size} item-level error(s): " +
      itemErrors.map { e =>
        s"[index=${e.index} id=${e.id} type=${e.errorType} status=${e.status} reason=${e.reason}]"
      }.mkString(", ")
    if (itemErrors.nonEmpty && itemErrors.forall(isRetriable)) {
      new RetriableIntegrityException(msg)
    } else {
      new FatalConnectException(msg)
    }
  }

  private def normalize(s: String): String =
    Option(s).getOrElse("").toLowerCase.replaceAll("[^a-z0-9]+", "_")
}
