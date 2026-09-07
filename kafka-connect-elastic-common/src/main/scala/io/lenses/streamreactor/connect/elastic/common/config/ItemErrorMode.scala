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
package io.lenses.streamreactor.connect.elastic.common.config

import org.apache.kafka.common.config.ConfigException

/**
 * Controls what happens to a bulk item that fails with a *permanent*, per-document error
 * (HTTP 400, e.g. a mapper conflict).
 *
 *  - [[Fail]] (default): the connector throws. Kafka Connect kills the task with offsets
 *    uncommitted, so the whole batch is redelivered on restart. No data is lost, but the
 *    connector stops.
 *  - [[Drop]]: the failing record(s) are discarded (logged at WARN) and the batch is treated as
 *    handled so offsets advance past them, keeping the task running. No dead-letter queue is
 *    required.
 *  - [[Dlq]]: the failing record(s) are reported to the dead-letter queue and the batch is
 *    treated as handled so offsets advance past them, keeping the task running. Only takes
 *    effect when a DLQ is actually configured (see `DlqGate.enabled`); otherwise behaves as
 *    [[Fail]].
 *
 * [[Drop]] and [[Dlq]] never divert batch-wide conditions (403/404) or retriable errors
 * (429/409) — only per-document HTTP 400 rejections. See `BulkItemErrorClassifier.isDivertible`.
 */
sealed trait ItemErrorMode

object ItemErrorMode {
  case object Fail extends ItemErrorMode
  case object Drop extends ItemErrorMode
  case object Dlq  extends ItemErrorMode

  def fromString(value: String): ItemErrorMode =
    Option(value).map(_.trim.toLowerCase) match {
      case Some("fail") => Fail
      case Some("drop") => Drop
      case Some("dlq")  => Dlq
      case other =>
        throw new ConfigException(
          s"Invalid value '${other.getOrElse("")}' for bulk.item.errors; expected 'fail', 'drop' or 'dlq'",
        )
    }
}
