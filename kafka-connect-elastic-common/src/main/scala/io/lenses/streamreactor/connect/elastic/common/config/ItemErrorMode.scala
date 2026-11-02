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

import java.util.Locale

/**
 * Controls handling of a *permanent*, per-document bulk item error (HTTP 400).
 *
 *  - [[Fail]] (default): throws. Task stops, offsets uncommitted, batch redelivered on restart.
 *  - [[Drop]]: discards the record (WARN-logged), offsets advance. No DLQ required.
 *  - [[Dlq]]: reports the record to the DLQ, offsets advance. Requires a DLQ configured
 *    (`DlqGate.enabled`); otherwise behaves as [[Fail]].
 *
 * [[Drop]]/[[Dlq]] never divert batch-wide (403/404) or retriable (429/409) errors — only
 * per-document HTTP 400. See `BulkItemErrorClassifier.isDivertible`.
 */
sealed trait ItemErrorMode {

  /** The `bulk.item.errors` config value that resolves to this mode — the inverse of [[ItemErrorMode.fromString]]. */
  def configValue: String
}

object ItemErrorMode {
  case object Fail extends ItemErrorMode { val configValue = "fail" }
  case object Drop extends ItemErrorMode { val configValue = "drop" }
  case object Dlq  extends ItemErrorMode { val configValue = "dlq"  }

  def fromString(value: String): ItemErrorMode =
    Option(value).map(_.trim.toLowerCase(Locale.ROOT)) match {
      case Some("fail") => Fail
      case Some("drop") => Drop
      case Some("dlq")  => Dlq
      case other =>
        throw new ConfigException(
          s"Invalid value '${other.getOrElse("")}' for bulk.item.errors; expected 'fail', 'drop' or 'dlq'",
        )
    }
}
