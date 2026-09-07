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
 * Controls what happens to a bulk item that fails with a *permanent* error (e.g. a mapper conflict)
 * once a real Kafka Connect dead-letter queue is available (see `DlqGate`).
 *
 *  - [[Fail]] (default): the connector throws. Kafka Connect kills the task with offsets
 *    uncommitted, so the whole batch is redelivered on restart. No data is lost, but the
 *    connector stops.
 *  - [[Skip]]: the failing record(s) are reported to the dead-letter queue and the batch is
 *    treated as handled so offsets advance past them, keeping the task running. Only takes
 *    effect when a DLQ is actually configured (see `DlqGate.enabled`); otherwise behaves as
 *    [[Fail]].
 */
sealed trait ItemErrorDlqMode

object ItemErrorDlqMode {
  case object Fail extends ItemErrorDlqMode
  case object Skip extends ItemErrorDlqMode

  def fromString(value: String): ItemErrorDlqMode =
    Option(value).map(_.trim.toLowerCase) match {
      case Some("fail") => Fail
      case Some("skip") => Skip
      case other =>
        throw new ConfigException(
          s"Invalid value '${other.getOrElse("")}' for bulk.item.errors.dlq; expected 'fail' or 'skip'",
        )
    }
}
