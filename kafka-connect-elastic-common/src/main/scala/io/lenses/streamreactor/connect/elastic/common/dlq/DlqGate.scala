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
package io.lenses.streamreactor.connect.elastic.common.dlq

import org.apache.kafka.connect.sink.ErrantRecordReporter

/**
 * Resolves whether a real Kafka Connect dead-letter queue is available for this task.
 *
 * A non-null [[ErrantRecordReporter]] alone is NOT sufficient to conclude a DLQ exists. Kafka
 * hands a sink task a non-null reporter whenever `errors.deadletterqueue.topic.name` is set
 * **OR** `errors.log.enable=true`:
 *
 * {{{
 * // org.apache.kafka.connect.runtime.SinkConnectorConfig
 * public boolean enableErrantRecordReporter() {
 *     String dqlTopic = dlqTopicName();
 *     return !dqlTopic.isEmpty() || enableErrorLog();
 * }
 * }}}
 *
 * So `errors.log.enable=true` on its own produces a non-null reporter with no DLQ topic behind
 * it. Reporting in that state would write only a log line, then advance offsets past a document
 * that was never durably persisted anywhere — exactly the silent loss closed by
 * [[https://github.com/lensesio-dev/stream-reactor/pull/415 #415]]. [[enabled]] therefore requires
 * both signals: a non-null reporter AND a non-empty `errors.deadletterqueue.topic.name` read
 * directly from the task's own config map.
 */
final case class DlqGate(reporter: Option[ErrantRecordReporter], dlqTopicConfigured: Boolean) {
  def enabled: Boolean = reporter.isDefined && dlqTopicConfigured
}

object DlqGate {

  /** Standard Kafka Connect sink connector property; not connector-prefixed. */
  val DlqTopicNameConfigKey = "errors.deadletterqueue.topic.name"

  /** Standard Kafka Connect sink connector property; not connector-prefixed. */
  val ErrorsToleranceConfigKey = "errors.tolerance"

  val disabled: DlqGate = DlqGate(None, dlqTopicConfigured = false)

  def from(reporter: Option[ErrantRecordReporter], conf: Map[String, String]): DlqGate =
    DlqGate(
      reporter           = reporter,
      dlqTopicConfigured = conf.get(DlqTopicNameConfigKey).exists(_.trim.nonEmpty),
    )
}
