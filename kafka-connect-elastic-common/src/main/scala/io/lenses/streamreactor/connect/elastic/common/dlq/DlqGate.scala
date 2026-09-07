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
 * Resolves whether a real Kafka Connect DLQ is available for this task.
 *
 * A non-null [[ErrantRecordReporter]] alone is not sufficient: Connect hands out a reporter
 * whenever `errors.deadletterqueue.topic.name` is set **OR** `errors.log.enable=true`:
 *
 * {{{
 * // org.apache.kafka.connect.runtime.SinkConnectorConfig
 * public boolean enableErrantRecordReporter() {
 *     String dqlTopic = dlqTopicName();
 *     return !dqlTopic.isEmpty() || enableErrorLog();
 * }
 * }}}
 *
 * [[enabled]] requires both: a non-null reporter AND a non-empty
 * `errors.deadletterqueue.topic.name` read from the task's config.
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
