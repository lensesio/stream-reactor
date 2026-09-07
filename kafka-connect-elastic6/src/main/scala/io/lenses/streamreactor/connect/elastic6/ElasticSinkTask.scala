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
package io.lenses.streamreactor.connect.elastic6

import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.sink.AbstractElasticSinkTask
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic6.config.ElasticConfigConstants

import scala.jdk.CollectionConverters.MapHasAsJava

class ElasticSinkTask extends AbstractElasticSinkTask {

  override protected def asciiArtResource: String = "/elastic-ascii.txt"

  override protected def errorRetryIntervalKey: String = ElasticConfigConstants.ERROR_RETRY_INTERVAL

  override protected def progressEnabledKey: String = ElasticConfigConstants.PROGRESS_COUNTER_ENABLED

  // NB: ES6 does not register bulk.item.errors.dlq in its ConfigDef and KElastic6BulkClient never
  // populates BulkItemError.position, so ElasticCommonSettings.itemErrorDlqMode stays at its Fail
  // default here regardless of dlqGate — the DLQ feature itself is a no-op for ES6.
  //
  // RELEASE NOTE — ES6 IS affected by one side effect of this feature despite the above:
  // JsonBulkWriter.handleTransportFailure (shared across ES6/ES7/OpenSearch) now reclassifies a
  // bulk transport failure (connection reset, timeout, etc) as RetriableIntegrityException
  // instead of a bare RuntimeException. Under connect.elastic.error.policy=NOOP this used to be
  // swallowed with a WARN log, silently advancing offsets past unwritten records; it now fails
  // the task instead (still retried under error.policy=RETRY while the retry budget remains).
  // This closes a pre-existing silent-data-loss hole, but it is a genuine behaviour change for
  // any ES6 deployment running with error.policy=NOOP that upgrades to this version.
  override protected def createWriter(conf: Map[String, String], dlqGate: DlqGate): JsonBulkWriter = {
    ElasticConfig.config.parse(conf.asJava)
    ElasticWriter(ElasticConfig(conf), dlqGate)
  }
}
