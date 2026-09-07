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

  // ES6 has no registered bulk.item.errors key at all (see createWriter below) — this key is
  // never present in confMap, so validateItemErrorConfiguration's lookup always falls back to
  // the "fail" default, which matches ES6's actual pinned behaviour.
  override protected def itemErrorModeConfigKey: String =
    s"${ElasticConfigConstants.CONNECTOR_PREFIX}.${io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants.BULK_ITEM_ERRORS_SUFFIX}"

  override protected def strictItemErrorsConfigKey: String = ElasticConfigConstants.BULK_STRICT_ITEM_ERRORS_KEY

  // ES6: bulk.item.errors not registered (KElastic6BulkClient doesn't populate
  // BulkItemError.position) — itemErrorMode stays Fail regardless of dlqGate.
  //
  // Side effect: JsonBulkWriter.handleTransportFailure (shared) reclassifies transport failures
  // as RetriableIntegrityException instead of a bare RuntimeException. NOOP no longer swallows
  // them (previously WARN-logged, offsets advanced); RETRY still retries them.
  override protected def createWriter(conf: Map[String, String], dlqGate: DlqGate): JsonBulkWriter = {
    ElasticConfig.config.parse(conf.asJava)
    ElasticWriter(ElasticConfig(conf), dlqGate)
  }
}
