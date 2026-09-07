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
package io.lenses.streamreactor.connect.opensearch

import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.opensearch.client.opensearch.OpenSearchClient

object OpenSearchWriter {

  def apply(config: OpenSearchConfig, dlqGate: DlqGate = DlqGate.disabled): JsonBulkWriter = {
    val settings  = OpenSearchSettings(config)
    val transport = OpenSearchTransportFactory.create(settings)
    val osClient  = new OpenSearchClient(transport)
    val kClient   = new KOpenSearchClient(osClient, settings)
    new JsonBulkWriter(kClient, settings.common, dlqGate)
  }
}
