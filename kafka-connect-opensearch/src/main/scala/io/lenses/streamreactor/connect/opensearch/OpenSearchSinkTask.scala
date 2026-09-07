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

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonConfigConstants
import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.sink.AbstractElasticSinkTask
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings

import scala.jdk.CollectionConverters.MapHasAsJava

class OpenSearchSinkTask extends AbstractElasticSinkTask with StrictLogging {

  override protected def asciiArtResource: String = "/opensearch-ascii.txt"

  override protected def errorRetryIntervalKey: String = OpenSearchConfigConstants.ERROR_RETRY_INTERVAL

  override protected def progressEnabledKey: String = ElasticCommonConfigConstants.PROGRESS_COUNTER_ENABLED

  override protected def createWriter(conf: Map[String, String], dlqGate: DlqGate): JsonBulkWriter = {
    OpenSearchConfig.config.parse(conf.asJava)
    val osConfig = OpenSearchConfig(conf.asJava)
    val settings = OpenSearchSettings(osConfig)

    logger.info(
      "OpenSearch Sink Task starting: host(s)={}, port={}, protocol={}, awsSigning={}, " +
        "strictItemErrors={}, kcqlCount={}. " +
        "[Migration note] Use connect.opensearch.* keys; connect.elastic7.* keys are NOT forwarded here.",
      settings.hosts.mkString(","),
      settings.port,
      settings.protocol,
      settings.awsSigningEnabled,
      settings.strictItemErrors,
      settings.common.kcqls.size,
    )
    if (settings.port == 9300) {
      logger.warn(
        "[Migration] connect.opensearch.port is set to 9300, which is the Elasticsearch TCP transport port. " +
          "OpenSearch exposes its REST API on the HTTP port (default 9200). " +
          "Update connect.opensearch.port=9200 (or your cluster's HTTP port) to avoid connection failures.",
      )
    }

    OpenSearchWriter(osConfig, dlqGate)
  }
}
