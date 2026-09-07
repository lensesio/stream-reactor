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
package io.lenses.streamreactor.connect.elastic7

import io.lenses.streamreactor.connect.elastic.common.dlq.DlqGate
import io.lenses.streamreactor.connect.elastic.common.sink.AbstractElasticSinkTask
import io.lenses.streamreactor.connect.elastic.common.writer.JsonBulkWriter
import io.lenses.streamreactor.connect.elastic7.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic7.config.ElasticConfigConstants

import scala.jdk.CollectionConverters.MapHasAsJava

class ElasticSinkTask extends AbstractElasticSinkTask {

  override protected def asciiArtResource: String = "/elastic-ascii.txt"

  override protected def errorRetryIntervalKey: String = ElasticConfigConstants.ERROR_RETRY_INTERVAL

  override protected def progressEnabledKey: String = ElasticConfigConstants.PROGRESS_COUNTER_ENABLED

  override protected def createWriter(conf: Map[String, String], dlqGate: DlqGate): JsonBulkWriter = {
    ElasticConfig.config.parse(conf.asJava)
    ElasticWriter(ElasticConfig(conf), dlqGate)
  }
}
