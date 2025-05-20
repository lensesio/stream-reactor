/*
 * Copyright 2017-2025 Lenses.io Ltd
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

import io.lenses.streamreactor.connect.elastic7.config.ElasticConfig
import io.lenses.streamreactor.connect.elastic7.config.ElasticConfigConstants
import io.lenses.streamreactor.connect.elastic7.config.ElasticSettings
import com.sksamuel.elastic4s.ElasticNodeEndpoint

import scala.util.Failure
import scala.util.Success
import scala.util.Try

object ElasticWriter {

  /**
    * Construct a JSONWriter.
    *
    * @param config An elasticSinkConfig to extract settings from.
    * @return An ElasticJsonWriter to write records from Kafka to ElasticSearch.
    */
  def apply(config: ElasticConfig): ElasticJsonWriter = {

    val hostNames = config.getString(ElasticConfigConstants.HOSTS).split(",")
    val protocol  = config.getString(ElasticConfigConstants.PROTOCOL)
    val port      = config.getInt(ElasticConfigConstants.ES_PORT)
    val prefix = Try(config.getString(ElasticConfigConstants.ES_PREFIX)) match {
      case Success("")           => None
      case Success(configString) => Some(configString)
      case Failure(_)            => None
    }

    val settings = ElasticSettings(config)

    new ElasticJsonWriter(
      KElasticClient.createHttpClient(settings, endpoints(hostNames, protocol, port, prefix).toIndexedSeq),
      settings,
    )
  }

  private def endpoints(hostNames: Array[String], protocol: String, port: Integer, prefix: Option[String]) =
    hostNames
      .map(hostname => ElasticNodeEndpoint(protocol, hostname, port, prefix))
}
