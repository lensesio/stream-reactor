/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.elastic6

import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.elastic6.config.ElasticSettings
import com.datamountaineer.streamreactor.connect.elastic6.indexname.CreateIndex.getIndexName
import com.sksamuel.elastic4s.bulk.BulkRequest
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticNodeEndpoint, ElasticProperties, Response}
import com.sksamuel.elastic4s.mappings.MappingDefinition
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig.Builder
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder

import scala.concurrent.Future

trait KElasticClient extends AutoCloseable {
  def index(kcql: Kcql)

  def execute(definition: BulkRequest): Future[Any]
}


object KElasticClient extends StrictLogging {

  def createHttpClient(settings: ElasticSettings, endpoints: Seq[ElasticNodeEndpoint]): KElasticClient = {
    if (settings.httpBasicAuthUsername.nonEmpty && settings.httpBasicAuthPassword.nonEmpty) {
      lazy val provider = {
        val provider = new BasicCredentialsProvider
        val credentials = new UsernamePasswordCredentials(settings.httpBasicAuthUsername, settings.httpBasicAuthPassword)
        provider.setCredentials(AuthScope.ANY, credentials)
        provider
      }

      val client: ElasticClient = ElasticClient(
        ElasticProperties(endpoints),
        (requestConfigBuilder: Builder) => requestConfigBuilder,
        (httpClientBuilder: HttpAsyncClientBuilder) => httpClientBuilder.setDefaultCredentialsProvider(provider)
      )
      new HttpKElasticClient(client)
    } else {
      val client: ElasticClient = ElasticClient(ElasticProperties(endpoints))
      new HttpKElasticClient(client)
    }
  }
}

class HttpKElasticClient(client: ElasticClient) extends KElasticClient {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def index(kcql: Kcql): Unit = {
    require(kcql.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target:${kcql.getTarget}")

    val indexName = getIndexName(kcql)
    client.execute {
      Option(kcql.getDocType) match {
        case None => createIndex(indexName)
        case Some(documentType) => createIndex(indexName).mappings(MappingDefinition(documentType))
      }
    }
  }

  override def execute(definition: BulkRequest): Future[Response[BulkResponse]] = client.execute(definition)

  override def close(): Unit = client.close()
}
