/*
 * Copyright 2017-2024 Lenses.io Ltd
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

import cats.implicits.toBifunctorOps
import com.sksamuel.elastic4s.bulk.BulkRequest
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.typesafe.scalalogging.StrictLogging
import io.lenses.kcql.Kcql
import io.lenses.streamreactor.common.utils.CyclopsToScalaEither.convertToScalaEither
import io.lenses.streamreactor.common.utils.CyclopsToScalaOption.convertToScalaOption
import io.lenses.streamreactor.connect.elastic6.config.ElasticSettings
import io.lenses.streamreactor.connect.elastic6.indexname.CreateIndex.getIndexNameForAutoCreate
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.config.RequestConfig.Builder
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder

import scala.concurrent.Future

trait KElasticClient extends AutoCloseable {
  def index(kcql: Kcql): Unit

  def execute(definition: BulkRequest): Future[Any]
}

object KElasticClient extends StrictLogging {

  def createHttpClient(settings: ElasticSettings, endpoints: Seq[ElasticNodeEndpoint]): KElasticClient = {
    val maybeProvider: Option[BasicCredentialsProvider] = {
      for {
        httpBasicAuthUsername <- Option.when(settings.httpBasicAuthUsername.nonEmpty)(settings.httpBasicAuthUsername)
        httpBasicAuthPassword <- Option.when(settings.httpBasicAuthPassword.nonEmpty)(settings.httpBasicAuthPassword)
      } yield {
        val credentials = new UsernamePasswordCredentials(httpBasicAuthUsername, httpBasicAuthPassword)
        val provider    = new BasicCredentialsProvider
        provider.setCredentials(AuthScope.ANY, credentials)
        provider
      }
    }
    val client: ElasticClient = ElasticClient(
      ElasticProperties(endpoints),
      (requestConfigBuilder: Builder) => requestConfigBuilder,
      (httpClientBuilder: HttpAsyncClientBuilder) => {
        maybeProvider.foreach(httpClientBuilder.setDefaultCredentialsProvider)
        convertToScalaEither(settings.storesInfo.toSslContext)
          .map(convertToScalaOption)
          .leftMap(throw _)
          .merge
          .map(httpClientBuilder.setSSLContext)
        httpClientBuilder
      },
    )
    new HttpKElasticClient(client)
  }
}

class HttpKElasticClient(client: ElasticClient) extends KElasticClient {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def index(kcql: Kcql): Unit = {
    require(kcql.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target:${kcql.getTarget}")

    getIndexNameForAutoCreate(kcql).leftMap(throw _).map {
      indexName: String =>
        client.execute {
          createIndex(indexName)
        }
    }
    ()
  }

  override def execute(definition: BulkRequest): Future[Response[BulkResponse]] = client.execute(definition)

  override def close(): Unit = client.close()
}
