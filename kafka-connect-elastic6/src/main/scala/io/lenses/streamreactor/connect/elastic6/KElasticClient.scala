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

import com.sksamuel.elastic4s.bulk.BulkRequest
import com.sksamuel.elastic4s.http._
import com.sksamuel.elastic4s.http.bulk.BulkResponse
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow
import io.lenses.streamreactor.connect.elastic.common.config.ElasticCommonSettings
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.config.RequestConfig.Builder
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

trait KElasticClient extends AutoCloseable {
  def execute(definition: BulkRequest): Future[Response[BulkResponse]]

  def createIndex(indexName: String): Unit
}

object KElasticClient extends StrictLogging {

  def createHttpClient(settings: ElasticCommonSettings, endpoints: Seq[ElasticNodeEndpoint]): KElasticClient = {
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
      (requestConfigBuilder: Builder) =>
        requestConfigBuilder
          .setConnectTimeout(settings.writeTimeout)
          .setSocketTimeout(settings.writeTimeout)
          .setConnectionRequestTimeout(settings.writeTimeout),
      (httpClientBuilder: HttpAsyncClientBuilder) => {
        maybeProvider.foreach(httpClientBuilder.setDefaultCredentialsProvider)
        unpackOrThrow(settings.storesInfo.toSslContext).map(httpClientBuilder.setSSLContext(_))
        httpClientBuilder
      },
    )
    new HttpKElasticClient(client)
  }
}

class HttpKElasticClient(client: ElasticClient) extends KElasticClient {

  import com.sksamuel.elastic4s.http.ElasticDsl._

  override def execute(definition: BulkRequest): Future[Response[BulkResponse]] = client.execute(definition)

  override def createIndex(indexName: String): Unit = {
    import com.sksamuel.elastic4s.http.ElasticDsl.{ createIndex => dslCreateIndex }
    // Await completion so the index exists before the first bulk request is sent.
    val _ = Await.result(client.execute(dslCreateIndex(indexName)), 30.seconds)
  }

  override def close(): Unit = client.close()
}
