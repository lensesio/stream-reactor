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

package com.datamountaineer.streamreactor.connect.elastic7

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.datamountaineer.kcql.Kcql
import com.datamountaineer.streamreactor.connect.elastic7.config.ElasticSettings
import com.datamountaineer.streamreactor.connect.elastic7.indexname.CreateIndex.getIndexName
import com.sksamuel.elastic4s.aws.Aws4RequestSigner
import com.sksamuel.elastic4s.requests.bulk.BulkRequest
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import com.sksamuel.elastic4s.{ElasticClient, ElasticNodeEndpoint, ElasticProperties, Response, http}
import com.sksamuel.elastic4s.http.JavaClient
import com.typesafe.scalalogging.StrictLogging
import org.apache.http.{HttpRequest, HttpRequestInterceptor}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig.Builder
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.protocol.HttpContext
import org.apache.kafka.connect.errors.ConnectException
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback

import scala.concurrent.Future

trait KElasticClient extends AutoCloseable {
  def index(kcql: Kcql): Unit

  def execute(definition: BulkRequest): Future[Any]
}


object KElasticClient extends StrictLogging {

  def createHttpClient(settings: ElasticSettings, endpoints: Seq[ElasticNodeEndpoint]): KElasticClient = {

    settings.awsCredentials match {
      case Some(credentials) =>
        val signed: HttpClientConfigCallback = new SignedClientConfig(settings)
        val javaClient: JavaClient = JavaClient(ElasticProperties(endpoints), signed)
        val elasticClient = ElasticClient(javaClient)
        new HttpKElasticClient(elasticClient)

      case _ =>

        if (settings.httpBasicAuthUsername.nonEmpty && settings.httpBasicAuthPassword.nonEmpty) {
          lazy val provider = {
            val provider = new BasicCredentialsProvider
            val credentials = new UsernamePasswordCredentials(settings.httpBasicAuthUsername, settings.httpBasicAuthPassword)
            provider.setCredentials(AuthScope.ANY, credentials)
            provider
          }

          val javaClient = JavaClient(ElasticProperties(endpoints),
            (requestConfigBuilder: Builder) => requestConfigBuilder,
            (httpClientBuilder: HttpAsyncClientBuilder) => httpClientBuilder.setDefaultCredentialsProvider(provider))

          val client: ElasticClient = ElasticClient(javaClient)
          new HttpKElasticClient(client)
        } else {
          val client : ElasticClient = ElasticClient(JavaClient(ElasticProperties(endpoints)))
          new HttpKElasticClient(client)
        }
    }
  }

  private class SignedClientConfig(settings: ElasticSettings) extends HttpClientConfigCallback {
    override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder =
      httpClientBuilder.addInterceptorLast(new Aws4HttpRequestInterceptor(settings))
  }

  private class Aws4HttpRequestInterceptor(settings: ElasticSettings) extends HttpRequestInterceptor {

    private val (key, secret, region) = settings.awsCredentials match {
      case Some(credentials) => (credentials.secretKey, credentials.accessKey, credentials.region)
      case _ => throw new ConnectException("No AWS credentials configured")
    }

    private val chainProvider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(key, secret))
    private val signer        = new Aws4RequestSigner(chainProvider, region)

    override def process(request: HttpRequest, context: HttpContext): Unit = signer.withAws4Headers(request)
  }
}

class HttpKElasticClient(client: ElasticClient) extends KElasticClient {

  import com.sksamuel.elastic4s.ElasticDsl._

  override def index(kcql: Kcql): Unit = {
    require(kcql.isAutoCreate, s"Auto-creating indexes hasn't been enabled for target: [${kcql.getTarget}]")

    val indexName = getIndexName(kcql)
    client.execute {
      createIndex(indexName)
    }
  }

  override def execute(definition: BulkRequest): Future[Response[BulkResponse]] = client.execute(definition)

  override def close(): Unit = client.close()
}