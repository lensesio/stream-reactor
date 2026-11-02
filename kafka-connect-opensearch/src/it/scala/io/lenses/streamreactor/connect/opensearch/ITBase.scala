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

import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkOp
import io.lenses.streamreactor.connect.elastic.common.bulk.BulkResult
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import io.lenses.streamreactor.connect.testcontainers.OpensearchContainer
import io.lenses.streamreactor.connect.testcontainers.SecurityPkiFixture
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch.core.SearchRequest
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder
import org.apache.hc.core5.http.HttpHost
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Seconds
import org.scalatest.time.Span

import scala.util.Try

/**
 * Shared base trait for OpenSearch integration tests.
 *
 * Provides:
 *  - Container lifecycle management (start/stop)
 *  - An `opensearch-java` `OpenSearchClient` pointing at the container
 *  - A polling helper for asserting document counts
 *  - Common test record helpers
 */
trait ITBase
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with ITResourceRegistry
    with Eventually
    with StrictLogging {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(60, Seconds), interval = Span(2, Seconds))

  val container: OpensearchContainer = OpensearchContainer()

  protected lazy val client: OpenSearchClient = buildClient()

  protected def buildClient(): OpenSearchClient = {
    val host      = container.hostNetwork.httpHostAddress.split(":")
    val httpHost  = new HttpHost("http", host(0), host(1).toInt)
    val transport = ApacheHttpClient5TransportBuilder.builder(httpHost).build()
    new OpenSearchClient(transport)
  }

  override def beforeAll(): Unit = {
    container.start()
    logger.info(
      s"[IT] Container started: image=${container.container.getDockerImageName}, address=${container.hostNetwork.httpHostAddress}",
    )
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    Try(client._transport().close())
    container.stop()
  }

  /** Poll until the given index has exactly `expectedCount` documents. */
  def awaitDocumentCount(index: String, expectedCount: Long): Unit = {
    val _ = eventually {
      val resp = client.search(
        (s: SearchRequest.Builder) => s.index(index),
        classOf[JsonNode],
      )
      resp.hits().total().value() shouldBe expectedCount
    }
  }

  protected val schema: Schema = SchemaBuilder.struct()
    .field("id", Schema.INT32_SCHEMA)
    .field("name", Schema.STRING_SCHEMA)
    .build()

  protected def sinkRecord(topic: String, offset: Int, id: Int, name: String): SinkRecord = {
    val struct = new Struct(schema).put("id", id).put("name", name)
    new SinkRecord(topic, 0, Schema.STRING_SCHEMA, s"key-$id", schema, struct, offset.toLong)
  }

  protected def baseProps(
    host:  String,
    port:  Int,
    kcql:  String,
    extra: Map[String, String] = Map.empty,
  ): Map[String, String] =
    Map(
      HOSTS   -> host,
      ES_PORT -> port.toString,
      KCQL    -> kcql,
    ) ++ extra

  protected def makeKClient(props: Map[String, String]): KOpenSearchClient = {
    val config    = OpenSearchConfig(props)
    val settings  = OpenSearchSettings(config)
    val transport = OpenSearchTransportFactory.create(settings)
    val osClient  = new OpenSearchClient(transport)
    track(new KOpenSearchClient(osClient, settings))
  }

  protected def bulkWrite(kClient: KOpenSearchClient, ops: Seq[BulkOp]): BulkResult =
    kClient.bulk(ops).get

  /**
   * Build a raw [[OpenSearchClient]] for direct REST calls (e.g. `/_plugins/_security/whoami`).
   * When `maybePki` is provided, the client is configured with the PKI keystore and truststore.
   */
  protected def openSearchClient(
    host:     String,
    port:     Int,
    maybePki: Option[SecurityPkiFixture] = None,
  ): OpenSearchClient = {
    val httpHost = new HttpHost("https", host, port)
    val builder  = ApacheHttpClient5TransportBuilder.builder(httpHost)
    builder.setMapper(new JacksonJsonpMapper())
    maybePki.foreach { pki =>
      builder.setHttpClientConfigCallback { b =>
        val sslCtx = buildSslContextFromPki(pki)
        val cm = PoolingAsyncClientConnectionManagerBuilder.create()
          .setTlsStrategy(new DefaultClientTlsStrategy(sslCtx))
          .build()
        b.setConnectionManager(cm)
      }
    }
    new OpenSearchClient(builder.build())
  }

  private def buildSslContextFromPki(pki: SecurityPkiFixture): javax.net.ssl.SSLContext = {
    import java.io.FileInputStream
    import java.security.KeyStore
    import javax.net.ssl.KeyManagerFactory
    import javax.net.ssl.TrustManagerFactory
    import javax.net.ssl.SSLContext

    val keyStore = KeyStore.getInstance("JKS")
    val ksStream = new FileInputStream(pki.keystorePath.toFile)
    try keyStore.load(ksStream, "changeit".toCharArray)
    finally ksStream.close()

    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(keyStore, "changeit".toCharArray)

    val trustStore = KeyStore.getInstance("JKS")
    val tsStream   = new FileInputStream(pki.truststorePath.toFile)
    try trustStore.load(tsStream, "changeit".toCharArray)
    finally tsStream.close()

    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(trustStore)

    val sslCtx = SSLContext.getInstance("TLS")
    sslCtx.init(kmf.getKeyManagers, tmf.getTrustManagers, null)
    sslCtx
  }
}
