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

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.transport.aws.AwsSdk2Transport
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.http.ExecutableHttpRequest
import software.amazon.awssdk.http.HttpExecuteRequest
import software.amazon.awssdk.http.SdkHttpClient
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region

import java.time.Duration

/**
 * Integration tests for AWS SigV4 signing using WireMock as the target.
 *
 * WireMock stubs verify that:
 * (a) The signed POST /_bulk request reaches WireMock with an Authorization header (B2).
 * (b) The signed GET / info probe also carries Authorization (B2).
 * (c) 302 redirect from WireMock is NOT followed (credential replay prevention) (C2).
 *
 * C5 - LocalStack compromise note:
 * A full LocalStack-backed `OpenSearchSigV4LocalStackIT` would provide stronger guarantees
 * (real SigV4 endpoint verification, actual credential chain resolution), but it requires:
 *  - A LocalStack Docker image in testcontainers (>2 GB)
 *  - OpenSearch Serverless endpoint configuration on LocalStack
 *  - Additional `software.amazon.awssdk:opensearchserverless` dependencies
 *
 * The WireMock-only IT is the accepted compromise for this project: it verifies that the
 * `Authorization: AWS4-HMAC-SHA256 ...` header is present and well-formed on BOTH the info probe
 * and the bulk request, which covers the connector's signing behaviour without requiring a real
 * AWS endpoint. The risk accepted by this compromise is that the SigV4 signature ALGORITHM is
 * not verified end-to-end against a real AWS endpoint; the AWS SDK's own unit tests cover that.
 */
class OpenSearchSigV4WireMockIT extends AnyFunSuite with Matchers with BeforeAndAfterAll with ITResourceRegistry {

  private var wireMock: WireMockServer = _

  override def beforeAll(): Unit = {
    wireMock = new WireMockServer(wireMockConfig().dynamicPort())
    wireMock.start()
  }

  override def afterAll(): Unit =
    wireMock.stop()

  private def doc = JsonNodeFactory.instance.objectNode().put("field", "value")

  /**
   * Thin [[SdkHttpClient]] wrapper that downgrades the URI scheme from `https` to `http` before
   * forwarding to the underlying client.
   *
   * [[AwsSdk2Transport]] always builds HTTPS URIs and adds the SigV4 `Authorization` header BEFORE
   * calling `prepareRequest`, so by the time this wrapper sees the request the header is already
   * present. Downgrading to HTTP here lets the actual TCP call reach plain-HTTP WireMock while
   * keeping the signed header intact — which is all this IT needs to assert.
   */
  private class HttpDowngradeClient(delegate: SdkHttpClient) extends SdkHttpClient {
    override def prepareRequest(req: HttpExecuteRequest): ExecutableHttpRequest = {
      val modified = req.httpRequest().toBuilder.protocol("http").build()
      delegate.prepareRequest(
        HttpExecuteRequest.builder()
          .request(modified)
          .contentStreamProvider(req.contentStreamProvider().orElse(null))
          .build(),
      )
    }
    override def close():      Unit   = delegate.close()
    override def clientName(): String = delegate.clientName()
  }

  /**
   * Build a [[KOpenSearchClient]] backed by a real [[AwsSdk2Transport]] pointed at the WireMock HTTP
   * server on the given `service` name.
   *
   * We construct the transport directly rather than routing through [[OpenSearchTransportFactory]] /
   * [[OpenSearchSettings]] to avoid the HTTPS-enforcement guard that exists in production code
   * (signing over HTTP exposes credentials in a real deployment but is acceptable for a local
   * WireMock IT whose sole job is to assert that the `Authorization` header is present and
   * well-formed on every outbound request).
   *
   * [[AwsSdk2Transport]] always uses HTTPS internally; [[HttpDowngradeClient]] intercepts the
   * signed request and downgrades the URI scheme to HTTP so it reaches the plain-HTTP WireMock
   * server while the `Authorization` header remains intact.
   */
  private def makeSigV4Client(service: String): KOpenSearchClient = {
    val host = "localhost"
    val port = wireMock.port()

    val credentials = AwsBasicCredentials.create(
      "AKIAIOSFODNN7EXAMPLE",
      "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )
    val apacheClient = track(ApacheHttpClient.builder()
      .socketTimeout(Duration.ofMillis(10000))
      .connectionTimeout(Duration.ofMillis(10000))
      .build())
    val httpClient = new HttpDowngradeClient(apacheClient)
    val options = AwsSdk2TransportOptions.builder()
      .setMapper(new JacksonJsonpMapper())
      .setCredentials(StaticCredentialsProvider.create(credentials))
      .build()
    val transport = new AwsSdk2Transport(httpClient, s"$host:$port", service, Region.of("us-east-1"), options)
    val osClient  = new OpenSearchClient(transport)

    // Settings are needed only for KOpenSearchClient's strictItemErrors / lifecycle behaviour;
    // they do NOT affect the transport's signing — that is entirely owned by AwsSdk2Transport above.
    val settingsProps = Map(
      HOSTS   -> host,
      ES_PORT -> port.toString,
      KCQL    -> "INSERT INTO idx SELECT * FROM topic",
    )
    val settings = OpenSearchSettings(OpenSearchConfig(settingsProps))
    val kClient  = track(new KOpenSearchClient(osClient, settings))
    // Trigger the info probe (GET /) so its Authorization header is captured by WireMock.
    // Each test sets up the GET / stub before calling makeSigV4Client.
    kClient.start().fold(throw _, identity)
    kClient
  }

  private val infoBody =
    """{
      |  "name": "test",
      |  "cluster_name": "test",
      |  "cluster_uuid": "test-cluster-uuid",
      |  "version": {
      |    "distribution": "opensearch",
      |    "number": "2.13.0",
      |    "build_type": "tar",
      |    "build_hash": "0000000000000000000000000000000000000000",
      |    "build_date": "2024-01-01T00:00:00.000Z",
      |    "build_snapshot": false,
      |    "lucene_version": "9.10.0",
      |    "minimum_wire_compatibility_version": "7.10.0",
      |    "minimum_index_compatibility_version": "7.0.0",
      |    "build_flavor": "default"
      |  },
      |  "tagline": "The OpenSearch Project"
      |}""".stripMargin

  test("B2: signed bulk POST reaches WireMock with Authorization header on both info probe and bulk") {
    wireMock.resetAll()
    wireMock.stubFor(
      get(urlEqualTo("/")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(infoBody),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo("/_bulk")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json")
          .withBody("""{"took":1,"errors":false,"items":[]}"""),
      ),
    )

    val kClient = makeSigV4Client("es")
    val result  = kClient.bulk(Seq(InsertOp("idx", "1", doc, None, None)))

    result.isSuccess shouldBe true
    result.get.errors shouldBe false

    // B2: The info() probe (GET /) MUST carry a SigV4 Authorization header.
    wireMock.verify(
      getRequestedFor(urlEqualTo("/"))
        .withHeader("Authorization", matching("AWS4-HMAC-SHA256.*")),
    )
    // Bulk POST must also be signed.
    wireMock.verify(
      postRequestedFor(urlEqualTo("/_bulk"))
        .withHeader("Authorization", matching("AWS4-HMAC-SHA256.*")),
    )
  }

  test("B3: aoss signing service — Authorization header contains service=aoss in credential scope") {
    wireMock.resetAll()
    wireMock.stubFor(
      get(urlEqualTo("/")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(infoBody),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo("/_bulk")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json")
          .withBody("""{"took":1,"errors":false,"items":[]}"""),
      ),
    )

    val kClient = makeSigV4Client("aoss")
    val result  = kClient.bulk(Seq(InsertOp("idx", "3", doc, None, None)))

    result.isSuccess shouldBe true

    // B3: Authorization header credential scope must name "aoss" as the signing service.
    // Format: AWS4-HMAC-SHA256 Credential=<key>/<date>/us-east-1/aoss/aws4_request, ...
    wireMock.verify(
      postRequestedFor(urlEqualTo("/_bulk"))
        .withHeader("Authorization", matching(".*us-east-1/aoss/aws4_request.*")),
    )
    wireMock.verify(
      getRequestedFor(urlEqualTo("/"))
        .withHeader("Authorization", matching(".*us-east-1/aoss/aws4_request.*")),
    )
  }

  test("C2: 302 to same-host redirect path is not followed — zero requests land on redirect target") {
    wireMock.resetAll()
    // The redirect target is a different path on the SAME WireMock host.
    // We stub it to return a valid response — if redirect-following were enabled,
    // WireMock would record a request there. We assert zero such requests.
    val redirectTarget = "/_bulk_redirected"
    wireMock.stubFor(
      get(urlEqualTo("/")).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json").withBody(infoBody),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo("/_bulk")).willReturn(
        aResponse().withStatus(302)
          .withHeader("Location", s"http://localhost:${wireMock.port()}$redirectTarget"),
      ),
    )
    wireMock.stubFor(
      post(urlEqualTo(redirectTarget)).willReturn(
        aResponse().withStatus(200).withHeader("Content-Type", "application/json")
          .withBody("""{"took":1,"errors":false,"items":[]}"""),
      ),
    )

    val kClient = makeSigV4Client("es")
    val result  = kClient.bulk(Seq(InsertOp("idx", "2", doc, None, None)))

    // The no-redirect behaviour means the bulk must fail (302 is not a valid bulk response).
    result.isFailure shouldBe true

    // Crucially: the redirect target must have received ZERO requests.
    wireMock.verify(0, postRequestedFor(urlEqualTo(redirectTarget)))
  }
}
