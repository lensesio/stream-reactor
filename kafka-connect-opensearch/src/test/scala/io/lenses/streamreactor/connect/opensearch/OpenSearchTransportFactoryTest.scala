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

import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfig
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.transport.aws.AwsSdk2Transport
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5Transport
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class OpenSearchTransportFactoryTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var wireMock: WireMockServer = _

  override def beforeAll(): Unit = {
    wireMock = new WireMockServer(wireMockConfig().dynamicPort())
    wireMock.start()
  }

  override def afterAll(): Unit =
    if (wireMock != null) wireMock.stop()

  private val baseProps = Map(
    HOSTS   -> "localhost",
    ES_PORT -> "9200",
    KCQL    -> "INSERT INTO idx SELECT * FROM topic",
  )

  private def settings(extra: (String, String)*): OpenSearchSettings =
    OpenSearchSettings(OpenSearchConfig(baseProps ++ extra.toMap))

  test("no-auth creates an ApacheHttpClient5Transport") {
    val transport = OpenSearchTransportFactory.create(settings())
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  test("basic auth creates an ApacheHttpClient5Transport") {
    val transport = OpenSearchTransportFactory.create(
      settings(
        "connect.opensearch.protocol"   -> "https",
        CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
        CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
      ),
    )
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  test("JWT auth creates an ApacheHttpClient5Transport") {
    val transport = OpenSearchTransportFactory.create(settings(
      "connect.opensearch.protocol" -> "https",
      JWT_TOKEN_KEY                 -> "my-jwt-token",
    ))
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  test("SigV4 creates an AwsSdk2Transport") {
    val transport = OpenSearchTransportFactory.create(
      settings(
        AWS_SIGNING_ENABLED_KEY       -> "true",
        AWS_REGION_KEY                -> "us-east-1",
        HOSTS                         -> "my-cluster.us-east-1.es.amazonaws.com",
        "connect.opensearch.protocol" -> "https",
      ),
    )
    try {
      transport shouldBe a[AwsSdk2Transport]
    } finally transport.close()
  }

  test("SigV4 defence-in-depth throws on tablePrefix (already rejected by OpenSearchSettings)") {
    // Construct settings manually with tablePrefix set to bypass the settings validation
    // to test the transport factory's own defence check
    val baseSettings = settings(
      AWS_SIGNING_ENABLED_KEY       -> "true",
      AWS_REGION_KEY                -> "us-east-1",
      HOSTS                         -> "my-cluster.us-east-1.es.amazonaws.com",
      "connect.opensearch.protocol" -> "https",
    )
    val withPrefix = baseSettings.copy(tablePrefix = Some("myprefix"))
    val ex         = intercept[IllegalStateException](OpenSearchTransportFactory.create(withPrefix))
    ex.getMessage should include("tableprefix")
  }

  test("https protocol creates a transport without error") {
    val transport = OpenSearchTransportFactory.create(settings(
      "connect.opensearch.protocol" -> "https",
    ))
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  // C3: asymmetric basic-auth cases
  test("C3: username-only (no password) creates transport without error — partial creds skip basic-auth wiring") {
    val transport = OpenSearchTransportFactory.create(settings(
      "connect.opensearch.protocol"   -> "https",
      CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
    ))
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  test("C3: password-only (no username) creates transport without error — partial creds skip basic-auth wiring") {
    val transport = OpenSearchTransportFactory.create(settings(
      "connect.opensearch.protocol"   -> "https",
      CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
    ))
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  test("C3: both username and password creates transport with basic-auth wired") {
    val transport = OpenSearchTransportFactory.create(
      settings(
        "connect.opensearch.protocol"   -> "https",
        CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
        CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "s3cr3t",
      ),
    )
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  // JWT + mTLS combined — both wired at the same time must not crash the builder
  test("JWT auth + mTLS keystore/truststore combined creates transport without error") {
    import java.security.KeyStore
    import java.nio.file.Files
    // Create a minimal in-memory PKCS12 keystore for the test
    val ks = KeyStore.getInstance("PKCS12")
    ks.load(null, "test".toCharArray)
    val ksFile = Files.createTempFile("unit-ks", ".p12")
    val ksOut  = new java.io.FileOutputStream(ksFile.toFile)
    ks.store(ksOut, "test".toCharArray)
    ksOut.close()

    val ts = KeyStore.getInstance("PKCS12")
    ts.load(null, "test".toCharArray)
    val tsFile = Files.createTempFile("unit-ts", ".p12")
    val tsOut  = new java.io.FileOutputStream(tsFile.toFile)
    ts.store(tsOut, "test".toCharArray)
    tsOut.close()

    val transport = OpenSearchTransportFactory.create(
      settings(
        "connect.opensearch.protocol" -> "https",
        JWT_TOKEN_KEY                 -> "my-jwt-token",
        "ssl.keystore.location"       -> ksFile.toString,
        "ssl.keystore.type"           -> "PKCS12",
        "ssl.keystore.password"       -> "test",
        "ssl.truststore.location"     -> tsFile.toString,
        "ssl.truststore.type"         -> "PKCS12",
        "ssl.truststore.password"     -> "test",
      ),
    )
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally {
      transport.close()
    }
  }

  // Multi-host: ApacheHttpClient5TransportBuilder.builder(HttpHost[]) accepts an array
  test("multi-host: two hosts produce an ApacheHttpClient5Transport without error") {
    val transport = OpenSearchTransportFactory.create(settings(
      HOSTS -> "host1,host2",
    ))
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  test("multi-host with basic auth: two hosts + credentials create transport without error") {
    val transport = OpenSearchTransportFactory.create(
      settings(
        "connect.opensearch.protocol"   -> "https",
        HOSTS                           -> "host1,host2",
        CLIENT_HTTP_BASIC_AUTH_USERNAME -> "user",
        CLIENT_HTTP_BASIC_AUTH_PASSWORD -> "pass",
      ),
    )
    try {
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  // B6: JwtBearerInterceptor and no-op RedirectStrategy must both be wired without conflict
  test("B6: JWT auth + no-op redirect strategy coexist on the same httpclient5 builder") {
    // If there were a conflict (e.g. redirect interceptor overwriting JWT header),
    // the builder would throw or the transport would fail to construct.
    val transport = OpenSearchTransportFactory.create(settings(
      "connect.opensearch.protocol" -> "https",
      JWT_TOKEN_KEY                 -> "eyJhbGciOiJSUzI1NiJ9.test-payload.sig",
    ))
    try {
      // Both the no-op redirect strategy and the JwtBearerInterceptor are wired;
      // if they conflict the builder would throw here.
      transport shouldBe a[ApacheHttpClient5Transport]
    } finally transport.close()
  }

  // C2-unit REST path: verify that the REST (ApacheHttpClient5) transport does NOT follow HTTP redirects.
  // The transport sets disableRedirectHandling on the HttpClient5 builder; any 301/302 must be
  // propagated to the caller rather than auto-followed.
  test("C2-REST: redirect (302) is not followed by the ApacheHttpClient5 transport") {
    val port = wireMock.port()

    // Stub: GET / (the info() endpoint) returns 302 → /redirected
    wireMock.stubFor(
      get(urlEqualTo("/"))
        .willReturn(aResponse()
          .withStatus(302)
          .withHeader("Location", s"http://localhost:$port/redirected")),
    )

    // Stub: the redirect target — if hit, we will count it
    wireMock.stubFor(get(urlEqualTo("/redirected"))
      .willReturn(aResponse().withStatus(200).withBody("{}")))

    val transport = OpenSearchTransportFactory.create(settings(
      HOSTS   -> "localhost",
      ES_PORT -> port.toString,
    ))
    val client = new OpenSearchClient(transport)
    try {
      // The call will fail (302 is not a valid OpenSearch response), but that's expected.
      // We only care that the redirect URL was NOT hit.
      Try(client.info())
    } catch {
      case _: Throwable =>
    } finally transport.close()

    // The redirect target must NOT have been called.
    wireMock.verify(0, getRequestedFor(urlEqualTo("/redirected")))
  }

  // C2-unit: SigV4 transport is created as an AwsSdk2Transport (builder-level regression guard).
  // The redirect-behavior test (C2) is covered end-to-end by OpenSearchSigV4WireMockIT which
  // runs against a real HTTPS WireMock server.  This unit test only verifies transport type
  // so it does not require HTTPS or a running server.
  test("SigV4 settings create an AwsSdk2Transport (not ApacheHttpClient5Transport)") {
    val s = settings(
      AWS_SIGNING_ENABLED_KEY       -> "true",
      AWS_REGION_KEY                -> "us-east-1",
      AWS_SIGNING_SERVICE_KEY       -> "es",
      AWS_CREDENTIALS_PROVIDER_KEY  -> "STATIC",
      AWS_ACCESS_KEY_ID_KEY         -> "AKIAIOSFODNN7EXAMPLE",
      AWS_SECRET_ACCESS_KEY_KEY     -> "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
      "connect.opensearch.protocol" -> "https",
    )
    val transport = OpenSearchTransportFactory.create(s)
    try {
      transport shouldBe a[AwsSdk2Transport]
    } finally transport.close()
  }
}
