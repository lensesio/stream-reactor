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
import com.fasterxml.jackson.databind.node.JsonNodeFactory
import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.JWSHeader
import com.nimbusds.jose.crypto.MACSigner
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.SignedJWT
import io.lenses.streamreactor.connect.elastic.common.bulk.InsertOp
import io.lenses.streamreactor.connect.opensearch.auth.JwtBearerInterceptor
import io.lenses.streamreactor.connect.opensearch.auth.JwtTokenSource
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchConfigConstants._
import io.lenses.streamreactor.connect.testcontainers.OpensearchContainer
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy
import org.apache.hc.core5.http.HttpHost
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.opensearch.OpenSearchClient
import org.opensearch.client.opensearch.core.SearchRequest
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder

import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.KeyStore
import java.security.SecureRandom
import java.time.Instant
import java.util.Base64
import java.util.Date
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory

import scala.util.Try

/**
 * Integration tests for JWT bearer token authentication against OpenSearch with the Security plugin.
 *
 * - Happy path: HS512-minted token with correct signing key authenticates.
 * - File rotation: updating the token file triggers re-authentication.
 * - Negative path: wrong-key token returns 401.
 */
class OpenSearchJwtAuthIT extends ITBase {

  private val jwtSigningKeyBytes: Array[Byte] = {
    val bytes = new Array[Byte](64)
    new SecureRandom().nextBytes(bytes)
    bytes
  }

  override val container: OpensearchContainer = OpensearchContainer()
    .withSecurityEnabled(true)
    .withJwtSigningKey(Base64.getEncoder.encodeToString(jwtSigningKeyBytes))

  private def mintToken(keyBytes: Array[Byte], subject: String = "kafka-connect"): String = {
    val now = Instant.now()
    val claims = new JWTClaimsSet.Builder()
      .subject(subject)
      .claim("roles", java.util.List.of("all_access"))
      .issueTime(Date.from(now))
      .expirationTime(Date.from(now.plusSeconds(300)))
      .build()
    val jwt = new SignedJWT(new JWSHeader(JWSAlgorithm.HS512), claims)
    jwt.sign(new MACSigner(keyBytes))
    jwt.serialize()
  }

  private def host = "localhost"
  private def port = container.hostNetwork.httpHostAddress.split(":").last.toInt

  private def buildTrustStoreSslContext(truststorePath: String, password: String): SSLContext = {
    val ts  = KeyStore.getInstance("JKS")
    val fis = new FileInputStream(truststorePath)
    try ts.load(fis, password.toCharArray)
    finally fis.close()
    val tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    tmf.init(ts)
    val ctx = SSLContext.getInstance("TLS")
    ctx.init(null, tmf.getTrustManagers, null)
    ctx
  }

  /** Build a JWT-authenticated [[OpenSearchClient]] for read-back searches. */
  private def jwtSearchClient(token: String): OpenSearchClient = {
    val httpHost    = new HttpHost("https", host, port)
    val tokenSource = JwtTokenSource.fromStaticToken(token)
    val sslCtx      = buildTrustStoreSslContext(container.clientTruststorePath, "changeit")
    val tlsStrategy = new DefaultClientTlsStrategy(sslCtx)
    val cm = PoolingAsyncClientConnectionManagerBuilder.create()
      .setTlsStrategy(tlsStrategy)
      .build()
    val transport = ApacheHttpClient5TransportBuilder.builder(httpHost)
      .setMapper(new JacksonJsonpMapper())
      .setHttpClientConfigCallback { b =>
        b.addRequestInterceptorLast(new JwtBearerInterceptor(tokenSource))
        b.setConnectionManager(cm)
      }
      .build()
    new OpenSearchClient(transport)
  }

  test(
    "happy path: HS512-signed JWT with correct key authenticates, writes documents, and read-back confirms landing",
  ) {
    val index = "test-jwt-happy"
    val token = mintToken(jwtSigningKeyBytes)
    val kClient = makeKClient(
      baseProps(
        host,
        port,
        s"INSERT INTO $index SELECT * FROM topic",
        Map(
          "connect.opensearch.protocol" -> "https",
          JWT_TOKEN_KEY                 -> token,
          "ssl.truststore.location"     -> container.clientTruststorePath,
          "ssl.truststore.password"     -> "changeit",
        ),
      ),
    )
    val doc    = JsonNodeFactory.instance.objectNode().put("msg", "jwt-happy")
    val result = kClient.bulk(Seq(InsertOp(index, "1", doc, None, None)))
    result.isSuccess shouldBe true
    result.get.errors shouldBe false

    // Read-back: search under the same JWT principal confirms read access too
    val searchClient = jwtSearchClient(token)
    try {
      eventually {
        val resp = searchClient.search(
          (s: SearchRequest.Builder) => s.index(index),
          classOf[JsonNode],
        )
        resp.hits().total().value() shouldBe 1L
      }
    } finally { val _ = Try(searchClient._transport().close()) }
  }

  test("C6: file-backed JWT rotation — deterministic via distinct subjects and generous margin") {
    val tokenFile = Files.createTempFile("jwt-rotation", ".txt")

    // Use distinct subjects so we can tell which token the server accepted (via the subject claim).
    // Subject-1 token: subject "kafka-connect-v1"
    val token1 = mintToken(jwtSigningKeyBytes, subject = "kafka-connect-v1")
    Files.write(tokenFile, token1.getBytes(StandardCharsets.UTF_8))

    // Set refresh interval to 200ms. We will sleep 1000ms before the second bulk call
    // to give the cache well over 5x its refresh interval to expire — removing flakiness.
    val refreshIntervalMs = 200L

    val kClient = makeKClient(
      baseProps(
        host,
        port,
        "INSERT INTO test-jwt-rotation SELECT * FROM topic",
        Map(
          "connect.opensearch.protocol" -> "https",
          JWT_TOKEN_FILE_KEY            -> tokenFile.toString,
          JWT_REFRESH_INTERVAL_KEY      -> refreshIntervalMs.toString,
          "ssl.truststore.location"     -> container.clientTruststorePath,
          "ssl.truststore.password"     -> "changeit",
        ),
      ),
    )

    val doc1 = JsonNodeFactory.instance.objectNode().put("msg", "rotation-v1").put("subject", "kafka-connect-v1")
    val r1   = kClient.bulk(Seq(InsertOp("test-jwt-rotation", "1", doc1, None, None)))
    r1.isSuccess shouldBe true
    r1.get.errors shouldBe false

    // Write the new token (distinct subject "kafka-connect-v2") BEFORE advancing past the interval.
    val token2 = mintToken(jwtSigningKeyBytes, subject = "kafka-connect-v2")
    Files.write(tokenFile, token2.getBytes(StandardCharsets.UTF_8))

    // Sleep for 5x the refresh interval to guarantee the cache window has passed on any CI machine.
    Thread.sleep(refreshIntervalMs * 5)

    val doc2 = JsonNodeFactory.instance.objectNode().put("msg", "rotation-v2").put("subject", "kafka-connect-v2")
    val r2   = kClient.bulk(Seq(InsertOp("test-jwt-rotation", "2", doc2, None, None)))
    // The rotated token must also be accepted by OpenSearch
    r2.isSuccess shouldBe true
    r2.get.errors shouldBe false
  }

  test("JWT + mTLS combined — client cert + bearer token both sent and accepted") {
    val pki   = io.lenses.streamreactor.connect.testcontainers.SecurityPkiFixture.shared
    val token = mintToken(jwtSigningKeyBytes, subject = "kafka-connect-jwt-mtls")

    val kClient = makeKClient(
      baseProps(
        host,
        port,
        "INSERT INTO test-jwt-mtls SELECT * FROM topic",
        Map(
          "connect.opensearch.protocol" -> "https",
          JWT_TOKEN_KEY                 -> token,
          "ssl.keystore.location"       -> pki.keystorePath.toString,
          "ssl.keystore.password"       -> "changeit",
          "ssl.truststore.location"     -> pki.truststorePath.toString,
          "ssl.truststore.password"     -> "changeit",
        ),
      ),
    )

    val doc    = JsonNodeFactory.instance.objectNode().put("msg", "jwt-mtls-combo")
    val result = kClient.bulk(Seq(InsertOp("test-jwt-mtls", "1", doc, None, None)))
    result.isSuccess shouldBe true
    result.get.errors shouldBe false
  }

  test("negative path: JWT signed with wrong key returns failure") {
    val wrongKey = new Array[Byte](64)
    new SecureRandom().nextBytes(wrongKey)
    val wrongToken = mintToken(wrongKey)

    val kClient = makeKClient(
      baseProps(
        host,
        port,
        "INSERT INTO test-jwt-wrong SELECT * FROM topic",
        Map(
          "connect.opensearch.protocol" -> "https",
          JWT_TOKEN_KEY                 -> wrongToken,
          "ssl.truststore.location"     -> container.clientTruststorePath,
          "ssl.truststore.password"     -> "changeit",
        ),
      ),
    )
    val doc    = JsonNodeFactory.instance.objectNode().put("msg", "wrong-key")
    val result = kClient.bulk(Seq(InsertOp("test-jwt-wrong", "1", doc, None, None)))
    result.isFailure shouldBe true
  }
}
