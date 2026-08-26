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

import com.typesafe.scalalogging.StrictLogging
import io.lenses.streamreactor.common.security.StoresInfo
import io.lenses.streamreactor.common.util.EitherUtils.unpackOrThrow
import io.lenses.streamreactor.connect.opensearch.auth.AwsCredentialsProviderFactory
import io.lenses.streamreactor.connect.opensearch.auth.JwtBearerInterceptor
import io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings
import org.apache.hc.client5.http.auth.AuthScope
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder
import org.apache.hc.client5.http.protocol.RedirectStrategy
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy
import org.apache.hc.core5.http.HttpHost
import org.apache.hc.core5.http.HttpRequest
import org.apache.hc.core5.http.HttpResponse
import org.apache.hc.core5.http.protocol.HttpContext
import org.apache.hc.core5.util.Timeout
import org.opensearch.client.json.jackson.JacksonJsonpMapper
import org.opensearch.client.transport.OpenSearchTransport
import org.opensearch.client.transport.aws.AwsSdk2Transport
import org.opensearch.client.transport.aws.AwsSdk2TransportOptions
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder
import software.amazon.awssdk.http.apache.ApacheHttpClient
import software.amazon.awssdk.regions.Region

import java.io.FileInputStream
import java.net.URI
import java.security.KeyStore
import java.time.Duration
import javax.net.ssl.KeyManagerFactory
import javax.net.ssl.TrustManagerFactory
import scala.jdk.OptionConverters._ // provides .toScala for java.util.Optional (cyclops Option → .toOptional() first)

/**
 * Factory for [[OpenSearchTransport]] instances.
 *
 * Selects between the httpclient5 REST transport and the AWS SigV4 transport based on settings.
 * `tablePrefix` is not supported on any transport and is rejected by
 * [[io.lenses.streamreactor.connect.opensearch.config.OpenSearchSettings]].
 *
 * Hardening applied on all transports:
 *  - HTTP redirect following is disabled on the REST path via a no-op [[RedirectStrategy]].
 *  - The AWS SDK [[ApacheHttpClient]] never follows redirects by design (max-redirects = 0 via SDK defaults).
 *  - [[JacksonJsonpMapper]] is set explicitly on both transports so JSON mapping is deterministic.
 *  - `connect.opensearch.write.timeout` is wired as HTTP response + connect timeout on the REST path,
 *    and as socket + connection timeout on the SigV4 path.
 *  - mTLS material from [[StoresInfo]] is propagated to both transports when configured.
 *
 * == Intentional HC4 / HC5 Dual Stack ==
 *
 * The REST path uses Apache HttpClient 5 (HC5) via [[ApacheHttpClient5TransportBuilder]].
 * The SigV4 path uses the AWS SDK's [[ApacheHttpClient]] which internally wraps Apache HttpClient 4 (HC4).
 * Both HTTP client libraries are therefore on the classpath simultaneously.
 *
 * This is an **accepted and intentional** design decision:
 *  - HC5 is the modern default required by `opensearch-java` for the REST transport.
 *  - The AWS SDK v2 bundles and uses HC4 internally; migrating SigV4 to `url-connection-client`
 *    or `aws-crt-client` would require additional testing of authentication flows and is deferred.
 *  - The two clients are completely isolated: the REST path never uses HC4, the SigV4 path never uses HC5.
 *  - Connection-pool tuning knobs (`connect.opensearch.max.connections.*`) apply to the HC5 REST path only.
 *
 * To migrate away from the dual stack, replace [[ApacheHttpClient]] in [[createSigV4Transport]] with:
 *  - `software.amazon.awssdk:url-connection-client` (JDK [[java.net.HttpURLConnection]], no extra dep)
 *  - `software.amazon.awssdk:aws-crt-client` (HTTP/2-native, requires `aws-crt` native dep)
 */
object OpenSearchTransportFactory extends StrictLogging {

  def create(settings: OpenSearchSettings): OpenSearchTransport =
    if (settings.awsSigningEnabled) {
      createSigV4Transport(settings)
    } else {
      createHttpClient5Transport(settings)
    }

  private def createSigV4Transport(settings: OpenSearchSettings): OpenSearchTransport = {
    if (settings.tablePrefix.nonEmpty) {
      throw new IllegalStateException(
        s"OpenSearchTransportFactory: connect.opensearch.tableprefix is not supported " +
          s"(tablePrefix=${settings.tablePrefix.getOrElse("")}); " +
          s"upstream config validation should have rejected this — file a bug if you see this exception",
      )
    }

    val writeTimeoutMs = settings.common.writeTimeout.toLong

    val sdkHttpClientBuilder = ApacheHttpClient.builder()
      .socketTimeout(Duration.ofMillis(writeTimeoutMs))
      .connectionTimeout(Duration.ofMillis(writeTimeoutMs))

    // Propagate StoresInfo TLS material into the SigV4 transport when configured.
    wireSigV4TlsMaterial(settings.common.storesInfo, sdkHttpClientBuilder)

    val httpClient = sdkHttpClientBuilder.build()

    val options = AwsSdk2TransportOptions.builder()
      .setMapper(new JacksonJsonpMapper())
      .setCredentials(AwsCredentialsProviderFactory.create(settings))
      .build()

    new AwsSdk2Transport(
      httpClient,
      settings.sigV4Host,
      settings.awsSigningService,
      Region.of(settings.awsRegion),
      options,
    )
  }

  /**
   * Wires StoresInfo TLS material (keystore/truststore) into the AWS SDK [[ApacheHttpClient.Builder]]
   * for the SigV4 transport path.
   *
   * We cannot extract `KeyManager[]` / `TrustManager[]` from a post-init `SSLContext`, so instead we
   * load the JKS / PKCS12 stores directly from the `StoresInfo` component fields and build fresh
   * `KeyManagerFactory` / `TrustManagerFactory` objects that we pass to the AWS SDK builder.
   */
  private[opensearch] def wireSigV4TlsMaterial(
    storesInfo: StoresInfo,
    builder:    ApacheHttpClient.Builder,
  ): Unit = {
    // cyclops Option → java.util.Optional → scala.Option
    storesInfo.getMaybeKeyStore.toOptional().toScala.foreach { ks =>
      val keyStore = KeyStore.getInstance(ks.getStoreType.toString)
      val password = ks.getStorePassword
      val is       = new FileInputStream(ks.getStorePath.toFile)
      try keyStore.load(is, Option(password).map(_.toCharArray).orNull)
      finally is.close()
      val kmf = KeyManagerFactory.getInstance(
        ks.getManagerAlgorithm.toOptional().toScala.getOrElse(KeyManagerFactory.getDefaultAlgorithm),
      )
      kmf.init(keyStore, Option(password).map(_.toCharArray).orNull)
      builder.tlsKeyManagersProvider(() => kmf.getKeyManagers)
      logger.debug("SigV4 transport: wired key manager from keystore {}", ks.getStorePath)
    }

    storesInfo.getMaybeTrustStore.toOptional().toScala.foreach { ts =>
      val trustStore = KeyStore.getInstance(ts.getStoreType.toString)
      val password   = ts.getStorePassword.toOptional().toScala
      val is         = new FileInputStream(ts.getStorePath.toFile)
      try trustStore.load(is, password.map(_.toCharArray).orNull)
      finally is.close()
      val tmf = TrustManagerFactory.getInstance(
        ts.getManagerAlgorithm.toOptional().toScala.getOrElse(TrustManagerFactory.getDefaultAlgorithm),
      )
      tmf.init(trustStore)
      builder.tlsTrustManagersProvider(() => tmf.getTrustManagers)
      logger.debug("SigV4 transport: wired trust manager from truststore {}", ts.getStorePath)
    }
  }

  private def createHttpClient5Transport(settings: OpenSearchSettings): OpenSearchTransport = {
    val httpHosts: Array[HttpHost] =
      settings.hosts.map(h => new HttpHost(settings.protocol, h, settings.port)).toArray
    val writeTimeoutMs = settings.common.writeTimeout.toLong

    val builder = ApacheHttpClient5TransportBuilder.builder(httpHosts: _*)
    // Explicit JacksonJsonpMapper — deterministic JSON mapping for both happy and error paths.
    builder.setMapper(new JacksonJsonpMapper())
    builder.setHttpClientConfigCallback { httpClientBuilder =>
      // (1) No-op redirect strategy — prevents credential replay via 30x redirects.
      //     Applied unconditionally so future config changes (adding auth) do not silently
      //     become a redirect-following path.
      httpClientBuilder.setRedirectStrategy(
        new RedirectStrategy {
          override def isRedirected(request:   HttpRequest, response: HttpResponse, context: HttpContext): Boolean = false
          override def getLocationURI(request: HttpRequest, response: HttpResponse, context: HttpContext): URI     = null
        },
      )

      // (2) HTTP-layer timeouts. `connect.opensearch.write.timeout` is milliseconds
      //     (default 300000 = 5 minutes), matching ES6/ES7.
      val requestConfig = RequestConfig.custom()
        .setConnectionRequestTimeout(Timeout.ofMilliseconds(writeTimeoutMs))
        .setResponseTimeout(Timeout.ofMilliseconds(writeTimeoutMs))
        .build()
      httpClientBuilder.setDefaultRequestConfig(requestConfig)

      // (3) Basic auth — only wire when BOTH username and password are non-empty.
      //     AuthScope uses null/−1 wildcards to match any host, mirroring ES7's AuthScope.ANY.
      if (settings.common.httpBasicAuthUsername.nonEmpty && settings.common.httpBasicAuthPassword.nonEmpty) {
        val credentialsProvider = new BasicCredentialsProvider()
        credentialsProvider.setCredentials(
          new AuthScope(null, null, -1, null, null),
          new UsernamePasswordCredentials(
            settings.common.httpBasicAuthUsername,
            settings.common.httpBasicAuthPassword.toCharArray,
          ),
        )
        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
      } else if (settings.jwtTokenSource.isDefined) {
        // (4) JWT bearer token interceptor (mutually exclusive with basic auth per upstream validation).
        httpClientBuilder.addRequestInterceptorLast(new JwtBearerInterceptor(settings.jwtTokenSource.get))
      }

      // (5) TLS / mTLS + connection-pool sizing.
      //     Applied AFTER the auth wiring so that mTLS + basic-auth and mTLS + JWT both work.
      //     Connection-pool limits come from connect.opensearch.max.connections.per.route /
      //     connect.opensearch.max.connections.total (default 5 per-route; set higher for high-throughput deployments).
      val sslContextOpt = unpackOrThrow(settings.common.storesInfo.toSslContext)
      val cmBuilder = PoolingAsyncClientConnectionManagerBuilder.create()
        .setMaxConnPerRoute(settings.maxConnectionsPerRoute)
        .setMaxConnTotal(settings.maxConnectionsTotal)
      Option(sslContextOpt.orElse(null)).foreach { sslContext =>
        cmBuilder.setTlsStrategy(new DefaultClientTlsStrategy(sslContext))
      }
      httpClientBuilder.setConnectionManager(cmBuilder.build())

      httpClientBuilder
    }

    builder.build()
  }
}
