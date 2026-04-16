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
package io.lenses.streamreactor.connect.azure.cosmosdb
import io.lenses.streamreactor.connect.azure.cosmosdb.config.CosmosDbSinkSettings
import org.apache.kafka.connect.errors.ConnectException
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.scalatest.EitherValues
import io.lenses.streamreactor.connect.azure.cosmosdb.util.CosmosEmulatorTestBase

import java.net.MalformedURLException

class CosmosClientProviderTest
    extends AnyFunSuiteLike
    with MockitoSugar
    with Matchers
    with EitherValues
    with CosmosEmulatorTestBase
    with BeforeAndAfterAll {

  override def beforeAll(): Unit = {

    super.beforeAll()

    ()
  }

  // tests a real connection to the Cosmos DB emulator
  test("return CosmosClient with default gateway config when valid settings and no proxy") {
    val settings = mock[CosmosDbSinkSettings]
    when(settings.proxy).thenReturn(None)
    when(settings.endpoint).thenReturn(cosmosEmulatorContainer.getEmulatorEndpoint)
    when(settings.masterKey).thenReturn(cosmosEmulatorContainer.getEmulatorKey)
    when(settings.consistency).thenReturn(com.azure.cosmos.ConsistencyLevel.EVENTUAL)

    val client = CosmosClientProvider.get(settings).value

    client should not be null
    verify(settings, times(1)).endpoint
    verify(settings, times(1)).masterKey
    verify(settings, times(1)).consistency
  }

  // sets up a connection destined to fail due to a proxy configuration
  // this shows that the CosmosClientProvider can handle proxy settings
  test("return CosmosClient with custom proxy configuration when valid settings and proxy") {
    val settings = mock[CosmosDbSinkSettings]
    when(settings.proxy).thenReturn(Some(s"http://localhost:12345"))
    when(settings.endpoint).thenReturn(cosmosEmulatorContainer.getEmulatorEndpoint)
    when(settings.masterKey).thenReturn("masterKey")
    when(settings.consistency).thenReturn(com.azure.cosmos.ConsistencyLevel.EVENTUAL)

    CosmosClientProvider.get(settings).left.value match {
      case ex: ConnectException =>
        ex.getMessage should startWith("Exception while creating CosmosClient")
        verify(settings, times(1)).proxy
        verify(settings, times(1)).endpoint
        verify(settings, times(1)).masterKey
        verify(settings, times(1)).consistency
    }
  }

  test("throw exception when endpoint is missing") {
    val settings = mock[CosmosDbSinkSettings]
    when(settings.proxy).thenReturn(None)
    when(settings.endpoint).thenReturn(null)
    when(settings.masterKey).thenReturn("masterKey")
    when(settings.consistency).thenReturn(com.azure.cosmos.ConsistencyLevel.EVENTUAL)

    CosmosClientProvider.get(settings).left.value match {
      case ex: ConnectException =>
        ex.getMessage should startWith("Null value found in CosmosClient settings")
        verify(settings, times(1)).endpoint
    }
  }

  test("return HTTP ProxyOptions for http protocol") {
    val proxy  = "http://proxyhost:8080"
    val result = CosmosClientProvider.convertProxy(proxy).value
    result.getType shouldBe com.azure.core.http.ProxyOptions.Type.HTTP
    result.getAddress.getHostName shouldBe "proxyhost"
    result.getAddress.getPort shouldBe 8080
  }

  test("return SOCKS4 ProxyOptions for socks4 protocol") {
    val proxy  = "socks4://proxyhost:1080"
    val result = CosmosClientProvider.convertProxy(proxy).value
    result.getType shouldBe com.azure.core.http.ProxyOptions.Type.SOCKS4
    result.getAddress.getHostName shouldBe "proxyhost"
    result.getAddress.getPort shouldBe 1080
  }

  test("return SOCKS5 ProxyOptions for socks5 protocol") {
    val proxy  = "socks5://proxyhost:1080"
    val result = CosmosClientProvider.convertProxy(proxy).value
    result.getType shouldBe com.azure.core.http.ProxyOptions.Type.SOCKS5
    result.getAddress.getHostName shouldBe "proxyhost"
    result.getAddress.getPort shouldBe 1080
  }

  test("handle uppercase protocol correctly") {
    val proxy  = "HTTP://proxyhost:8080"
    val result = CosmosClientProvider.convertProxy(proxy).value
    result.getType shouldBe com.azure.core.http.ProxyOptions.Type.HTTP
    result.getAddress.getHostName shouldBe "proxyhost"
    result.getAddress.getPort shouldBe 8080
  }

  test("throw MalformedURLException for invalid proxy URL") {
    val proxy = "invalid_proxy_url"
    val ex    = CosmosClientProvider.convertProxy(proxy).left.value
    ex shouldBe a[MalformedURLException]
    ex.getMessage should startWith("Proxy protocol has not been specified")
  }

  test("throw MalformedURLException when port is missing") {
    val proxy = "http://proxyhost"
    val ex    = CosmosClientProvider.convertProxy(proxy).left.value
    ex shouldBe a[MalformedURLException]
    ex.getMessage should startWith("Proxy port has not been specified")
  }

  test("throw MalformedURLException for unsupported proxy protocol") {
    val proxy = "ftp://proxyhost:2121"
    val ex    = CosmosClientProvider.convertProxy(proxy).left.value
    ex shouldBe a[MalformedURLException]
    ex.getMessage should startWith("Unsupported proxy protocol specified")
  }
}
