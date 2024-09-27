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
package io.lenses.streamreactor.connect.http.sink.client

import cats.effect.IO
import cats.effect.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import org.mockito.ArgumentMatchers.any
import org.scalatest.EitherValues
//import cats.implicits.catsSyntaxOptionId
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.BasicCredentials
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.matching.EqualToPattern
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import org.http4s.Method
import org.http4s.Request
import org.http4s.client.Client
import org.http4s.jdkhttpclient.JdkHttpClient
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AsyncFunSuiteLike
import org.scalatest.matchers.should.Matchers

class HttpRequestSenderIT
    extends AsyncIOSpec
    with AsyncFunSuiteLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockitoSugar
    with Matchers
    with EitherValues {

  private val Host = "localhost"

  private val wireMockServer = new WireMockServer()
  private val expectedUrl    = "/some/thing"

  private val sinkName = "mySinkName"

  override protected def beforeAll(): Unit = {
    wireMockServer.start()
    WireMock.configureFor(Host, wireMockServer.port())
  }

  override protected def afterAll(): Unit = wireMockServer.stop()

  override protected def beforeEach(): Unit = wireMockServer.resetRequests()

  private val HttpResponseBody = "Hello world!"
  test("should send a PUT request by default") {

    stubFor(put(urlEqualTo(expectedUrl))
      .willReturn(aResponse.withHeader("Content-Type", "text/plain")
        .withBody(HttpResponseBody)))

    JdkHttpClient.simple[IO].use {
      client =>
        val requestSender = new NoAuthenticationHttpRequestSender(
          sinkName,
          Method.PUT,
          client,
        )
        val processedTemplate = ProcessedTemplate(
          s"${wireMockServer.baseUrl()}$expectedUrl",
          "mycontent",
          Seq(
            "X-Awesome-Header" -> "stream-reactor",
            "Content-Type"     -> "application/xml",
          ),
        )
        requestSender.sendHttpRequest(processedTemplate).asserting {
          response =>
            WireMock.verify(
              putRequestedFor(urlEqualTo(expectedUrl))
                .withHeader("X-Awesome-Header", equalTo("stream-reactor"))
                .withHeader("Content-Type", equalTo("application/xml"))
                .withRequestBody(new EqualToPattern("mycontent")),
            )
            response.value should be(HttpResponseSuccess(200, HttpResponseBody))
        }
    }

  }

  test("should send a POST request with basic auth") {

    stubFor(
      post(urlEqualTo(expectedUrl))
        .withBasicAuth("myUser", "myPassword")
        .willReturn(aResponse.withHeader("Content-Type", "text/plain")
          .withBody(HttpResponseBody)),
    )

    JdkHttpClient.simple[IO].use {
      client =>
        val requestSender = new BasicAuthenticationHttpRequestSender(
          sinkName,
          Method.POST,
          client,
          "myUser",
          "myPassword",
        )
        val processedTemplate = ProcessedTemplate(
          s"${wireMockServer.baseUrl()}$expectedUrl",
          "mycontent",
          Seq("X-Awesome-Header" -> "stream-reactor"),
        )
        requestSender.sendHttpRequest(processedTemplate).asserting {
          response =>
            WireMock.verify(
              postRequestedFor(urlEqualTo(expectedUrl))
                .withHeader("X-Awesome-Header", equalTo("stream-reactor"))
                .withBasicAuth(new BasicCredentials("myUser", "myPassword"))
                .withRequestBody(new EqualToPattern("mycontent")),
            )
            response.value should be(HttpResponseSuccess(200, HttpResponseBody))
        }
    }
  }

  test("should error when client is thoroughly broken") {

    val expectedException = new IllegalArgumentException("No fun allowed today")
    val mockClient        = mock[Client[IO]]
    when(mockClient.run(any[Request[IO]])).thenReturn(
      Resource.eval(IO.raiseError(expectedException)),
    )

    val requestSender = new NoAuthenticationHttpRequestSender(
      sinkName,
      Method.PUT,
      mockClient,
    )
    val processedTemplate = ProcessedTemplate(
      s"${wireMockServer.baseUrl()}$expectedUrl",
      "mycontent",
      Seq("X-Awesome-Header" -> "stream-reactor"),
    )
    requestSender.sendHttpRequest(processedTemplate).asserting {
      response =>
        response.left.value should be(HttpResponseFailure("No fun allowed today", expectedException.some, none, none))
    }

  }

}
