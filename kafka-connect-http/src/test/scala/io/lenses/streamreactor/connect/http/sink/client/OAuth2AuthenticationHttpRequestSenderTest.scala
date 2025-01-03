/*
 * Copyright 2017-2025 Lenses.io Ltd
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
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.catsSyntaxOptionId
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessToken
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessTokenProvider
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import org.http4s._
import org.http4s.client.Client
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.EitherValues
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString

import java.time.Instant

class OAuth2AuthenticationHttpRequestSenderTest
    extends AsyncFreeSpec
    with AsyncIOSpec
    with Matchers
    with MockitoSugar
    with EitherValues {

  private val StatusCodeOK      = 200
  private val ResponseContentOK = "OK"
  "OAuth2AuthenticationHttpRequestSender" - {
    "should attach the OAuth2 token header to the request" in {
      val sinkName = "sink"
      val method   = Method.POST
      val client: Client[IO] = mock[Client[IO]]
      when(client.run(any[Request[IO]])).thenReturn(
        Resource.pure(Response[IO](status = Status.Ok).withEntity(ResponseContentOK)),
      )

      val token = "token"
      val tokenProvider = new AccessTokenProvider[IO] {
        override def requestToken(): IO[AccessToken] =
          IO.pure(AccessToken(token, "type", Instant.now().plusSeconds(10)))
      }

      val sender = new OAuth2AuthenticationHttpRequestSender(sinkName, method, client, tokenProvider)
      val template = ProcessedTemplate(
        "http://localhost:8080",
        "content",
        List("header" -> "value"),
      )

      val requestCaptor: ArgumentCaptor[Request[IO]] = ArgumentCaptor.forClass(classOf[Request[IO]])

      sender.sendHttpRequest(template).asserting {
        response =>
          verify(client).run(requestCaptor.capture())

          val capturedRequest: Request[IO] = requestCaptor.getValue

          // Assertions
          capturedRequest.method shouldBe method
          capturedRequest.uri shouldBe Uri.unsafeFromString("http://localhost:8080")
          capturedRequest.headers.headers should contain(Header.Raw(CIString("header"), "value"))
          capturedRequest.headers.headers should contain(Header.Raw(CIString("Authorization"), s"Bearer $token"))

          response.value should be(HttpResponseSuccess(StatusCodeOK, ResponseContentOK.some))
      }
    }
  }
}
