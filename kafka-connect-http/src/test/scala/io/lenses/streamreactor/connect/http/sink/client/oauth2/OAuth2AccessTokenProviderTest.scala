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
package io.lenses.streamreactor.connect.http.sink.client.oauth2

import cats.effect._
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits._
import io.circe._
import org.http4s._
import org.http4s.circe._
import org.http4s.client._
import org.http4s.implicits._
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class OAuth2AccessTokenProviderTest extends AsyncWordSpec with Matchers with AsyncIOSpec with EitherValues {

  def mockClient(response: Response[IO]): Client[IO] = Client[IO] { _ =>
    Resource.pure(response)
  }

  "OAuth2AccessTokenProvider" should {

    "successfully retrieve and decode an access token" in {
      val jsonResponse = io.circe.parser.parse(
        """
          |{
          |  "access_token": "mockedAccessToken",
          |  "token_type": "Bearer",
          |  "expires_in": 3600
          |}
          |""".stripMargin,
      ).getOrElse(Json.Null)

      // Mocked HTTP response
      val response = Response[IO](
        status = Status.Ok,
      ).withEntity(jsonResponse)

      // Mock client returning the response
      val client = mockClient(response)

      // Instantiate the provider with the mock client
      val tokenProvider = new OAuth2AccessTokenProvider[IO](
        client       = client,
        tokenUri     = uri"https://example.com/token",
        clientId     = "client-id",
        clientSecret = "client-secret",
        scope        = None,
        headers      = List.empty,
      )

      // Test the requestToken method
      for {
        result <- tokenProvider.requestToken()
      } yield {
        result.value shouldBe "mockedAccessToken"
        result.`type` shouldBe "Bearer"
        result.expires.toEpochMilli should be > 0L
      }
    }

    "raise an error if the access token response cannot be decoded" in {
      // Mocked malformed JSON response (missing required fields)
      val malformedJsonResponse = Json.fromString("""
        {
          "wrong_field": "value"
        }
      """)

      val response = Response[IO](
        status = Status.Ok,
      ).withEntity(malformedJsonResponse)

      val client = mockClient(response)

      val tokenProvider = new OAuth2AccessTokenProvider[IO](
        client       = client,
        tokenUri     = uri"https://example.com/token",
        clientId     = "client-id",
        clientSecret = "client-secret",
        scope        = None,
        headers      = List.empty,
      )

      // Test the requestToken method expecting an exception
      tokenProvider.requestToken().attempt.map { result =>
        result shouldBe a[Left[_, _]]
      }
    }

    "handle non-200 status codes gracefully" in {
      val response = Response[IO](
        status = Status.BadRequest,
      ).withEntity("Bad Request")

      val client = mockClient(response)

      val tokenProvider = new OAuth2AccessTokenProvider[IO](
        client       = client,
        tokenUri     = uri"https://example.com/token",
        clientId     = "client-id",
        clientSecret = "client-secret",
        scope        = None,
        headers      = List.empty,
      )

      tokenProvider.requestToken().attempt.map { result =>
        result shouldBe a[Left[_, _]]
      }
    }
  }
}
