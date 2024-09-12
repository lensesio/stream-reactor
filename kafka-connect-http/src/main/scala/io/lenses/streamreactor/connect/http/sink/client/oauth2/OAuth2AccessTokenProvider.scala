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
package io.lenses.streamreactor.connect.http.sink.client.oauth2

import cats.effect.Concurrent
import cats.effect.kernel.Clock
import cats.implicits._
import io.circe.Decoder
import io.circe.HCursor
import io.circe.Json
import org.http4s._
import org.http4s.circe._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.headers.`Content-Type`

import java.time.Instant

case class AccessTokenResponse(token: String, tokenType: String, expiresIn: Option[Long]) {

  /**
    * Convert the response to an access token.
    * @param startTimestamp - The epoch time before the token request was issued. This is used to calculate the expiry time.
    * @return
    */
  def toAccessToken(startTimestamp: Long): AccessToken = {
    val instant = expiresIn.fold(Instant.MAX)(expiration => Instant.ofEpochMilli(startTimestamp + expiration * 1000))
    AccessToken(token, tokenType, instant)
  }
}

object AccessTokenResponse {
  // Define a decoder that uses configurable field names for the access token, token type, and expires in
  def decoder(
    accessTokenField: String,
    tokenTypeField:   String,
    expiresInField:   String,
  ): Decoder[AccessTokenResponse] = { (c: HCursor) =>
    for {
      token     <- c.downField(accessTokenField).as[String]
      tokenType <- c.downField(tokenTypeField).as[String]
      expiresIn <- c.downField(expiresInField).as[Option[Long]]
    } yield AccessTokenResponse(token, tokenType, expiresIn)
  }
}
class OAuth2AccessTokenProvider[F[_]: Concurrent: Clock](
  client:           Client[F],
  tokenUri:         Uri,
  clientId:         String,
  clientSecret:     String,
  scope:            Option[String],
  headers:          List[Header.Raw],
  accessTokenField: String = "access_token", // Configurable access token field name
  tokenTypeField:   String = "token_type", // Configurable token type field name
  expiresInField:   String = "expires_in", // Configurable expires in field name
) extends AccessTokenProvider[F] {

  private val body = {
    val basic = UrlForm(
      "grant_type"    -> "client_credentials",
      "client_id"     -> clientId,
      "client_secret" -> clientSecret,
    )
    scope.fold(basic)(s => basic + ("scope" -> s))
  }

  override def requestToken(): F[AccessToken] = {
    val request = Request[F](Method.POST, tokenUri)
      .withEntity(body)
      .putHeaders(headers)
      .putHeaders(
        `Content-Type`(MediaType.application.`x-www-form-urlencoded`),
        Authorization(BasicCredentials(clientId, clientSecret)),
      )

    // Send the request and decode the response with the configurable fields
    {
      for {
        now          <- Clock[F].realTime
        jsonResponse <- client.expect[Json](request)
        accessTokenE = jsonResponse.as(
          AccessTokenResponse.decoder(accessTokenField, tokenTypeField, expiresInField),
        )
        accessToken <- accessTokenE.leftMap(e => new Exception(s"Failed to decode access token: $e")).liftTo[F]
      } yield accessToken.toAccessToken(now.toMillis)
    }
  }
}
