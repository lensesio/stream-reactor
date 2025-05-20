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
package io.lenses.streamreactor.connect.http.sink.client.oauth2.cache

import cats.effect.Clock
import cats.effect.Concurrent
import cats.effect.Ref
import cats.implicits._
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessToken
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessTokenProvider

import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class CachedAccessTokenProvider[F[_]: Concurrent: Clock](
  delegate:              AccessTokenProvider[F],
  cache:                 Ref[F, Option[AccessToken]],
  tokenExpirationBuffer: FiniteDuration = 1.minute, // Buffer before token expiration to request a new one
) extends AccessTokenProvider[F] {

  override def requestToken(): F[AccessToken] =
    for {
      now <- Clock[F].realTime
      cachedAccessToken: Option[AccessToken] <- cache.get
      token <- cachedAccessToken match {
        case Some(cachedToken) if !isTokenExpired(cachedToken, now) => cachedToken.pure[F]
        case _ => delegate.requestToken().flatMap { accessToken =>
            cache.set(Some(accessToken)).as(accessToken)
          }
      }
    } yield token

  private def isTokenExpired(accessToken: AccessToken, now: FiniteDuration): Boolean =
    accessToken.expires.toEpochMilli - now.toMillis < tokenExpirationBuffer.toMillis

}
