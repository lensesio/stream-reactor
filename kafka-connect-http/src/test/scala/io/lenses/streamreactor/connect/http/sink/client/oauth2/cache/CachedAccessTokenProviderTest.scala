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
package io.lenses.streamreactor.connect.http.sink.client.oauth2.cache

import cats.effect.Clock
import cats.effect.IO
import cats.effect.Ref
import cats.effect.kernel.Async
import cats.effect.testing.scalatest.AsyncIOSpec
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessToken
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessTokenProvider
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.concurrent.duration._

class CachedAccessTokenProviderTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  class MockAccessTokenProvider(clock: Clock[IO], val callCount: Ref[IO, Int]) extends AccessTokenProvider[IO] {

    override def requestToken(): IO[AccessToken] =
      for {
        count       <- callCount.updateAndGet(_ + 1)
        currentTime <- clock.monotonic
      } yield AccessToken(
        s"token$count",
        "Bearer",
        Instant.ofEpochMilli(currentTime.toMillis + 3600000),
      )
  }

  def mockClock(currentTime: Long): Clock[IO] = new Clock[IO] {
    def applicative: Async[IO] = IO.asyncForIO

    def monotonic: IO[FiniteDuration] = IO.pure(currentTime.millis)

    def realTime: IO[FiniteDuration] = IO.pure(currentTime.millis)
  }

  def createProvider(
    initialToken:     Option[AccessToken] = None,
    currentTime:      Long                = System.currentTimeMillis(),
    expirationBuffer: FiniteDuration      = 1.minute,
  ): IO[(CachedAccessTokenProvider[IO], MockAccessTokenProvider)] =
    for {
      clock       <- IO(mockClock(currentTime))
      callCount   <- Ref.of[IO, Int](0)
      mockProvider = new MockAccessTokenProvider(clock, callCount)
      cache       <- Ref.of[IO, Option[AccessToken]](initialToken)
      provider = new CachedAccessTokenProvider[IO](
        mockProvider,
        cache,
        tokenExpirationBuffer = expirationBuffer,
      )(IO.asyncForIO, clock)
    } yield (provider, mockProvider)

  "CachedAccessTokenProvider" - {

    "should return cached token if it is not expired" in {
      val initialToken =
        AccessToken("cachedToken", "Bearer", Instant.ofEpochMilli(System.currentTimeMillis() + 3600000))
      createProvider(Some(initialToken)).flatMap {
        case (provider, mockProvider) =>
          for {
            token <- provider.requestToken()
            count <- mockProvider.callCount.get
          } yield {
            token shouldBe initialToken
            count shouldBe 0
          }
      }
    }

    "should request a new token if cache is empty" in {
      createProvider().flatMap {
        case (provider, mockProvider) =>
          for {
            token <- provider.requestToken()
            count <- mockProvider.callCount.get
          } yield {
            token.value shouldBe "token1"
            count shouldBe 1
          }
      }
    }

    "should request a new token if cached token is expired" in {
      val expiredToken =
        AccessToken("expiredToken",
                    "Bearer",
                    Instant.ofEpochMilli(System.currentTimeMillis() - 1.minute.toMillis + 1.second.toMillis),
        )
      createProvider(Some(expiredToken)).flatMap {
        case (provider, mockProvider) =>
          for {
            token <- provider.requestToken()
            count <- mockProvider.callCount.get
          } yield {
            token.value shouldBe "token1"
            count shouldBe 1
          }
      }
    }

    "should request a new token if cached token is about to expire" in {
      val almostExpiredToken =
        AccessToken("almostExpired", "Bearer", Instant.ofEpochMilli(System.currentTimeMillis() + 30000))
      createProvider(Some(almostExpiredToken)).flatMap {
        case (provider, mockProvider) =>
          for {
            token <- provider.requestToken()
            count <- mockProvider.callCount.get
          } yield {
            token.value shouldBe "token1"
            count shouldBe 1
          }
      }
    }
  }
}
