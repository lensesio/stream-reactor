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
import cats.effect.Ref
import cats.implicits.catsSyntaxEitherId
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessToken
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessTokenProvider
import io.lenses.streamreactor.connect.http.sink.client.oauth2.OAuth2AccessTokenProvider
import io.lenses.streamreactor.connect.http.sink.client.oauth2.cache.CachedAccessTokenProvider
import io.lenses.streamreactor.connect.http.sink.metrics.HttpSinkMetricsMBean
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import org.http4s.EntityDecoder
import org.http4s._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.headers.`Content-Type`
import org.typelevel.ci.CIString

class NoAuthenticationHttpRequestSender(
  sinkName: String,
  method:   Method,
  client:   Client[IO],
  metrics:  HttpSinkMetricsMBean,
) extends HttpRequestSender(sinkName, method, client, metrics) {

  override protected def updateRequest(request: Request[IO]): IO[Request[IO]] = IO.pure(request)
}

class BasicAuthenticationHttpRequestSender(
  sinkName: String,
  method:   Method,
  client:   Client[IO],
  username: String,
  password: String,
  metrics:  HttpSinkMetricsMBean,
) extends HttpRequestSender(sinkName, method, client, metrics) {

  override protected def updateRequest(request: Request[IO]): IO[Request[IO]] =
    IO.pure(request.putHeaders(Authorization(BasicCredentials(username, password))))
}

class OAuth2AuthenticationHttpRequestSender(
  sinkName:      String,
  method:        Method,
  client:        Client[IO],
  tokenProvider: AccessTokenProvider[IO],
  metrics:       HttpSinkMetricsMBean,
) extends HttpRequestSender(sinkName, method, client, metrics) {

  override protected def updateRequest(request: Request[IO]): IO[Request[IO]] =
    for {
      token          <- tokenProvider.requestToken()
      requestWithAuth = request.putHeaders(Authorization(Credentials.Token(AuthScheme.Bearer, token.value)))
    } yield requestWithAuth
}

object HttpRequestSender {
  def apply(
    sinkName:       String,
    method:         Method,
    client:         Client[IO],
    authentication: Authentication,
    metrics:        HttpSinkMetricsMBean,
  ): IO[HttpRequestSender] =
    authentication match {
      case NoAuthentication => IO(new NoAuthenticationHttpRequestSender(sinkName, method, client, metrics))
      case BasicAuthentication(username, password) =>
        IO(new BasicAuthenticationHttpRequestSender(sinkName, method, client, username, password, metrics))
      case OAuth2Authentication(uri, clientId, clientSecret, tokenProperty, clientScope, clientHeaders) =>
        val rawHeaders = clientHeaders.map { case (k, v) => Header.Raw(CIString(k), v) }
        val tokenProvider =
          new OAuth2AccessTokenProvider(client, uri, clientId, clientSecret, clientScope, rawHeaders, tokenProperty)
        for {
          ref                <- Ref.of[IO, Option[AccessToken]](none)
          cachedTokenProvider = new CachedAccessTokenProvider(tokenProvider, ref)
        } yield new OAuth2AuthenticationHttpRequestSender(sinkName, method, client, cachedTokenProvider, metrics)
    }
}

abstract class HttpRequestSender(
  sinkName: String,
  method:   Method,
  client:   Client[IO],
  metrics:  HttpSinkMetricsMBean,
) extends LazyLogging {

  private case class HeaderInfo(contentType: Option[`Content-Type`], headers: Headers)

  private def buildHeaders(headers: Seq[(String, String)]): Either[Throwable, HeaderInfo] = {

    val (contentTypeHeaders, otherHeaders) = headers
      .partition(_._1.equalsIgnoreCase("Content-Type"))

    for {
      contentTypeSingle <- Either.cond(
        contentTypeHeaders.size <= 1,
        contentTypeHeaders.headOption.map {
          case (_, ct) => ct
        },
        new IllegalArgumentException("Excessive content types"),
      )
      contentTypeParsed: Option[`Content-Type`] <- contentTypeSingle.map(`Content-Type`.parse) match {
        case Some(Left(ex)) => Left(ex)
        case Some(Right(r: `Content-Type`)) => Right(Some(r))
        case None => Right(none)
      }
    } yield {
      HeaderInfo(
        contentTypeParsed,
        Headers(otherHeaders.map {
          case (name, value) =>
            Header.ToRaw.rawToRaw(new Header.Raw(CIString(name), value))
        }: _*),
      )
    }
  }

  protected def updateRequest(request: Request[IO]): IO[Request[IO]]

  /**
   * Sends an HTTP request based on the provided processed template.
   *
   * This method constructs an HTTP request using the provided template,
   * adds necessary headers and authentication, and sends the request using the client.
   * It processes the response and handles any errors that may occur during the request.
   *
   * @param processedTemplate the template containing the endpoint, content, and headers for the request
   * @return an `IO` containing either a `HttpResponseFailure` or a `HttpResponseSuccess`
   */
  def sendHttpRequest(
    processedTemplate: ProcessedTemplate,
  ): IO[Either[HttpResponseFailure, HttpResponseSuccess]] =
    for {
      tpl: ProcessedTemplate <- IO.pure(processedTemplate)

      uri <- IO.pure(Uri.unsafeFromString(processedTemplate.endpoint))
      _   <- IO.delay(logger.debug(s"[$sinkName] sending a http request to url $uri"))

      clientHeaders: HeaderInfo <- IO.fromEither(buildHeaders(tpl.headers))

      request <- IO {
        Request[IO](
          method  = method,
          uri     = uri,
          headers = clientHeaders.headers,
        )
          .withEntity(processedTemplate.content)
      }
      requestWithContentType = clientHeaders.contentType.fold(request)(request.withContentType)
      // Add authentication if present
      authenticatedRequest <- updateRequest(requestWithContentType)
      _                    <- IO.delay(logger.debug(s"[$sinkName] Auth: $authenticatedRequest"))
      startTime            <- IO(System.nanoTime())
      response             <- executeRequestAndHandleErrors(authenticatedRequest)
      //only record the request time if the request was successful
      _ <- response.fold(
        _ => IO.unit,
        _ => IO(metrics.recordRequestTime((System.nanoTime() - startTime) / 1000000)),
      )
      _ <- IO.delay(logger.trace(s"[$sinkName] Response: $response"))
    } yield response

  implicit val optionStringDecoder: EntityDecoder[IO, Option[String]] =
    EntityDecoder.decodeBy(MediaType.text.plain) { msg =>
      DecodeResult.success(msg.as[String].map {
        case body if body.nonEmpty => body.some
        case _                     => none
      })
    }

  /**
   * Executes the HTTP request and handles any errors that occur.
   *
   * This method sends the authenticated HTTP request using the provided client,
   * processes the response, and handles any errors that may occur during the request.
   *
   * @param authenticatedRequest the authenticated HTTP request to be sent
   * @return an `IO` containing either a `HttpResponseFailure` or a `HttpResponseSuccess`
   */
  private def executeRequestAndHandleErrors(
    authenticatedRequest: Request[IO],
  ): IO[Either[HttpResponseFailure, HttpResponseSuccess]] =
    client.run(authenticatedRequest).use { response =>
      response.as[Option[String]].map {
        body =>
          if (response.status.isSuccess) {
            metrics.increment2xxCount()
            HttpResponseSuccess(response.status.code, body).asRight[HttpResponseFailure]
          } else {
            if (response.status.code >= 400 && response.status.code < 500) {
              metrics.increment4xxCount()
            } else if (response.status.code >= 500 && response.status.code < 600) {
              metrics.increment5xxCount()
            } else {
              metrics.incrementOtherErrorsCount()
            }
            HttpResponseFailure(
              message         = "Request failed with error response",
              cause           = Option.empty,
              statusCode      = response.status.code.some,
              responseContent = body,
            ).asLeft[HttpResponseSuccess]
          }

      }
    }.handleErrorWith { error =>
      IO.pure(
        HttpResponseFailure(
          message         = error.getMessage,
          cause           = error.some,
          statusCode      = none,
          responseContent = none,
        ).asLeft[HttpResponseSuccess],
      )
    }

}
