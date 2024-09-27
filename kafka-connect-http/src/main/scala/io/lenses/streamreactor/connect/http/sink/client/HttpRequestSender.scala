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
import cats.effect.Ref
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.http.sink.client.oauth2.cache.CachedAccessTokenProvider
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessToken
import io.lenses.streamreactor.connect.http.sink.client.oauth2.AccessTokenProvider
import io.lenses.streamreactor.connect.http.sink.client.oauth2.OAuth2AccessTokenProvider
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import org.http4s._
import org.http4s.client.Client
import org.http4s.headers.Authorization
import org.http4s.headers.`Content-Type`
import org.typelevel.ci.CIString

class NoAuthenticationHttpRequestSender(
  sinkName: String,
  method:   Method,
  client:   Client[IO],
) extends HttpRequestSender(sinkName, method, client) {

  override protected def updateRequest(request: Request[IO]): IO[Request[IO]] = IO.pure(request)
}

class BasicAuthenticationHttpRequestSender(
  sinkName: String,
  method:   Method,
  client:   Client[IO],
  username: String,
  password: String,
) extends HttpRequestSender(sinkName, method, client) {

  override protected def updateRequest(request: Request[IO]): IO[Request[IO]] =
    IO.pure(request.putHeaders(Authorization(BasicCredentials(username, password))))
}

class OAuth2AuthenticationHttpRequestSender(
  sinkName:      String,
  method:        Method,
  client:        Client[IO],
  tokenProvider: AccessTokenProvider[IO],
) extends HttpRequestSender(sinkName, method, client) {

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
  ): IO[HttpRequestSender] = authentication match {
    case NoAuthentication => IO(new NoAuthenticationHttpRequestSender(sinkName, method, client))
    case BasicAuthentication(username, password) =>
      IO(new BasicAuthenticationHttpRequestSender(sinkName, method, client, username, password))
    case OAuth2Authentication(uri, clientId, clientSecret, tokenProperty, clientScope, clientHeaders) =>
      val rawHeaders = clientHeaders.map { case (k, v) => Header.Raw(CIString(k), v) }
      val tokenProvider =
        new OAuth2AccessTokenProvider(client, uri, clientId, clientSecret, clientScope, rawHeaders, tokenProperty)
      for {
        ref                <- Ref.of[IO, Option[AccessToken]](none)
        cachedTokenProvider = new CachedAccessTokenProvider(tokenProvider, ref)
      } yield new OAuth2AuthenticationHttpRequestSender(sinkName, method, client, cachedTokenProvider)
  }
}

abstract class HttpRequestSender(
  sinkName: String,
  method:   Method,
  client:   Client[IO],
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
      response             <- executeRequestAndHandleErrors(authenticatedRequest)
      _                    <- IO.delay(logger.trace(s"[$sinkName] Response: $response"))
    } yield response

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
      response.as[String].map { body =>
        if (response.status.isSuccess) {
          Right(HttpResponseSuccess(response.status.code, body))
        } else {
          Left(
            HttpResponseFailure(
              message         = "Request failed with error response",
              cause           = Option.empty,
              statusCode      = response.status.code.some,
              responseContent = body.some,
            ),
          )
        }
      }
    }.handleErrorWith { error =>
      IO.pure(Left(HttpResponseFailure(
        message         = error.getMessage,
        cause           = error.some,
        statusCode      = none,
        responseContent = none,
      )))
    }
}
