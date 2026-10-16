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
package io.lenses.streamreactor.connect.http.sink.client

import java.net.Authenticator
import java.net.CookieHandler
import java.net.ProxySelector
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse
import java.net.http.WebSocket
import java.time.Duration
import java.util.Optional
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executor
import javax.net.ssl.SSLContext
import javax.net.ssl.SSLParameters

/**
 * Delegating `HttpClient` that gives every request without its own timeout a JDK-level
 * `HttpRequest` timeout.
 *
 * http4s-jdk-http-client waits for response headers inside an uncancelable `Resource` acquire, so
 * an effect-level timeout cannot interrupt a request the endpoint never answers. A JDK request
 * timeout fails the exchange itself with `HttpTimeoutException` and tears it down, so the acquire
 * completes, its resources are released, and nothing is left behind. It bounds the wait for
 * response headers only; the response body is bounded separately.
 */
class RequestTimeoutHttpClient(delegate: HttpClient, requestTimeout: Duration) extends HttpClient {

  override def cookieHandler():       Optional[CookieHandler] = delegate.cookieHandler()
  override def connectTimeout():      Optional[Duration]      = delegate.connectTimeout()
  override def followRedirects():     HttpClient.Redirect     = delegate.followRedirects()
  override def proxy():               Optional[ProxySelector] = delegate.proxy()
  override def sslContext():          SSLContext              = delegate.sslContext()
  override def sslParameters():       SSLParameters           = delegate.sslParameters()
  override def authenticator():       Optional[Authenticator] = delegate.authenticator()
  override def version():             HttpClient.Version      = delegate.version()
  override def executor():            Optional[Executor]      = delegate.executor()
  override def newWebSocketBuilder(): WebSocket.Builder       = delegate.newWebSocketBuilder()

  override def send[T](request: HttpRequest, handler: HttpResponse.BodyHandler[T]): HttpResponse[T] =
    delegate.send(withTimeout(request), handler)

  override def sendAsync[T](
    request: HttpRequest,
    handler: HttpResponse.BodyHandler[T],
  ): CompletableFuture[HttpResponse[T]] =
    delegate.sendAsync(withTimeout(request), handler)

  override def sendAsync[T](
    request:            HttpRequest,
    handler:            HttpResponse.BodyHandler[T],
    pushPromiseHandler: HttpResponse.PushPromiseHandler[T],
  ): CompletableFuture[HttpResponse[T]] =
    delegate.sendAsync(withTimeout(request), handler, pushPromiseHandler)

  // Rebuilt field by field: the copying `HttpRequest.newBuilder(HttpRequest, BiPredicate)` is JDK 16+
  // and this module compiles against the JDK 11 API.
  private[client] def withTimeout(request: HttpRequest): HttpRequest =
    if (request.timeout().isPresent) request
    else {
      val builder = HttpRequest.newBuilder(request.uri())
        .method(request.method(), request.bodyPublisher().orElse(BodyPublishers.noBody()))
        .expectContinue(request.expectContinue())
        .timeout(requestTimeout)
      request.version().ifPresent { v => builder.version(v); () }
      request.headers().map().forEach { (name, values) =>
        values.forEach { value => builder.header(name, value); () }
      }
      builder.build()
    }
}
