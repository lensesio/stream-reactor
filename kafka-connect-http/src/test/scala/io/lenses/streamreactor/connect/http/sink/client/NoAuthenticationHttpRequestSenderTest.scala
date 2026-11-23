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

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.unsafe.IORuntime
import io.lenses.streamreactor.connect.http.sink.config.CompressionType
import io.lenses.streamreactor.connect.http.sink.metrics.HttpSinkMetrics
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import org.http4s.Header
import org.http4s.Method
import org.http4s.Request
import org.http4s.Response
import org.http4s.Status
import org.http4s.client.Client
import org.http4s.client.middleware.Retry
import org.http4s.client.middleware.RetryPolicy
import org.http4s.implicits.http4sLiteralsSyntax
import org.http4s.jdkhttpclient.JdkHttpClient
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString

import java.io.ByteArrayInputStream
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPInputStream
import com.sun.net.httpserver.HttpServer
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class NoAuthenticationHttpRequestSenderTest extends AnyFunSuiteLike with Matchers with MockitoSugar {
  test("returns the same API request") {
    implicit val runtime: IORuntime = IORuntime.global
    val sinkName = "sink"
    val method   = Method.POST
    val client: Client[IO] = mock[Client[IO]]
    when(client.run(any[Request[IO]])).thenReturn(Resource.pure(Response[IO](status = Status.Ok).withEntity("OK")))

    val metrics = new HttpSinkMetrics
    val sender  = new NoAuthenticationHttpRequestSender(sinkName, method, client, metrics)
    val template = ProcessedTemplate(
      "http://localhost:8080",
      "content",
      List("header" -> "value"),
    )
    sender.sendHttpRequest(template).unsafeRunSync()

    val requestCaptor: ArgumentCaptor[Request[IO]] = ArgumentCaptor.forClass(classOf[Request[IO]])
    verify(client).run(requestCaptor.capture())

    // Verify the request sent
    val capturedRequest: Request[IO] = requestCaptor.getValue

    // Assert that the request is as expected
    capturedRequest.method shouldBe method
    capturedRequest.uri shouldBe uri"http://localhost:8080"
    capturedRequest.headers.headers should contain(Header.Raw(CIString("header"), "value"))
    capturedRequest.body.compile.toVector.unsafeRunSync() shouldBe template.content.getBytes.toVector
  }

  test("gzip compression encodes the body and preserves the content type") {
    implicit val runtime: IORuntime  = IORuntime.global
    val client:           Client[IO] = mock[Client[IO]]
    when(client.run(any[Request[IO]])).thenReturn(Resource.pure(Response[IO](status = Status.Ok).withEntity("OK")))

    val sender = new NoAuthenticationHttpRequestSender(
      "sink",
      Method.POST,
      client,
      new HttpSinkMetrics,
      CompressionType.Gzip,
    )
    val template = ProcessedTemplate(
      "http://localhost:8080",
      "compressed content",
      List("Content-Type" -> "application/json"),
    )

    sender.sendHttpRequest(template).unsafeRunSync()

    val requestCaptor: ArgumentCaptor[Request[IO]] = ArgumentCaptor.forClass(classOf[Request[IO]])
    verify(client).run(requestCaptor.capture())
    val request = requestCaptor.getValue
    request.headers.headers should contain(Header.Raw(CIString("Content-Encoding"), "gzip"))
    request.headers.headers should contain(Header.Raw(CIString("Content-Type"), "application/json"))
    val compressedBody = request.body.compile.toVector.unsafeRunSync().toArray
    request.headers.headers.find(_.name == CIString("Content-Length")).map(_.value) shouldBe Some(
      compressedBody.length.toString,
    )

    val gzip = new GZIPInputStream(new ByteArrayInputStream(compressedBody))
    try new String(gzip.readAllBytes(), StandardCharsets.UTF_8) shouldBe template.content
    finally gzip.close()
  }

  test("gzip compression preserves the default content type") {
    implicit val runtime: IORuntime  = IORuntime.global
    val client:           Client[IO] = mock[Client[IO]]
    when(client.run(any[Request[IO]])).thenReturn(Resource.pure(Response[IO](status = Status.Ok).withEntity("OK")))

    val sender = new NoAuthenticationHttpRequestSender(
      "sink",
      Method.POST,
      client,
      new HttpSinkMetrics,
      CompressionType.Gzip,
    )
    sender.sendHttpRequest(ProcessedTemplate("http://localhost:8080", "compressed content", List.empty)).unsafeRunSync()

    val requestCaptor: ArgumentCaptor[Request[IO]] = ArgumentCaptor.forClass(classOf[Request[IO]])
    verify(client).run(requestCaptor.capture())
    requestCaptor.getValue.headers.headers should contain(
      Header.Raw(CIString("Content-Type"), "text/plain; charset=UTF-8"),
    )
  }

  test("gzip compression sends the body through the JDK HTTP client") {
    implicit val runtime: IORuntime = IORuntime.global
    val receivedBody          = new AtomicReference[Array[Byte]]()
    val receivedContentLength = new AtomicReference[String]()
    val server                = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext(
      "/gzip",
      exchange => {
        receivedBody.set(exchange.getRequestBody.readAllBytes())
        receivedContentLength.set(exchange.getRequestHeaders.getFirst("Content-Length"))
        exchange.sendResponseHeaders(200, -1)
        exchange.close()
      },
    )
    server.start()

    val content = "compressed content"
    val result = JdkHttpClient.simple[IO].use { client =>
      val sender = new NoAuthenticationHttpRequestSender(
        "sink",
        Method.POST,
        client,
        new HttpSinkMetrics,
        CompressionType.Gzip,
      )
      sender.sendHttpRequest(
        ProcessedTemplate(s"http://localhost:${server.getAddress.getPort}/gzip", content, List.empty),
      )
    }.guarantee(IO(server.stop(0))).unsafeRunSync()

    result.isRight shouldBe true
    receivedBody.get().length should be > 0
    receivedContentLength.get() shouldBe receivedBody.get().length.toString
    val gzip = new GZIPInputStream(new ByteArrayInputStream(receivedBody.get()))
    try new String(gzip.readAllBytes(), StandardCharsets.UTF_8) shouldBe content
    finally gzip.close()
  }

  test("gzip compression sends a complete body on each HTTP retry") {
    implicit val runtime: IORuntime = IORuntime.global
    val receivedBodies = new java.util.concurrent.ConcurrentLinkedQueue[Array[Byte]]()
    val attempts       = new AtomicInteger()
    val server         = HttpServer.create(new InetSocketAddress("localhost", 0), 0)
    server.createContext(
      "/gzip-retry",
      exchange => {
        receivedBodies.add(exchange.getRequestBody.readAllBytes())
        val status = if (attempts.getAndIncrement() == 0) Status.ServiceUnavailable.code else Status.Ok.code
        exchange.sendResponseHeaders(status, -1)
        exchange.close()
      },
    )
    server.start()

    val content = "compressed content"
    val retryPolicy: RetryPolicy[IO] = { (_, result, retryCount) =>
      if (result.exists(_.status == Status.ServiceUnavailable) && retryCount <= 1) Some(0.millis) else None
    }
    val result = JdkHttpClient.simple[IO].use { client =>
      val sender = new NoAuthenticationHttpRequestSender(
        "sink",
        Method.POST,
        Retry(retryPolicy)(client),
        new HttpSinkMetrics,
        CompressionType.Gzip,
      )
      sender.sendHttpRequest(
        ProcessedTemplate(s"http://localhost:${server.getAddress.getPort}/gzip-retry", content, List.empty),
      )
    }.guarantee(IO(server.stop(0))).unsafeRunSync()

    result.isRight shouldBe true
    attempts.get() shouldBe 2
    receivedBodies.asScala.toList should have size 2
    receivedBodies.asScala.foreach { bytes =>
      val gzip = new GZIPInputStream(new ByteArrayInputStream(bytes))
      try new String(gzip.readAllBytes(), StandardCharsets.UTF_8) shouldBe content
      finally gzip.close()
    }
  }
}
