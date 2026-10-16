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
import cats.effect.unsafe.implicits.global
import io.lenses.streamreactor.connect.http.sink.HttpWriterManager
import io.lenses.streamreactor.connect.http.sink.metrics.HttpSinkMetrics
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import org.http4s.Method
import org.http4s.jdkhttpclient.JdkHttpClient
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers

import java.io.InputStream
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

class RequestTimeoutHttpClientTest extends AnyFunSuiteLike with Matchers {

  // Loopback HTTP/1.1 server. `respond = false` accepts and reads requests but never answers.
  private class TestServer(respond: Boolean) extends AutoCloseable {
    private val server  = new ServerSocket(0, 50, InetAddress.getLoopbackAddress)
    private val clients = new ConcurrentLinkedQueue[Socket]()
    val requests        = new ConcurrentLinkedQueue[String]()
    val connectionClosed = new CountDownLatch(1)

    daemon { () =>
      while (!server.isClosed) {
        val socket = server.accept()
        clients.add(socket)
        daemon { () =>
          val in = socket.getInputStream
          if (respond) {
            requests.add(readRequest(in))
            socket.getOutputStream.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.US_ASCII))
            socket.getOutputStream.flush()
          }
          while (in.read() != -1) {}
          connectionClosed.countDown()
        }
      }
    }

    val url: String = s"http://127.0.0.1:${server.getLocalPort}/"

    override def close(): Unit = {
      server.close()
      clients.asScala.foreach(_.close())
    }

    private def daemon(body: () => Unit): Unit = {
      val thread = new Thread(() =>
        try body()
        catch { case _: Throwable => connectionClosed.countDown() },
      )
      thread.setDaemon(true)
      thread.start()
    }

    private def readRequest(in: InputStream): String = {
      val head = new StringBuilder
      while (!head.endsWith("\r\n\r\n")) head.append(in.read().toChar)
      val length = "(?i)content-length:\\s*(\\d+)".r.findFirstMatchIn(head).map(_.group(1).toInt).getOrElse(0)
      head.toString + new String(in.readNBytes(length), StandardCharsets.UTF_8)
    }
  }

  // Production wiring: the JDK request timeout under JdkHttpClient, with the effect-level
  // `withRequestTimeout` on top set far longer so only the JDK timeout can fire here.
  private def send(url: String, jdkTimeout: Duration, headers: Seq[(String, String)] = Seq.empty) =
    JdkHttpClient[IO](new RequestTimeoutHttpClient(HttpClient.newHttpClient(), jdkTimeout)).use { client =>
      val sender = new NoAuthenticationHttpRequestSender(
        "test",
        Method.POST,
        HttpWriterManager.withRequestTimeout(client, 30.seconds),
        new HttpSinkMetrics(),
      )
      IO.blocking(
        Await.result(sender.sendHttpRequest(ProcessedTemplate(url, "content", headers)).unsafeToFuture(), 5.seconds),
      )
    }.unsafeRunSync()

  test("a request the endpoint never answers fails and its connection is torn down") {
    val server = new TestServer(respond = false)
    try {
      send(server.url, Duration.ofMillis(200)).isLeft shouldBe true
      server.connectionClosed.await(5, TimeUnit.SECONDS) shouldBe true
    } finally server.close()
  }

  test("an answered request still succeeds and keeps its method, headers and body") {
    val server = new TestServer(respond = true)
    try {
      send(server.url, Duration.ofSeconds(5), Seq("X-Test" -> "kept")).isRight shouldBe true

      val request = server.requests.asScala.toList
      request should have size 1
      request.head should startWith("POST / HTTP/1.1")
      request.head.toLowerCase should include("x-test: kept")
      request.head should endWith("content")
    } finally server.close()
  }

  private val client = new RequestTimeoutHttpClient(HttpClient.newHttpClient(), Duration.ofSeconds(3))

  test("a request that already has a timeout is left unchanged") {
    val request = HttpRequest.newBuilder(URI.create("http://example.test")).timeout(Duration.ofSeconds(9)).build()

    client.withTimeout(request) should be theSameInstanceAs request
  }

  test("a request without a timeout gets the configured one and keeps its other fields") {
    val request = HttpRequest.newBuilder(URI.create("http://example.test/path"))
      .method("PUT", BodyPublishers.ofString("body"))
      .header("X-One", "a")
      .header("X-One", "b")
      .version(HttpClient.Version.HTTP_1_1)
      .expectContinue(true)
      .build()

    val timed = client.withTimeout(request)

    timed.timeout().get() shouldBe Duration.ofSeconds(3)
    timed.method() shouldBe "PUT"
    timed.uri() shouldBe request.uri()
    timed.headers().map() shouldBe request.headers().map()
    timed.version() shouldBe request.version()
    timed.expectContinue() shouldBe true
    timed.bodyPublisher().get().contentLength() shouldBe 4L
  }
}
