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
import cats.effect.Ref
import fs2.Stream
import cats.effect.kernel.Resource
import cats.effect.unsafe.IORuntime
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
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString

import scala.concurrent.duration.DurationInt

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

  test("response-code counters record the final response after retries, including exhausted retries") {
    implicit val runtime: IORuntime = IORuntime.global
    for (terminalStatus <- List(Status.Ok, Status.ServiceUnavailable)) {
      val metrics = new HttpSinkMetrics()
      val retryPolicy: RetryPolicy[IO] = { (_, result, retryCount) =>
        if (result.exists(_.status == Status.ServiceUnavailable) && retryCount <= 1) Some(0.millis) else None
      }
      val result = (for {
        responses <- Ref.of[IO, List[Status]](List(Status.ServiceUnavailable, terminalStatus))
        client = Client[IO] { _ =>
          Resource.eval(
            responses.modify {
              case status :: remaining => remaining -> Response[IO](status)
              case Nil                 => Nil       -> Response[IO](Status.InternalServerError)
            },
          )
        }
        sender  = new NoAuthenticationHttpRequestSender("sink", Method.POST, Retry(retryPolicy)(client), metrics)
        result <- sender.sendHttpRequest(ProcessedTemplate("http://example.test", "content", List.empty))
      } yield result).unsafeRunSync()

      result.isRight shouldBe terminalStatus.isSuccess
      metrics.getHttp200Count shouldBe (if (terminalStatus.isSuccess) 1L else 0L)
      metrics.getHttp503Count shouldBe (if (terminalStatus.isSuccess) 0L else 1L)
      metrics.get2xxCount shouldBe metrics.getHttp200Count
      metrics.get5xxCount shouldBe metrics.getHttp503Count
    }
  }

  test("transport and response-body failures do not increment response-code counters") {
    implicit val runtime: IORuntime = IORuntime.global
    val failure = new RuntimeException("request failed")
    val clients = List(
      Client[IO](_ => Resource.eval(IO.raiseError[Response[IO]](failure))),
      Client[IO](_ => Resource.pure(Response[IO](Status.Ok).withBodyStream(Stream.raiseError[IO](failure)))),
    )
    clients.foreach { client =>
      val metrics = new HttpSinkMetrics()
      val sender  = new NoAuthenticationHttpRequestSender("sink", Method.POST, client, metrics)
      val result =
        sender.sendHttpRequest(ProcessedTemplate("http://example.test", "content", List.empty)).unsafeRunSync()

      result.isLeft shouldBe true
      metrics.getHttp200Count shouldBe 0L
      metrics.getOtherHttpStatusCodeCount shouldBe 0L
      metrics.get2xxCount shouldBe 0L
      metrics.get4xxCount shouldBe 0L
      metrics.get5xxCount shouldBe 0L
      metrics.getOtherErrorsCount shouldBe 0L
    }
  }
}
