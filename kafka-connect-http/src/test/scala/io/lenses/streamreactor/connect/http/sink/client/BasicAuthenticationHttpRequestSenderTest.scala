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
import cats.effect.kernel.Resource
import cats.effect.unsafe.IORuntime
import io.lenses.streamreactor.connect.http.sink.metrics.HttpSinkMetrics
import io.lenses.streamreactor.connect.http.sink.metrics.MetricsRegistrar
import io.lenses.streamreactor.connect.http.sink.tpl.ProcessedTemplate
import org.http4s._
import org.http4s.client.Client
import org.http4s.implicits.http4sLiteralsSyntax
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentCaptor
import org.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.typelevel.ci.CIString

import java.lang.management.ManagementFactory
import javax.management.ObjectName

class BasicAuthenticationHttpRequestSenderTest extends AnyFunSuiteLike with Matchers with MockitoSugar {
  test("attaches the authorization header to the request") {
    implicit val runtime: IORuntime = IORuntime.global

    val sinkName = "sink" + java.util.UUID.randomUUID().toString
    val method   = Method.POST

    val client: Client[IO] = mock[Client[IO]]
    when(client.run(any[Request[IO]])).thenReturn(Resource.pure(Response[IO](status = Status.Ok).withEntity("OK")))

    val userName = "user"
    val password = "password"
    val metrics  = new HttpSinkMetrics
    MetricsRegistrar.registerMetricsMBean(metrics, sinkName, 1)
    val sender = new BasicAuthenticationHttpRequestSender(sinkName, method, client, userName, password, metrics)
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
    capturedRequest.headers.headers should contain(Header.Raw(CIString("Authorization"), "Basic dXNlcjpwYXNzd29yZA=="))

    metrics.get2xxCount shouldBe 1
    metrics.get4xxCount shouldBe 0
    metrics.get5xxCount shouldBe 0

    val mbs        = ManagementFactory.getPlatformMBeanServer
    val objectName = new ObjectName(MetricsRegistrar.NameTemplate.format(sinkName, 1))
    val mbean      = mbs.getMBeanInfo(objectName)
    mbean.getAttributes.map(_.getName) should contain allElementsOf List("4xxCount",
                                                                         "5xxCount",
                                                                         "P50RequestTimeMs",
                                                                         "P95RequestTimeMs",
                                                                         "P99RequestTimeMs",
                                                                         "OtherErrorsCount",
                                                                         "2xxCount",
    )
    mbs.getAttribute(objectName, "2xxCount") shouldBe 1
    mbs.getAttribute(objectName, "4xxCount") shouldBe 0
    mbs.getAttribute(objectName, "5xxCount") shouldBe 0
    mbs.getAttribute(objectName, "OtherErrorsCount") shouldBe 0
  }

  test("fails with 500") {
    implicit val runtime: IORuntime = IORuntime.global

    val sinkName = "sink" + java.util.UUID.randomUUID().toString
    val method   = Method.POST

    val client: Client[IO] = mock[Client[IO]]
    when(client.run(any[Request[IO]])).thenReturn(Resource.pure(Response[IO](status =
      Status.InternalServerError).withEntity("Internal Server Error")))

    val userName = "user"
    val password = "password"
    val metrics  = new HttpSinkMetrics
    MetricsRegistrar.registerMetricsMBean(metrics, sinkName, 1)
    val sender = new BasicAuthenticationHttpRequestSender(sinkName, method, client, userName, password, metrics)
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
    capturedRequest.headers.headers should contain(Header.Raw(CIString("Authorization"), "Basic dXNlcjpwYXNzd29yZA=="))

    metrics.get2xxCount shouldBe 0
    metrics.get4xxCount shouldBe 0
    metrics.get5xxCount shouldBe 1

    val mbs        = ManagementFactory.getPlatformMBeanServer
    val objectName = new ObjectName(MetricsRegistrar.NameTemplate.format(sinkName, 1))
    val mbean      = mbs.getMBeanInfo(objectName)
    mbean.getAttributes.map(_.getName) should contain allElementsOf List("4xxCount",
                                                                         "5xxCount",
                                                                         "P50RequestTimeMs",
                                                                         "P95RequestTimeMs",
                                                                         "P99RequestTimeMs",
                                                                         "OtherErrorsCount",
                                                                         "2xxCount",
    )
    mbs.getAttribute(objectName, "2xxCount") shouldBe 0
    mbs.getAttribute(objectName, "4xxCount") shouldBe 0
    mbs.getAttribute(objectName, "5xxCount") shouldBe 1
    mbs.getAttribute(objectName, "OtherErrorsCount") shouldBe 0
  }

  test("fails with 400") {
    implicit val runtime: IORuntime = IORuntime.global

    val sinkName = "sink" + java.util.UUID.randomUUID().toString
    val method   = Method.POST

    val client: Client[IO] = mock[Client[IO]]
    when(client.run(any[Request[IO]])).thenReturn(
      Resource.pure(Response[IO](status = Status.BadRequest).withEntity("Bad Request")),
    )

    val userName = "user"
    val password = "password"
    val metrics  = new HttpSinkMetrics
    MetricsRegistrar.registerMetricsMBean(metrics, sinkName, 1)
    val sender = new BasicAuthenticationHttpRequestSender(sinkName, method, client, userName, password, metrics)
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
    capturedRequest.headers.headers should contain(Header.Raw(CIString("Authorization"), "Basic dXNlcjpwYXNzd29yZA=="))

    metrics.get2xxCount shouldBe 0
    metrics.get4xxCount shouldBe 1
    metrics.get5xxCount shouldBe 0

    val mbs        = ManagementFactory.getPlatformMBeanServer
    val objectName = new ObjectName(MetricsRegistrar.NameTemplate.format(sinkName, 1))
    val mbean      = mbs.getMBeanInfo(objectName)
    mbean.getAttributes.map(_.getName) should contain allElementsOf List("4xxCount",
                                                                         "5xxCount",
                                                                         "P50RequestTimeMs",
                                                                         "P95RequestTimeMs",
                                                                         "P99RequestTimeMs",
                                                                         "OtherErrorsCount",
                                                                         "2xxCount",
    )
    mbs.getAttribute(objectName, "2xxCount") shouldBe 0
    mbs.getAttribute(objectName, "4xxCount") shouldBe 1
    mbs.getAttribute(objectName, "5xxCount") shouldBe 0
  }
}
