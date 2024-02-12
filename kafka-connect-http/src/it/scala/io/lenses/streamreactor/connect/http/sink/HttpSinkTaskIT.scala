package io.lenses.streamreactor.connect.http.sink

import cats.effect.IO
import cats.effect.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.{ post => httpPost }
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.exactly
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.client.WireMock.containing
import com.github.tomakehurst.wiremock.client.WireMock.equalToXml
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod
import io.lenses.streamreactor.connect.http.sink.config.BatchConfiguration
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.time.Minute
import org.scalatest.time.Span
import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class HttpSinkTaskIT extends AsyncFunSuite with AsyncIOSpec with Eventually {
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(1, Minute))
  def wireMockServer: Resource[IO, WireMockServer] =
    for {
      server <- Resource.eval(
        IO.delay(new WireMockServer(wireMockConfig().dynamicPort().bindAddress(Host))),
      )
      _ <- Resource.make(IO.delay(server.start()))(_ => IO.delay(server.stop()))
    } yield server

  def sinkTask(config: String): Resource[IO, HttpSinkTask] =
    for {
      task <- Resource.eval(IO.delay(new HttpSinkTask()))
      _    <- Resource.make(IO.delay(task.start(Map("connect.http.config" -> config).asJava)))(_ => IO.delay(task.stop()))
    } yield task

  private val Host  = "localhost"
  private val users = SampleData.Employees

  test("data triggers post calls") {
    val path = "/awesome/endpoint"
    (for {
      server <- wireMockServer
      configuration = HttpSinkConfig(
        authentication   = Option.empty,
        method           = HttpMethod.Post,
        endpoint         = s"http://$Host:${server.port()}/awesome/endpoint",
        content          = "test",
        headers          = Seq(),
        sslConfig        = Option.empty,
        batch            = BatchConfiguration(2L.some, none, none).some,
        errorThreshold   = none,
        uploadSyncPeriod = none,
      ).toJson
      _         = server.stubFor(httpPost(urlEqualTo(path)).willReturn(aResponse().withStatus(200)))
      sinkTask <- sinkTask(configuration)
      _ = sinkTask.put(
        users.zipWithIndex.map {
          case (struct, i) => new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
        }.asJava,
      )
    } yield server).use { server =>
      IO.delay(eventually(server.verify(exactly(3), postRequestedFor(urlEqualTo(path)))))
    }
  }

  test("data triggers post calls to individual templated endpoints for single records") {
    val path = "/awesome/endpoint/.*"
    (for {
      server <- wireMockServer
      config = HttpSinkConfig(
        authentication   = Option.empty,
        method           = HttpMethod.Post,
        endpoint         = s"http://$Host:${server.port()}/awesome/endpoint/{{value.name}}",
        content          = "{salary: {{value.salary}}}",
        headers          = Seq(),
        sslConfig        = Option.empty,
        batch            = BatchConfiguration(1L.some, none, none).some,
        errorThreshold   = none,
        uploadSyncPeriod = none,
      ).toJson
      _         = server.stubFor(httpPost(urlMatching(path)).willReturn(aResponse().withStatus(200)))
      sinkTask <- sinkTask(config)
      _ = sinkTask.put(
        users.zipWithIndex.map {
          case (struct, i) => new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
        }.asJava,
      )
    } yield server).use { server =>
      // verify REST calls
      IO.delay {
        eventually {
          server.verify(
            exactly(1),
            postRequestedFor(urlEqualTo("/awesome/endpoint/martin")).withRequestBody(containing("{salary: 35896.00}")),
          )
          server.verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/jackie")))
          server.verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/adam")))
          server.verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/jonny")))
          server.verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/jim")))
          server.verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/wilson")))
          server.verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/milson")))
        }
      }
    }
  }

  // TODO: I don't think this is a valid use case unless you want to aggregate records.  It doesn't really make sense, perhaps this should throw an error instead.
  test("data batched to single endpoint for multiple records using a simple template uses the first record") {
    val path = "/awesome/endpoint/.*"
    (for {
      server <- wireMockServer
      configuration = HttpSinkConfig(
        authentication   = Option.empty,
        method           = HttpMethod.Post,
        endpoint         = s"http://$Host:${server.port()}/awesome/endpoint/{{value.name}}",
        content          = "{salary: {{value.salary}}}",
        headers          = Seq(),
        sslConfig        = Option.empty,
        batch            = BatchConfiguration(7L.some, none, none).some,
        errorThreshold   = none,
        uploadSyncPeriod = none,
      ).toJson
      _     = server.stubFor(httpPost(urlMatching(path)).willReturn(aResponse().withStatus(200)))
      task <- sinkTask(configuration)
      _ = task.put(
        users.zipWithIndex.map {
          case (struct, i) => new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
        }.asJava,
      )
    } yield server).use { server =>
      IO.delay {
        eventually {
          server.verify(
            exactly(1),
            postRequestedFor(urlEqualTo("/awesome/endpoint/martin")).withRequestBody(containing("{salary: 35896.00}")),
          )
        }
      }
    }

  }

  test("data batched to single endpoint for multiple records using a loop template") {
    val path = "/awesome/endpoint/.*"
    val expected =
      """
        |<salaries>
        |  <salary>35896.00</salary>
        |  <salary>60039.00</salary>
        |  <salary>65281.00</salary>
        |  <salary>66560.00</salary>
        |  <salary>63530.00</salary>
        |  <salary>23309.00</salary>
        |  <salary>10012.00</salary>
        |</salaries>
        |""".stripMargin

    (for {
      server <- wireMockServer
      configuration = HttpSinkConfig(
        authentication = Option.empty,
        method         = HttpMethod.Post,
        endpoint       = s"http://$Host:${server.port()}/awesome/endpoint/{{value.name}}",
        content =
          s"""
             | <salaries>
             |  {{#message}}
             |    <salary>{{value.salary}}</salary>
             |   {{/message}}
             | </salaries>""".stripMargin,
        headers          = Seq(),
        sslConfig        = Option.empty,
        batch            = BatchConfiguration(7L.some, none, none).some,
        errorThreshold   = none,
        uploadSyncPeriod = none,
      ).toJson
      _     = server.stubFor(httpPost(urlMatching(path)).willReturn(aResponse().withStatus(200)))
      task <- sinkTask(configuration)
      _ = task.put(
        users.zipWithIndex.map {
          case (struct, i) => new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
        }.asJava,
      )
    } yield server).use { server =>
      IO.delay {
        // verify REST calls
        eventually {
          server.verify(
            exactly(1),
            postRequestedFor(urlEqualTo("/awesome/endpoint/martin")).withRequestBody(equalToXml(expected)),
          )
        }
      }
    }
  }

  test("broken endpoint will return failure and error will be thrown") {

    val path = "/awesome/endpoint"
    (for {
      server <- wireMockServer
      configuration = HttpSinkConfig(
        Option.empty,
        HttpMethod.Post,
        s"http://$Host:${server.port()}/awesome/endpoint",
        s"""
           | Ultimately not important for this test""".stripMargin,
        Seq(),
        Option.empty,
        BatchConfiguration(
          1L.some,
          none,
          none,
        ).some,
        none,
        none,
      ).toJson
      _     = server.stubFor(httpPost(urlMatching(path)).willReturn(aResponse().withStatus(404)))
      task <- sinkTask(configuration)
    } yield task).use { task =>
      IO.delay {
        eventually {
          // put data
          assertThrows[ConnectException] {
            task.put(
              users.zipWithIndex.map {
                case (struct, i) =>
                  new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
              }.asJava,
            )
          }
        }
      }
    }
  }

}
