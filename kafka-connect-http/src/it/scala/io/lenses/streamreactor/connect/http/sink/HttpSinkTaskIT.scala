package io.lenses.streamreactor.connect.http.sink

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.IO
import cats.effect.Resource
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock.aResponse
import com.github.tomakehurst.wiremock.client.WireMock.containing
import com.github.tomakehurst.wiremock.client.WireMock.equalToXml
import com.github.tomakehurst.wiremock.client.WireMock.exactly
import com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor
import com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo
import com.github.tomakehurst.wiremock.client.WireMock.urlMatching
import com.github.tomakehurst.wiremock.client.WireMock.{ post => httpPost }
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod
import io.lenses.streamreactor.connect.http.sink.config._
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AsyncFunSuite
import org.scalatest.time.Minute
import org.scalatest.time.Span

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava

class HttpSinkTaskIT extends AsyncFunSuite with AsyncIOSpec with Eventually {

  val ERROR_REPORTING_ENABLED_PROP   = "connect.reporting.error.config.enabled";
  val SUCCESS_REPORTING_ENABLED_PROP = "connect.reporting.success.config.enabled";
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

  def sinkTaskUsingProps(config: Map[String, String]): Resource[IO, HttpSinkTask] =
    for {
      task <- Resource.eval(IO.delay(new HttpSinkTask()))
      _    <- Resource.make(IO.delay(task.start(config.asJava)))(_ => IO.delay(task.stop()))
    } yield task

  private val Host             = "localhost"
  private val users            = SampleData.Employees
  private val noAuthentication = "none"

  test("data triggers post calls") {
    val path = "/awesome/endpoint"
    (for {
      server <- wireMockServer
      config: Map[String, String] = Map(
        HttpSinkConfigDef.HttpMethodProp         -> HttpMethod.Post.toString,
        HttpSinkConfigDef.HttpEndpointProp       -> s"http://$Host:${server.port()}/awesome/endpoint",
        HttpSinkConfigDef.HttpRequestContentProp -> "test {{value.name}}",
        HttpSinkConfigDef.AuthenticationTypeProp -> noAuthentication,
        HttpSinkConfigDef.BatchCountProp         -> "1",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      )
      _         = server.stubFor(httpPost(urlEqualTo(path)).willReturn(aResponse().withStatus(200)))
      sinkTask <- sinkTaskUsingProps(config)
      _ = sinkTask.put(
        users.zipWithIndex.map {
          case (struct, i) => new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
        }.asJava,
      )
    } yield server).use { server =>
      IO.delay(
        eventually(
          server.verify(exactly(7), postRequestedFor(urlEqualTo(path))),
        ),
      )
    }
  }

  test("data triggers post calls to individual templated endpoints for single records") {
    val path = "/awesome/endpoint/.*"
    (for {
      server <- wireMockServer
      config: Map[String, String] = Map(
        HttpSinkConfigDef.HttpMethodProp         -> HttpMethod.Post.toString,
        HttpSinkConfigDef.HttpEndpointProp       -> s"http://$Host:${server.port()}/awesome/endpoint/{{value.name}}",
        HttpSinkConfigDef.HttpRequestContentProp -> "{salary: {{value.salary}}}",
        HttpSinkConfigDef.AuthenticationTypeProp -> noAuthentication,
        HttpSinkConfigDef.BatchCountProp         -> "1",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      )
      _         = server.stubFor(httpPost(urlMatching(path)).willReturn(aResponse().withStatus(200)))
      sinkTask <- sinkTaskUsingProps(config)
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
      config: Map[String, String] = Map(
        HttpSinkConfigDef.HttpMethodProp         -> HttpMethod.Post.toString,
        HttpSinkConfigDef.HttpEndpointProp       -> s"http://$Host:${server.port()}/awesome/endpoint/{{value.name}}",
        HttpSinkConfigDef.HttpRequestContentProp -> "{salary: {{value.salary}}}",
        HttpSinkConfigDef.AuthenticationTypeProp -> noAuthentication,
        HttpSinkConfigDef.BatchCountProp         -> "1",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      )
      _     = server.stubFor(httpPost(urlEqualTo(path)).willReturn(aResponse().withStatus(200)))
      task <- sinkTaskUsingProps(config)
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
      config: Map[String, String] = Map(
        HttpSinkConfigDef.HttpMethodProp   -> HttpMethod.Post.toString,
        HttpSinkConfigDef.HttpEndpointProp -> s"http://$Host:${server.port()}/awesome/endpoint/{{value.name}}",
        HttpSinkConfigDef.HttpRequestContentProp ->
          s"""
             | <salaries>
             |  {{#message}}
             |    <salary>{{value.salary}}</salary>
             |   {{/message}}
             | </salaries>""".stripMargin,
        HttpSinkConfigDef.AuthenticationTypeProp -> noAuthentication,
        HttpSinkConfigDef.BatchCountProp         -> "7",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      )
      _     = server.stubFor(httpPost(urlMatching(path)).willReturn(aResponse().withStatus(200)))
      task <- sinkTaskUsingProps(config)
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
      config: Map[String, String] = Map(
        HttpSinkConfigDef.HttpMethodProp   -> HttpMethod.Post.toString,
        HttpSinkConfigDef.HttpEndpointProp -> s"http://$Host:${server.port()}/awesome/endpoint",
        HttpSinkConfigDef.HttpRequestContentProp ->
          s"""
             | Ultimately not important for this test""".stripMargin,
        HttpSinkConfigDef.AuthenticationTypeProp -> noAuthentication,
        HttpSinkConfigDef.BatchCountProp         -> "1",
        ERROR_REPORTING_ENABLED_PROP             -> "false",
        SUCCESS_REPORTING_ENABLED_PROP           -> "false",
      )
      task <- sinkTaskUsingProps(config)
      _     = server.stubFor(httpPost(urlMatching(path)).willReturn(aResponse().withStatus(404)))
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
