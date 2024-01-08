package io.lenses.streamreactor.connect.http.sink

import cats.implicits.catsSyntaxOptionId
import cats.implicits.none
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig
import com.typesafe.scalalogging.LazyLogging
import io.lenses.streamreactor.connect.http.sink.client.HttpMethod
import io.lenses.streamreactor.connect.http.sink.config.BatchConfiguration
import io.lenses.streamreactor.connect.http.sink.config.HttpSinkConfig
import org.apache.kafka.connect.errors.ConnectException
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.time.Minute
import org.scalatest.time.Span

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Using
import scala.util.Using.Releasable

class HttpSinkTaskIT
    extends AnyFunSuiteLike
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Eventually
    with LazyLogging {
  override implicit def patienceConfig: PatienceConfig = PatienceConfig(timeout = Span(1, Minute))

  private val Host           = "localhost"
  private val wireMockServer = new WireMockServer(wireMockConfig().dynamicPort())

  override def beforeAll(): Unit = {
    wireMockServer.start()
    WireMock.configureFor(Host, wireMockServer.port())
  }

  override def beforeEach(): Unit =
    wireMockServer.resetAll()

  override def afterAll(): Unit =
    wireMockServer.stop()

  object HttpSinkTaskReleaseable extends Releasable[HttpSinkTask] {
    override def release(resource: HttpSinkTask): Unit = {
      logger.info("SHUTTING DOWN SINK TASK")
      resource.stop()
    }
  }

  implicit val releaseSinkTask: Releasable[HttpSinkTask] = HttpSinkTaskReleaseable

  private val users = SampleData.Employees

  test("data triggers post calls") {

    val configuration = HttpSinkConfig(
      Option.empty,
      HttpMethod.Post,
      s"http://$Host:${wireMockServer.port()}/awesome/endpoint",
      "test",
      Seq(),
      Option.empty,
      BatchConfiguration(
        2L.some,
        none,
        none,
      ).some,
      none,
      none,
    ).toJson

    val path = "/awesome/endpoint"
    stubFor(post(urlEqualTo(path))
      .willReturn(
        aResponse()
          .withStatus(200),
      ))

    Using.resource(createSinkTask(configuration)) {
      sinkTask: HttpSinkTask =>
        // put data
        sinkTask.put(
          users.zipWithIndex.map {
            case (struct, i) =>
              new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
          }.asJava,
        )
        eventually {
          verify(exactly(3), postRequestedFor(urlEqualTo(path)))
        }
    }

  }

  test("data triggers post calls to individual templated endpoints for single records") {

    val configuration = HttpSinkConfig(
      Option.empty,
      HttpMethod.Post,
      s"http://$Host:${wireMockServer.port()}/awesome/endpoint/{{value.name}}",
      "{salary: {{value.salary}}}",
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

    val path = "/awesome/endpoint/.*"
    stubFor(post(urlMatching(path))
      .willReturn(
        aResponse()
          .withStatus(200),
      ))

    Using.resource(createSinkTask(configuration)) {
      sinkTask: HttpSinkTask =>
        // put data
        sinkTask.put(
          users.zipWithIndex.map {
            case (struct, i) =>
              new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
          }.asJava,
        )

        // verify REST calls
        eventually {
          verify(
            exactly(1),
            postRequestedFor(urlEqualTo("/awesome/endpoint/martin"))
              .withRequestBody(containing("{salary: 35896.00}")),
          )
          verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/jackie")))
          verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/adam")))
          verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/jonny")))
          verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/jim")))
          verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/wilson")))
          verify(exactly(1), postRequestedFor(urlEqualTo("/awesome/endpoint/milson")))
        }

    }
  }

  // TODO: I don't think this is a valid use case unless you want to aggregate records.  It doesn't really make sense, perhaps this should throw an error instead.
  test("data batched to single endpoint for multiple records using a simple template uses the first record") {

    val configuration = HttpSinkConfig(
      Option.empty,
      HttpMethod.Post,
      s"http://$Host:${wireMockServer.port()}/awesome/endpoint/{{value.name}}",
      "{salary: {{value.salary}}}",
      Seq(),
      Option.empty,
      BatchConfiguration(
        7L.some,
        none,
        none,
      ).some,
      none,
      none,
    ).toJson

    val path = "/awesome/endpoint/.*"
    stubFor(post(urlMatching(path))
      .willReturn(
        aResponse()
          .withStatus(200),
      ))

    Using.resource(createSinkTask(configuration)) {
      sinkTask: HttpSinkTask =>
        // put data
        sinkTask.put(
          users.zipWithIndex.map {
            case (struct, i) =>
              new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
          }.asJava,
        )

        // verify REST calls
        eventually {
          verify(
            exactly(1),
            postRequestedFor(urlEqualTo("/awesome/endpoint/martin"))
              .withRequestBody(containing("{salary: 35896.00}")),
          )

        }

    }

  }

  test("data batched to single endpoint for multiple records using a loop template") {

    val configuration = HttpSinkConfig(
      Option.empty,
      HttpMethod.Post,
      s"http://$Host:${wireMockServer.port()}/awesome/endpoint/{{value.name}}",
      s"""
         | <salaries>
         |  {{#message}}
         |    <salary>{{value.salary}}</salary>
         |   {{/message}}
         | </salaries>""".stripMargin,
      Seq(),
      Option.empty,
      BatchConfiguration(
        7L.some,
        none,
        none,
      ).some,
      none,
      none,
    ).toJson

    val path = "/awesome/endpoint/.*"
    stubFor(post(urlMatching(path))
      .willReturn(
        aResponse()
          .withStatus(200),
      ))

    Using.resource(createSinkTask(configuration)) {
      sinkTask: HttpSinkTask =>
        // put data
        sinkTask.put(
          users.zipWithIndex.map {
            case (struct, i) =>
              new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
          }.asJava,
        )

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
        // verify REST calls
        eventually {
          verify(
            exactly(1),
            postRequestedFor(urlEqualTo("/awesome/endpoint/martin"))
              .withRequestBody(equalToXml(expected)),
          )

        }

    }

  }

  test("broken endpoint will return failure and error will be thrown") {

    val configuration = HttpSinkConfig(
      Option.empty,
      HttpMethod.Post,
      s"http://$Host:${wireMockServer.port()}/awesome/endpoint",
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

    val path = "/awesome/endpoint"
    stubFor(post(urlMatching(path))
      .willReturn(
        aResponse()
          .withStatus(404),
      ))

    Using.resource(createSinkTask(configuration)) {
      sinkTask: HttpSinkTask =>
        eventually {
          Thread.sleep(500)

          // put data
          assertThrows[ConnectException] {
            sinkTask.put(
              users.zipWithIndex.map {
                case (struct, i) =>
                  new SinkRecord("myTopic", 0, null, null, SampleData.EmployeesSchema, struct, i.toLong)
              }.asJava,
            )
          }
        }

    }

  }

  private def createSinkTask(configuration: String) = {
    val sinkTask = new HttpSinkTask()
    sinkTask.start(
      Map(
        "connect.http.config" -> configuration,
      ).asJava,
    )
    sinkTask
  }
}
