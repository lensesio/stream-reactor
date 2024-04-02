package io.lenses.streamreactor.connect.test

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.tomakehurst.wiremock.client.WireMock
import com.github.tomakehurst.wiremock.client.WireMock._
import com.github.tomakehurst.wiremock.http.RequestMethod
import com.github.tomakehurst.wiremock.verification.LoggedRequest
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.funsuite.AsyncFunSuiteLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter
import org.scalatest.EitherValues

import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala

class HttpSinkTest
    extends AsyncFunSuiteLike
    with BeforeAndAfter
    with AsyncIOSpec
    with Matchers
    with StreamReactorContainerPerSuite
    with LazyLogging
    with EitherValues
    with HttpConfiguration {

  private val stringSerializer  = classOf[StringSerializer]
  private val stringProducer    = createProducer[String, String](stringSerializer, stringSerializer)
  private val orderProducer     = createProducer[String, Order](stringSerializer, classOf[KafkaJsonSerializer[Order]])
  private val avroOrderProducer = createProducer[String, GenericRecord](stringSerializer, classOf[KafkaAvroSerializer])
  private val stringConverters = Map(
    "value.converter" -> "org.apache.kafka.connect.storage.StringConverter",
    "key.converter"   -> "org.apache.kafka.connect.storage.StringConverter",
  )

  private val avroConverters = Map(
    "value.converter"                -> "io.confluent.connect.avro.AvroConverter",
    "value.converter.schemas.enable" -> "true",
    "value.converter.schema.registry.url" -> schemaRegistryContainer.map(_.schemaRegistryUrl).getOrElse(
      fail("No SR url"),
    ),
    "key.converter" -> "org.apache.kafka.connect.storage.StringConverter",
  )

  private val jsonConverters = Map(
    "value.converter"                -> "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable" -> "false",
    "key.converter"                  -> "org.apache.kafka.connect.storage.StringConverter",
  )

  private lazy val container: WiremockContainer = new WiremockContainer()
    .withNetwork(network)

  override val connectorModule: String = "http"
  private var randomTestId = UUID.randomUUID().toString
  private def topic        = "topic" + randomTestId

  before {
    randomTestId = UUID.randomUUID().toString
  }

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  test("static string template should be sent to endpoint") {

    setUpWiremockResponse()

    val record = "My First Record"
    sendRecordWithProducer(stringProducer,
                           stringConverters,
                           randomTestId,
                           topic,
                           record,
                           "My Static Content Template",
    ).asserting {
      requests =>
        requests.size should be(1)
        val firstRequest = requests.head
        firstRequest.getMethod should be(RequestMethod.POST)
        new String(firstRequest.getBody) should be("My Static Content Template")
    }
  }

  test("dynamic string template containing message content should be sent to endpoint") {

    setUpWiremockResponse()

    val record = "My First Record"
    sendRecordWithProducer(stringProducer, stringConverters, randomTestId, topic, record, "{{value}}").asserting {
      requests =>
        requests.size should be(1)
        val firstRequest = requests.head
        firstRequest.getMethod should be(RequestMethod.POST)
        new String(firstRequest.getBody) should be("My First Record")
    }
  }

  test("dynamic string template containing json message fields should be sent to endpoint") {

    setUpWiremockResponse()

    sendRecordWithProducer[String, Order](
      orderProducer,
      jsonConverters,
      randomTestId,
      topic,
      Order(1, "myOrder product", 1.3d, 10),
      "product: {{value.product}}",
    ).asserting {
      requests =>
        requests.size should be(1)
        val firstRequest = requests.head
        firstRequest.getMethod should be(RequestMethod.POST)
        new String(firstRequest.getBody) should be("product: myOrder product")
    }
  }

  test("dynamic string template containing whole json message should be sent to endpoint") {

    setUpWiremockResponse()

    sendRecordWithProducer[String, Order](
      orderProducer,
      stringConverters,
      randomTestId,
      topic,
      Order(1, "myOrder product", 1.3d, 10),
      "whole product message: {{value}}",
    ).asserting {
      requests =>
        requests.size should be(1)
        val firstRequest = requests.head
        firstRequest.getMethod should be(RequestMethod.POST)
        new String(firstRequest.getBody) should be(
          "whole product message: {\"id\":1,\"product\":\"myOrder product\",\"price\":1.3,\"qty\":10,\"created\":null}",
        )
    }
  }

  test("dynamic string template containing avro message fields should be sent to endpoint") {

    setUpWiremockResponse()

    val order = Order(1, "myOrder product", 1.3d, 10, "March").toRecord
    sendRecordWithProducer[String, GenericRecord](
      avroOrderProducer,
      avroConverters,
      randomTestId,
      topic,
      order,
      "product: {{value.product}}",
    ).asserting {
      requests =>
        requests.size should be(1)
        val firstRequest = requests.head
        firstRequest.getMethod should be(RequestMethod.POST)
        new String(firstRequest.getBody) should be("product: myOrder product")
    }
  }

  private def setUpWiremockResponse(): Unit = {
    WireMock.configureFor(container.getHost, container.getFirstMappedPort)
    WireMock.reset()

    val url = s"/$randomTestId"
    stubFor(post(urlEqualTo(url))
      .willReturn(aResponse.withHeader("Content-Type", "text/plain")
        .withBody("Hello world!")))
    ()
  }

  private def sendRecordWithProducer[K, V](
    producer:        Resource[IO, KafkaProducer[K, V]],
    converters:      Map[String, String],
    randomTestId:    String,
    topic:           String,
    record:          V,
    contentTemplate: String,
  ): IO[List[LoggedRequest]] =
    producer.use {
      producer =>
        createConnectorResource(randomTestId, topic, contentTemplate, converters).use {
          _ =>
            IO {
              sendRecord[K, V](topic, producer, record)
              eventually {
                verify(postRequestedFor(urlEqualTo(s"/$randomTestId")))
                findAll(postRequestedFor(urlEqualTo(s"/$randomTestId"))).asScala.toList
              }
            }
        }
    }
  private def sendRecord[K, V](topic: String, producer: KafkaProducer[K, V], record: V): Unit = {
    producer.send(new ProducerRecord[K, V](topic, record)).get
    producer.flush()
  }

  def createConnectorResource(
    randomTestId:    String,
    topic:           String,
    contentTemplate: String,
    converters:      Map[String, String],
  ): Resource[IO, String] =
    createConnector(
      sinkConfig(
        randomTestId,
        s"${container.getNetworkAliasUrl}/$randomTestId",
        "post",
        contentTemplate,
        Seq(),
        topic,
        converters,
      ),
    )

}
