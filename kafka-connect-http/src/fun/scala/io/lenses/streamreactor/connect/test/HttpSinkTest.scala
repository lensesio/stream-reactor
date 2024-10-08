package io.lenses.streamreactor.connect.test

import cats.implicits._
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
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span

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
  private val BatchSizeSingleRecord    = 1
  private val BatchSizeMultipleRecords = 2

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
  private var randomTestId:     String = _
  private var topic:            String = _

  before {
    randomTestId = UUID.randomUUID().toString
    topic        = "topic" + randomTestId
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
    sendRecordsWithProducer(stringProducer,
                            stringConverters,
                            randomTestId,
                            topic,
                            "My Static Content Template",
                            BatchSizeSingleRecord,
                            false,
                            record,
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
    sendRecordsWithProducer(stringProducer,
                            stringConverters,
                            randomTestId,
                            topic,
                            record,
                            BatchSizeSingleRecord,
                            false,
                            "{{value}}",
    ).asserting {
      requests =>
        requests.size should be(1)
        val firstRequest = requests.head
        new String(firstRequest.getBody) should be(record)
        firstRequest.getMethod should be(RequestMethod.POST)
    }
  }

  test("dynamic string template containing json message fields should be sent to endpoint") {

    setUpWiremockResponse()

    sendRecordsWithProducer[String, Order](
      orderProducer,
      jsonConverters,
      randomTestId,
      topic,
      "product: {{value.product}}",
      BatchSizeSingleRecord,
      false,
      Order(1, "myOrder product", 1.3d, 10),
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

    sendRecordsWithProducer[String, Order](
      orderProducer,
      stringConverters,
      randomTestId,
      topic,
      "whole product message: {{value}}",
      BatchSizeSingleRecord,
      false,
      Order(1, "myOrder product", 1.3d, 10),
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

  test("batched dynamic string template containing whole json message should be sent to endpoint") {

    setUpWiremockResponse()

    sendRecordsWithProducer[String, Order](
      orderProducer,
      stringConverters,
      randomTestId,
      topic,
      "{\"data\":[{{#message}}{{value}},{{/message}}]}",
      BatchSizeMultipleRecords,
      true,
      Order(1, "myOrder product", 1.3d, 10),
      Order(2, "another product", 1.4d, 109),
    ).asserting {
      requests =>
        logger.info("Requests size: {}", requests.size)

        requests.size should be(1)
        val firstRequest = requests.head
        firstRequest.getMethod should be(RequestMethod.POST)
        logger.info("First request: {}", firstRequest)
        logger.info("First request body: {}", firstRequest.getBodyAsString)
        firstRequest.getBodyAsString should be(
          "{\"data\":[{\"id\":1,\"product\":\"myOrder product\",\"price\":1.3,\"qty\":10,\"created\":null},{\"id\":2,\"product\":\"another product\",\"price\":1.4,\"qty\":109,\"created\":null}]}",
        )
    }
  }

  test("dynamic string template containing avro message fields should be sent to endpoint") {

    setUpWiremockResponse()

    val order = Order(1, "myOrder product", 1.3d, 10, "March").toRecord
    sendRecordsWithProducer[String, GenericRecord](
      avroOrderProducer,
      avroConverters,
      randomTestId,
      topic,
      "product: {{value.product}}",
      BatchSizeSingleRecord,
      false,
      order,
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

  private def sendRecordsWithProducer[K, V](
    producer:        Resource[IO, KafkaProducer[K, V]],
    converters:      Map[String, String],
    randomTestId:    String,
    topic:           String,
    contentTemplate: String,
    batchSize:       Int,
    jsonTidy:        Boolean,
    record:          V*,
  ): IO[List[LoggedRequest]] =
    producer.use {
      producer =>
        createConnectorResource(randomTestId, topic, contentTemplate, converters, batchSize, jsonTidy).use {
          _ =>
            record.map {
              rec => IO(sendRecord[K, V](topic, producer, rec))
            }.sequence
              .map { _ =>
                eventually(timeout(Span(10, Seconds)), interval(Span(500, Millis))) {
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
    batchSize:       Int,
    jsonTidy:        Boolean,
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
        batchSize,
        jsonTidy,
      ),
    )

}
