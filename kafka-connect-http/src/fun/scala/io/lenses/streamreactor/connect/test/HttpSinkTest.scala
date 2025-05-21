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
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
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

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

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
  private var successTopicName: String = _
  private var failureTopicName: String = _

  before {
    randomTestId     = UUID.randomUUID().toString
    topic            = "topic" + randomTestId
    successTopicName = s"successTopic-$randomTestId"
    failureTopicName = s"failureTopic-$randomTestId"
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

    val records = (1 to 5).map(i => s"Record number $i")
    sendRecordsWithProducer(
      stringProducer,
      stringConverters,
      randomTestId,
      topic,
      "My Static Content Template",
      BatchSizeSingleRecord,
      false,
      5,
      0,
      records: _*,
    ).asserting {
      case (requests, successReporterRecords, failureReporterRecords) =>
        requests.size should be(5)
        requests.map(_.getBody).map(new String(_)).toSet should contain only "My Static Content Template"
        requests.map(_.getMethod).toSet should be(Set(RequestMethod.POST))

        failureReporterRecords should be(Seq.empty)

        successReporterRecords.size should be(5)
        val record = successReporterRecords.head
        record.topic() should be(successTopicName)
        record.value() should be("My Static Content Template")
    }
  }

  /**
   * Retries occur by default and the failed HTTP post will be retried twice before succeeding.
   */
  test("failing scenario written to error reporter") {

    setUpWiremockFailureResponse()

    sendRecordsWithProducer(
      stringProducer,
      stringConverters,
      randomTestId,
      topic,
      "My Static Content Template",
      BatchSizeSingleRecord,
      false,
      1,
      2,
      "Record number 1", // fails
      "Record number 2", // fails
      "Record number 3", // succeeds
    ).asserting {
      case (requests, successReporterRecords, failureReporterRecords) =>
        requests.size should be(3)
        requests.map(_.getBody).map(new String(_)).toSet should contain only "My Static Content Template"
        requests.map(_.getMethod).toSet should be(Set(RequestMethod.POST))

        failureReporterRecords.size should be(2)
        failureReporterRecords.foreach {
          rec =>
            rec.topic() should be(failureTopicName)
            rec.value() should be("My Static Content Template")
        }

        successReporterRecords.size should be(1)
        val successRecord = successReporterRecords.head
        successRecord.topic() should be(successTopicName)
        successRecord.value() should be("My Static Content Template")

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
                            1,
                            0,
                            "{{value}}",
    ).asserting {
      case (requests, _, _) =>
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
      1,
      0,
      Order(1, "myOrder product", 1.3d, 10),
    ).asserting {
      case (requests, _, _) =>
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
      1,
      0,
      Order(1, "myOrder product", 1.3d, 10),
    ).asserting {
      case (requests, _, _) =>
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
      1,
      0,
      Order(1, "myOrder product", 1.3d, 10),
      Order(2, "another product", 1.4d, 109),
    ).asserting {
      case (requests, _, _) =>
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
      1,
      0,
      order,
    ).asserting {
      case (requests, _, _) =>
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

  private def setUpWiremockFailureResponse(): Unit = {
    WireMock.configureFor(container.getHost, container.getFirstMappedPort)
    WireMock.resetAllScenarios()
    WireMock.resetAllRequests()
    WireMock.resetToDefault()
    WireMock.reset()

    val url = s"/$randomTestId"

    stubFor(
      post(urlEqualTo(url))
        .inScenario("failure")
        .whenScenarioStateIs("STARTED")
        .willSetStateTo("ONE ATTEMPT")
        .willReturn(aResponse.withStatus(404).withHeader("Content-Type", "text/plain").withBody("File Not Found")),
    )

    stubFor(
      post(urlEqualTo(url))
        .inScenario("failure")
        .whenScenarioStateIs("ONE ATTEMPT")
        .willSetStateTo("TWO ATTEMPTS")
        .willReturn(aResponse.withStatus(404).withHeader("Content-Type", "text/plain").withBody("File Not Found")),
    )

    stubFor(
      post(urlEqualTo(url))
        .inScenario("failure")
        .whenScenarioStateIs("TWO ATTEMPTS")
        .willReturn(aResponse.withHeader("Content-Type", "text/plain")
          .withBody("Hello world!")),
    )

    WireMock.setScenarioState("failure", "STARTED")

    ()

  }

  def getBootstrapServers: String = s"PLAINTEXT://kafka:9092"

  private def sendRecordsWithProducer[K, V](
    producer:          Resource[IO, KafkaProducer[K, V]],
    converters:        Map[String, String],
    randomTestId:      String,
    topic:             String,
    contentTemplate:   String,
    batchSize:         Int,
    jsonTidy:          Boolean,
    expectedSuccesses: Int,
    expectedFailures:  Int,
    record:            V*,
  ): IO[(
    List[LoggedRequest],
    Seq[ConsumerRecord[String, String]],
    Seq[ConsumerRecord[String, String]],
  )] =
    (for {
      successConsumer <- Resource.fromAutoCloseable(IO(createConsumer()))
      _               <- Resource.eval(IO(successConsumer.subscribe(Seq(successTopicName).asJava)))
      failureConsumer <- Resource.fromAutoCloseable(IO(createConsumer()))
      _               <- Resource.eval(IO(failureConsumer.subscribe(Seq(failureTopicName).asJava)))
      producer        <- producer
      _ <- createConnectorResource(
        randomTestId,
        topic,
        contentTemplate,
        converters,
        batchSize,
        jsonTidy,
        failureTopicName,
        successTopicName,
        getBootstrapServers,
      )
    } yield {

      record.map(
        sendRecord[K, V](topic, producer, _)
          .handleError { error =>
            logger.error("Error encountered sending record via producer", error)
            fail("Error encountered sending record via producer")
          },
      ).sequence.map { _ =>
        var successes = mutable.Seq[ConsumerRecord[String, String]]()
        var failures  = mutable.Seq[ConsumerRecord[String, String]]()
        eventually(timeout(Span(10, Seconds)), interval(Span(500, Millis))) {
          verify(postRequestedFor(urlEqualTo(s"/$randomTestId")))
          val posts = findAll(postRequestedFor(urlEqualTo(s"/$randomTestId"))).asScala.toList
          posts.size should be(expectedFailures + expectedSuccesses)

          successes ++= pollConsumer(successConsumer)
          successes.size should be(expectedSuccesses)

          failures ++= pollConsumer(failureConsumer)
          failures.size should be(expectedFailures)

          (posts, successes.toSeq, failures.toSeq)
        }
      }

    }).use(identity)

  private def pollConsumer[V, K](consumer: KafkaConsumer[String, String]) = {
    val polled       = consumer.poll(Duration.of(10, ChronoUnit.SECONDS))
    val seqOfRecords = polled.iterator().asScala.toSeq
    seqOfRecords
  }

  private def sendRecord[K, V](topic: String, producer: KafkaProducer[K, V], record: V): IO[Unit] =
    for {
      producerRecord <- IO.pure(new ProducerRecord[K, V](topic, record))
      scalaFuture     = IO(Future(producer.send(producerRecord).get(10, TimeUnit.SECONDS)))
      _              <- IO.fromFuture(scalaFuture)
      _              <- IO(producer.flush())
    } yield ()

  def createConnectorResource(
    randomTestId:          String,
    topic:                 String,
    contentTemplate:       String,
    converters:            Map[String, String],
    batchSize:             Int,
    jsonTidy:              Boolean,
    errorReportingTopic:   String,
    successReportingTopic: String,
    bootstrapServers:      String,
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
        errorReportingTopic,
        successReportingTopic,
        bootstrapServers,
      ),
    )

}
