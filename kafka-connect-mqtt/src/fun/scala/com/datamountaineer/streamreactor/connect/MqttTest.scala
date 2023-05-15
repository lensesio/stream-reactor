package com.datamountaineer.streamreactor.connect

import com.datamountaineer.streamreactor.connect.mqtt.config.MqttConfigConstants._
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import io.lenses.streamreactor.connect.testcontainers.scalatest.fixtures.connect.withConnector
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.eclipse.paho.client.mqttv3.MqttMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.UUID
import scala.io.Source
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Using

class MqttTest extends AnyFlatSpec with StreamReactorContainerPerSuite with Matchers {

  lazy val container: MqttContainer = MqttContainer().withNetwork(network)

  override val schemaRegistryContainer: Option[SchemaRegistryContainer] = None

  override val connectorModule: String = "mqtt"

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  behavior of "mqtt connector"
  val topicName = "orders"

  it should "sink records using json" in {

    Using.resources(
      MqttClientResource(
        container.getExtMqttConnectionUrl,
        container.mqttUser,
        container.mqttPassword,
        topicName,
      ),
      createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]]),
    ) {
      (mqttClient: MqttClientResource, producer: KafkaProducer[String, Order]) =>
        withConnector("mqtt-sink", sinkConfig) {
          writeRecordToTopic(producer)
          eventually {
            mqttClient.latestPayloadAsString.getOrElse(fail("not yet")) should be ("""{"id":1,"product":"OP-DAX-P-20150201-95.7","price":94.2,"qty":100,"created":null}""")
          }
          ()
        }
    }
  }

  it should "source records using json" in {

    Using.resources(
      MqttClientResource(
        container.getExtMqttConnectionUrl,
        container.mqttUser,
        container.mqttPassword,
        topicName,
      ),
      createConsumer(),
    ) {
      (mqttClient: MqttClientResource, consumer: KafkaConsumer[String, String]) =>
        consumer.subscribe(List("orders").asJava)
        withConnector("mqtt-source", sourceConfig) {
          mqttClient.client.publish("orders", createSourceMessage)
          eventually {
            val pollResults = consumer.poll(Duration.ofMillis(1000))

            pollResults.count() should be > 0
            val capturedValue = pollResults.iterator().next()
            capturedValue.topic() should be ("orders")
          }
          ()
        }
    }
  }

  private def createSourceMessage = {
    val payload = Source.fromResource("sample.json").mkString.getBytes()
    val msg = new MqttMessage(payload)
    msg.setQos(1)
    msg.setRetained(false)
    msg
  }

  private def writeRecordToTopic(producer: KafkaProducer[String, Order]): Unit = {
    val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100)
    producer.send(new ProducerRecord[String, Order]("orders", order)).get
    producer.flush()
  }

  private def sinkConfig: ConnectorConfiguration =
    ConnectorConfiguration.create
      .`with`("connector.class", "com.datamountaineer.streamreactor.connect.mqtt.sink.MqttSinkConnector")
      .`with`("tasks.max", "1")
      .`with`("topics", "orders")
      .`with`(KCQL_CONFIG, s"INSERT INTO orders SELECT * FROM orders")
      .`with`(HOSTS_CONFIG, container.getMqttConnectionUrl)
      .`with`(QS_CONFIG, "1")
      .`with`(CLEAN_SESSION_CONFIG, "true")
      .`with`(CLIENT_ID_CONFIG, UUID.randomUUID().toString)
      .`with`(CONNECTION_TIMEOUT_CONFIG, "1000")
      .`with`(KEEP_ALIVE_INTERVAL_CONFIG, "1000")
      .`with`(PASSWORD_CONFIG, container.mqttPassword)
      .`with`(USER_CONFIG, container.mqttUser)

  private def sourceConfig: ConnectorConfiguration =
    ConnectorConfiguration.create
      .`with`("connector.class", "com.datamountaineer.streamreactor.connect.mqtt.source.MqttSourceConnector")
      .`with`("tasks.max", "1")
      .`with`("topics", "orders")
      .`with`("connect.mqtt.clean", false)
      .`with`("connect.mqtt.converter.throw.on.error", true)
      .`with`("connect.progress.enabled", true)
      .`with`("connect.mqtt.log.message", true)
      .`with`("errors.log.include.messages", true)
      .`with`("errors.log.enable", true)
      .`with`("key.converter", "org.apache.kafka.connect.storage.StringConverter")
      .`with`("value.converter", "org.apache.kafka.connect.json.JsonConverter")
      .`with`("key.converter.schemas.enable", false)
      .`with`("value.converter.schemas.enable", false)
      .`with`(KCQL_CONFIG, s"INSERT INTO orders SELECT * FROM `orders` WITHCONVERTER=`com.datamountaineer.streamreactor.connect.converters.source.JsonSimpleConverter`")
      .`with`(HOSTS_CONFIG, container.getMqttConnectionUrl)
      .`with`(QS_CONFIG, "1")
      .`with`(CLEAN_SESSION_CONFIG, "true")
      .`with`(CLIENT_ID_CONFIG, UUID.randomUUID().toString)
      .`with`(CONNECTION_TIMEOUT_CONFIG, "1000")
      .`with`(KEEP_ALIVE_INTERVAL_CONFIG, "1000")
      .`with`(PASSWORD_CONFIG, container.mqttPassword)
      .`with`(USER_CONFIG, container.mqttUser)
}
