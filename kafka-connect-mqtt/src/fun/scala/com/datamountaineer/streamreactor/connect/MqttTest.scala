package com.datamountaineer.streamreactor.connect

import _root_.io.lenses.streamreactor.connect.testcontainers.connect._
import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.datamountaineer.streamreactor.connect.mqtt.config.MqttConfigConstants._
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class MqttTest extends AsyncFlatSpec with AsyncIOSpec with StreamReactorContainerPerSuite with Matchers {

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

    val resources = for {
      mqttClientAndPayload <- MqttClientResource(
        container.getExtMqttConnectionUrl,
        container.mqttUser,
        container.mqttPassword,
        topicName,
      )
      producer <- createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]])
      _        <- createConnector(sinkConfig)
    } yield (mqttClientAndPayload, producer)
    resources.use {
      case (fnLatestPayload, producer: KafkaProducer[String, Order]) =>
        IO {
          writeRecordToTopic(producer)
          eventually {
            fnLatestPayload().getOrElse(fail("not yet"))
          }
        }
    }.asserting(_ should be(
      """{"id":1,"product":"OP-DAX-P-20150201-95.7","price":94.2,"qty":100,"created":null}""",
    ))
  }

  private def writeRecordToTopic(producer: KafkaProducer[String, Order]): Unit = {
    val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100)
    producer.send(new ProducerRecord[String, Order]("orders", order)).get
    producer.flush()
  }

  private def sinkConfig: ConnectorConfiguration =
    ConnectorConfiguration(
      "mqtt-sink",
      Map(
        "connector.class"          -> StringCnfVal("com.datamountaineer.streamreactor.connect.mqtt.sink.MqttSinkConnector"),
        "tasks.max"                -> IntCnfVal(1),
        "topics"                   -> StringCnfVal("orders"),
        KCQL_CONFIG                -> StringCnfVal(s"INSERT INTO orders SELECT * FROM orders"),
        HOSTS_CONFIG               -> StringCnfVal(container.getMqttConnectionUrl),
        QS_CONFIG                  -> IntCnfVal(1),
        CLEAN_SESSION_CONFIG       -> BooleanCnfVal(true),
        CLIENT_ID_CONFIG           -> StringCnfVal(UUID.randomUUID().toString),
        CONNECTION_TIMEOUT_CONFIG  -> IntCnfVal(1000),
        KEEP_ALIVE_INTERVAL_CONFIG -> IntCnfVal(1000),
        PASSWORD_CONFIG            -> StringCnfVal(container.mqttPassword),
        USER_CONFIG                -> StringCnfVal(container.mqttUser),
      ),
    )

}
