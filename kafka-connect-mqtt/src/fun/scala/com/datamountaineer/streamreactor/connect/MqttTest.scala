package com.datamountaineer.streamreactor.connect

import com.datamountaineer.streamreactor.connect.mqtt.config.MqttConfigConstants._
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import io.lenses.streamreactor.connect.testcontainers.scalatest.fixtures.connect.withConnector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import scala.util.Using

class MqttTest extends AnyFlatSpec with StreamReactorContainerPerSuite with Matchers {

  lazy val container: MqttContainer = MqttContainer().withNetwork(network)

  override def schemaRegistryContainer(): Option[SchemaRegistryContainer] = None

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

}
