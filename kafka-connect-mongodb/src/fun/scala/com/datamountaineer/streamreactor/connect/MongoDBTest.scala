package com.datamountaineer.streamreactor.connect

import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.{MongoDBContainer, SchemaRegistryContainer}
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import io.lenses.streamreactor.connect.testcontainers.scalatest.fixtures.connect.withConnector
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Using

class MongoDBTest extends AnyFlatSpec with StreamReactorContainerPerSuite with Matchers {

  lazy val container: MongoDBContainer = MongoDBContainer().withNetwork(network)

  override def schemaRegistryContainer(): Option[SchemaRegistryContainer] = None

  override val connectorModule: String = "mongodb"

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  behavior of "MongoDB connector"

  it should "sink records" in {
    Using.resources(
      container.hostNetwork.mongoClient,
      createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]]),
    ) {
      (client, producer) =>
        withConnector("mongo-sink", sinkConfig()) {
          // Write records to topic
          val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100)
          producer.send(new ProducerRecord[String, Order]("orders", order)).get
          producer.flush()

          // Validate
          val database = client.getDatabase("connect")
          val orders   = database.getCollection("orders")

          eventually {
            val ordersCount = orders.countDocuments
            assert(ordersCount == 1)
          }

          val one = orders.find.iterator.next
          one.get("id", classOf[java.lang.Long]) should be(1)
          one.get("product", classOf[String]) should be("OP-DAX-P-20150201-95.7")
        }
    }
  }

  private def sinkConfig(): ConnectorConfiguration = {
    val config = ConnectorConfiguration.create
    config.`with`("connector.class", "com.datamountaineer.streamreactor.connect.mongodb.sink.MongoSinkConnector")
    config.`with`("tasks.max", "1")
    config.`with`("topics", "orders")
    config.`with`("connect.mongo.kcql", "INSERT INTO orders SELECT * FROM orders")
    config.`with`("connect.mongo.db", "connect")
    config.`with`("connect.mongo.connection", s"mongodb://mongo:${container.port}")
    config
  }
}
