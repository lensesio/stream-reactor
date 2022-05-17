package com.datamountaineer.streamreactor.connect

import com.datastax.driver.core.utils.UUIDs
import com.jayway.jsonpath.JsonPath
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import io.lenses.streamreactor.connect.testcontainers.scalatest.fixtures.connect.withConnector
import io.lenses.streamreactor.connect.testcontainers.CassandraContainer
import io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.utility.DockerImageName

import java.util.Collections
import scala.util.Using

class CassandraTest extends AnyFlatSpec with StreamReactorContainerPerSuite with Matchers {

  lazy val container: CassandraContainer = new CassandraContainer(DockerImageName.parse("cassandra:3.11.2"))
    .withNetwork(network)

  override def schemaRegistryContainer(): Option[SchemaRegistryContainer] = None

  override val connectorModule: String = "cassandra"

  behavior of "Cassandra connector"

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  it should "source records" in {
    Using.resources(container.cluster.connect(), createConsumer()) { (cassandraSession, consumer) =>
      // Create keyspace and table
      cassandraSession.execute(
        "CREATE KEYSPACE source WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
      )
      cassandraSession.execute(
        "CREATE TABLE source.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);",
      )

      withConnector("cassandra-source", sourceConfig()) {
        // Insert data
        cassandraSession.execute(
          "INSERT INTO source.orders (id, created, product, qty, price) VALUES (1, now(), 'OP-DAX-P-20150201-95.7', 100, 94.2);",
        )
        cassandraSession.execute(
          "INSERT INTO source.orders (id, created, product, qty, price) VALUES (2, now(), 'OP-DAX-C-20150201-100', 100, 99.5);",
        )
        cassandraSession.execute(
          "INSERT INTO source.orders (id, created, product, qty, price) VALUES (3, now(), 'FU-KOSPI-C-20150201-100', 200, 150);",
        )

        // Validate
        consumer.subscribe(Collections.singletonList("orders-topic"))
        val changeEvents: List[ConsumerRecord[String, String]] = drain(consumer, 3)

        {
          val changeEvent: ConsumerRecord[String, String] = changeEvents.head
          JsonPath.read[Int](changeEvent.value(), "$.id") should be(1)
          JsonPath.read[String](changeEvent.value(), "$.product") should be("OP-DAX-P-20150201-95.7")
          JsonPath.read[Int](changeEvent.value(), "$.qty") should be(100)
          JsonPath.read[Double](changeEvent.value(), "$.price") should be(94.2)
        }

        {
          val changeEvent: ConsumerRecord[String, String] = changeEvents(1)
          JsonPath.read[Int](changeEvent.value(), "$.id") should be(2)
          JsonPath.read[String](changeEvent.value(), "$.product") should be("OP-DAX-C-20150201-100")
          JsonPath.read[Int](changeEvent.value(), "$.qty") should be(100)
          JsonPath.read[Double](changeEvent.value(), "$.price") should be(99.5)
        }

        {
          val changeEvent: ConsumerRecord[String, String] = changeEvents(2)
          JsonPath.read[Int](changeEvent.value(), "$.id") should be(3)
          JsonPath.read[String](changeEvent.value(), "$.product") should be("FU-KOSPI-C-20150201-100")
          JsonPath.read[Int](changeEvent.value(), "$.qty") should be(200)
          JsonPath.read[Double](changeEvent.value(), "$.price") should be(150)
        }

        consumer.unsubscribe()
      }
    }
  }

  it should "sink records" in {
    Using.resources(container.cluster.connect(),
                    createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]]),
    ) { (cassandraSession, producer) =>
      // Create keyspace and table
      cassandraSession.execute(
        "CREATE KEYSPACE sink WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
      )
      cassandraSession.execute(
        "CREATE TABLE sink.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);",
      )

      withConnector("cassandra-sink", sinkConfig()) {
        // Write records to topic
        val order = Order(1, UUIDs.timeBased.toString, "OP-DAX-P-20150201-95.7", 94.2, 100)
        producer.send(new ProducerRecord[String, Order]("orders", order)).get
        producer.flush()

        eventually {
          val resultSet = cassandraSession.execute("SELECT * FROM sink.orders;")
          assert(resultSet.getAvailableWithoutFetching == 1)
        }

        val one = cassandraSession.execute("SELECT * FROM sink.orders;").one()
        one.get("id", classOf[Integer]) should be(1)
        one.get("product", classOf[String]) should be("OP-DAX-P-20150201-95.7")
        one.get("price", classOf[Float]) should be(94.2f)
        one.get("qty", classOf[Integer]) should be(100)
      }
    }
  }

  private def sourceConfig(): ConnectorConfiguration = {
    val config = ConnectorConfiguration.create
    config.`with`("connector.class",
                  "com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector",
    )
    config.`with`("connect.cassandra.key.space", "source")
    config.`with`("connect.cassandra.kcql",
                  "INSERT INTO orders-topic SELECT * FROM orders PK created INCREMENTALMODE=TIMEUUID",
    )
    config.`with`("connect.cassandra.contact.points", "cassandra")
    config.`with`("key.converter", "org.apache.kafka.connect.json.JsonConverter")
    config.`with`("key.converter.schemas.enable", "false")
    config.`with`("value.converter", "org.apache.kafka.connect.json.JsonConverter")
    config.`with`("value.converter.schemas.enable", "false")
    config
  }

  private def sinkConfig(): ConnectorConfiguration = {
    val config = ConnectorConfiguration.create
    config.`with`("connector.class", "com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector")
    config.`with`("tasks.max", "1")
    config.`with`("topics", "orders")
    config.`with`("connect.cassandra.key.space", "sink")
    config.`with`("connect.cassandra.port", "9042")
    config.`with`("connect.cassandra.kcql", "INSERT INTO orders SELECT * FROM orders")
    config.`with`("connect.cassandra.contact.points", "cassandra")
    config
  }
}
