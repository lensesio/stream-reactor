package com.datamountaineer.streamreactor.connect

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import com.datastax.driver.core.utils.UUIDs
import com.jayway.jsonpath.JsonPath
import io.confluent.kafka.serializers.KafkaJsonSerializer
import io.lenses.streamreactor.connect.model.Order
import io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import io.lenses.streamreactor.connect.testcontainers.connect._
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import io.lenses.streamreactor.connect.testcontainers.CassandraContainer
import io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.utility.DockerImageName

import java.util.Collections

class CassandraTest extends AsyncFlatSpec with AsyncIOSpec with StreamReactorContainerPerSuite with Matchers {

  lazy val container: CassandraContainer = new CassandraContainer(DockerImageName.parse("cassandra:4.1"))
    .withNetwork(network)

  override val schemaRegistryContainer: Option[SchemaRegistryContainer] = None

  override val connectorModule: String = "cassandra"

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  behavior of "Cassandra connector"

  it should "source records" in {
    val resources = for {
      session   <- Resource.fromAutoCloseable(IO(container.cluster.connect()))
      consumer  <- Resource.fromAutoCloseable(IO(createConsumer()))
      connector <- createConnector(sourceConfig())
    } yield {
      (session, consumer, connector)
    }

    resources.use {
      case (cassandraSession, consumer, _) =>
        IO {
          // Create keyspace and table
          cassandraSession.execute(
            "CREATE KEYSPACE source WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
          )
          cassandraSession.execute(
            "CREATE TABLE source.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);",
          )
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
          consumer.unsubscribe()
          changeEvents
        }
    }.asserting {
      (changeEvents: List[ConsumerRecord[String, String]]) =>
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
  }

  it should "sink records" in {
    val resources = for {
      session  <- Resource.fromAutoCloseable(IO(container.cluster.connect()))
      producer <- createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]])
      _        <- createConnector(sinkConfig())
    } yield {
      (session, producer)
    }

    resources.use {
      case (cassandraSession, producer) =>
        IO {
          // Create keyspace and table
          cassandraSession.execute(
            "CREATE KEYSPACE sink WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
          )
          cassandraSession.execute(
            "CREATE TABLE sink.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);",
          )

          // Write records to topic
          val order = Order(1, "OP-DAX-P-20150201-95.7", 94.2, 100, UUIDs.timeBased.toString)
          producer.send(new ProducerRecord[String, Order]("orders", order)).get
          producer.flush()

          eventually {
            val resultSet = cassandraSession.execute("SELECT * FROM sink.orders;")
            assert(resultSet.getAvailableWithoutFetching == 1)
          }

          cassandraSession.execute("SELECT * FROM sink.orders;").one()

        }
    }.asserting {
      one =>
        one.get("id", classOf[Integer]) should be(1)
        one.get("product", classOf[String]) should be("OP-DAX-P-20150201-95.7")
        one.get("price", classOf[Float]) should be(94.2f)
        one.get("qty", classOf[Integer]) should be(100)
    }

  }

  private def sourceConfig(): ConnectorConfiguration =
    ConnectorConfiguration(
      "cassandra-source",
      Map(
        "connector.class" -> StringCnfVal(
          "com.datamountaineer.streamreactor.connect.cassandra.source.CassandraSourceConnector",
        ),
        "connect.cassandra.key.space" -> StringCnfVal("source"),
        "connect.cassandra.kcql" -> StringCnfVal(
          "INSERT INTO orders-topic SELECT * FROM orders PK created INCREMENTALMODE=TIMEUUID",
        ),
        "connect.cassandra.contact.points" -> StringCnfVal("cassandra"),
        "key.converter"                    -> StringCnfVal("org.apache.kafka.connect.json.JsonConverter"),
        "key.converter.schemas.enable"     -> BooleanCnfVal(false),
        "value.converter"                  -> StringCnfVal("org.apache.kafka.connect.json.JsonConverter"),
        "value.converter.schemas.enable"   -> BooleanCnfVal(false),
      ),
    )

  private def sinkConfig(): ConnectorConfiguration =
    ConnectorConfiguration(
      "cassandra-sink",
      Map(
        "connector.class" -> StringCnfVal(
          "com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector",
        ),
        "tasks.max"                        -> IntCnfVal(1),
        "topics"                           -> StringCnfVal("orders"),
        "connect.cassandra.key.space"      -> StringCnfVal("sink"),
        "connect.cassandra.port"           -> IntCnfVal(9042),
        "connect.cassandra.kcql"           -> StringCnfVal("INSERT INTO orders SELECT * FROM orders"),
        "connect.cassandra.contact.points" -> StringCnfVal("cassandra"),
      ),
    )
}
