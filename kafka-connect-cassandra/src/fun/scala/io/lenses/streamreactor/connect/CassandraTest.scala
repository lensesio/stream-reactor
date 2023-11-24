package io.lenses.streamreactor.connect

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.testing.scalatest.AsyncIOSpec
import com.datastax.driver.core.Session
import com.datastax.driver.core.utils.UUIDs
import com.jayway.jsonpath.JsonPath
import _root_.io.confluent.kafka.serializers.KafkaJsonSerializer
import _root_.io.lenses.streamreactor.connect.model.Order
import _root_.io.lenses.streamreactor.connect.testcontainers.CassandraContainer
import _root_.io.lenses.streamreactor.connect.testcontainers.SchemaRegistryContainer
import _root_.io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import _root_.io.lenses.streamreactor.connect.testcontainers.connect._
import _root_.io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
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
      session  <- Resource.fromAutoCloseable(IO(container.cluster.connect()))
      consumer <- Resource.fromAutoCloseable(IO(createConsumer()))
      _        <- createConnector(sourceConfig())
    } yield {
      (session, consumer)
    }

    resources.use {
      case (cassandraSession, consumer) =>
        IO {

          // create table and insert data
          executeQueries(
            cassandraSession,
            "CREATE KEYSPACE source WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
            "CREATE TABLE source.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);",
            "INSERT INTO source.orders (id, created, product, qty, price) VALUES (1, now(), 'OP-DAX-P-20150201-95.7', 100, 94.2);",
            "INSERT INTO source.orders (id, created, product, qty, price) VALUES (2, now(), 'OP-DAX-C-20150201-100', 100, 99.5);",
            "INSERT INTO source.orders (id, created, product, qty, price) VALUES (3, now(), 'FU-KOSPI-C-20150201-100', 200, 150);",
          )

          // Validate
          consumer.subscribe(Collections.singletonList("orders-topic"))
          val changeEvents: List[ConsumerRecord[String, String]] = drain(consumer, 3)
          consumer.unsubscribe()
          changeEvents
        }
    }.asserting {
      changeEvents: List[ConsumerRecord[String, String]] =>
        validateChangeEvent(changeEvents.head, 1, "OP-DAX-P-20150201-95.7", 100, 94.2)
        validateChangeEvent(changeEvents(1), 2, "OP-DAX-C-20150201-100", 100, 99.5)
        validateChangeEvent(changeEvents(2), 3, "FU-KOSPI-C-20150201-100", 200, 150)
    }
  }

  it should "sink records" in {
    val resources = for {
      session <- Resource.fromAutoCloseable(IO(container.cluster.connect()))
      _ = // Create keyspace and table
      executeQueries(
        session,
        "CREATE KEYSPACE sink WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};",
        "CREATE TABLE sink.orders (id int, created timeuuid, product text, qty int, price float, PRIMARY KEY (id, created)) WITH CLUSTERING ORDER BY (created asc);",
      )
      producer <- createProducer[String, Order](classOf[StringSerializer], classOf[KafkaJsonSerializer[Order]])
      _        <- createConnector(sinkConfig())
    } yield {
      (session, producer)
    }

    resources.use {
      case (cassandraSession, producer) =>
        IO {

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
        "connector.class" -> ConfigValue(
          "io.lenses.streamreactor.connect.cassandra.source.CassandraSourceConnector",
        ),
        "connect.cassandra.key.space" -> ConfigValue("source"),
        "connect.cassandra.kcql" -> ConfigValue(
          "INSERT INTO orders-topic SELECT * FROM orders PK created INCREMENTALMODE=TIMEUUID",
        ),
        "connect.cassandra.contact.points" -> ConfigValue("cassandra"),
        "key.converter"                    -> ConfigValue("org.apache.kafka.connect.json.JsonConverter"),
        "key.converter.schemas.enable"     -> ConfigValue(false),
        "value.converter"                  -> ConfigValue("org.apache.kafka.connect.json.JsonConverter"),
        "value.converter.schemas.enable"   -> ConfigValue(false),
      ),
    )

  private def sinkConfig(): ConnectorConfiguration =
    ConnectorConfiguration(
      "cassandra-sink",
      Map(
        "connector.class" -> ConfigValue(
          "io.lenses.streamreactor.connect.cassandra.sink.CassandraSinkConnector",
        ),
        "tasks.max"                        -> ConfigValue(1),
        "topics"                           -> ConfigValue("orders"),
        "connect.cassandra.key.space"      -> ConfigValue("sink"),
        "connect.cassandra.port"           -> ConfigValue(9042),
        "connect.cassandra.kcql"           -> ConfigValue("INSERT INTO orders SELECT * FROM orders"),
        "connect.cassandra.contact.points" -> ConfigValue("cassandra"),
      ),
    )

  private def executeQueries(cassandraSession: Session, queries: String*): Unit =
    queries.foreach(
      cassandraSession.execute,
    )

  private def validateChangeEvent(
    record:  ConsumerRecord[String, String],
    id:      Int,
    product: String,
    qty:     Int,
    price:   Double,
  ) = {
    JsonPath.read[Int](record.value(), "$.id") should be(id)
    JsonPath.read[String](record.value(), "$.product") should be(product)
    JsonPath.read[Int](record.value(), "$.qty") should be(qty)
    JsonPath.read[Double](record.value(), "$.price") should be(price)
  }
}
