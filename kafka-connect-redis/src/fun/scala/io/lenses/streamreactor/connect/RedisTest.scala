package io.lenses.streamreactor.connect

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.lenses.streamreactor.connect.testcontainers.RedisContainer
import io.lenses.streamreactor.connect.testcontainers.connect.ConfigValue
import io.lenses.streamreactor.connect.testcontainers.connect.ConnectorConfiguration
import io.lenses.streamreactor.connect.testcontainers.connect.KafkaConnectClient.createConnector
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class RedisTest extends AsyncFlatSpec with AsyncIOSpec with StreamReactorContainerPerSuite with Matchers {

  lazy val container: RedisContainer = RedisContainer().withNetwork(network)

  override val connectorModule: String = "redis"

  override def beforeAll(): Unit = {
    container.start()
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    container.stop()
  }

  behavior of "Redis connector"

  it should "sink records" in {
    val resources = for {
      jedisClient <- container.hostNetwork.jedisClient
      producer    <- createProducer[String, Object](classOf[StringSerializer], classOf[KafkaAvroSerializer])
      connector   <- createConnector(sinkConfig())
    } yield (jedisClient, producer, connector)

    resources.use {
      case (client, producer, _) =>
        IO {
          val userSchema =
            "{\"type\":\"record\",\"name\":\"User\",\n" + "  \"fields\":[{\"name\":\"firstName\",\"type\":\"string\"}," + "{\"name\":\"lastName\",\"type\":\"string\"}," + "{\"name\":\"age\",\"type\":\"int\"}," + "{\"name\":\"salary\",\"type\":\"double\"}]}"
          val parser     = new Schema.Parser()
          val schema     = parser.parse(userSchema)
          val avroRecord = new GenericData.Record(schema)
          avroRecord.put("firstName", "John")
          avroRecord.put("lastName", "Smith")
          avroRecord.put("age", 30)
          avroRecord.put("salary", 4830)

          producer.send(new ProducerRecord[String, Object]("redis", avroRecord)).get
          producer.flush()

          eventually {
            val streamInfo = client.xinfoStream("lenses")
            assert(streamInfo.getLength == 1)
          }

          client.xinfoStream("lenses").getFirstEntry.getFields
        }.asserting {
          userFields =>
            userFields.get("firstName") should be("John")
            userFields.get("lastName") should be("Smith")
            userFields.get("age") should be("30")
            userFields.get("salary") should be("4830.0")
        }
    }
  }

  private def sinkConfig(): ConnectorConfiguration = ConnectorConfiguration(
    "redis-sink",
    Map(
      "connector.class"                     -> ConfigValue("io.lenses.streamreactor.connect.redis.sink.RedisSinkConnector"),
      "tasks.max"                           -> ConfigValue(1),
      "topics"                              -> ConfigValue("redis"),
      "connect.redis.host"                  -> ConfigValue(container.networkAlias),
      "connect.redis.port"                  -> ConfigValue(Integer.valueOf(container.port)),
      "connect.redis.kcql"                  -> ConfigValue("INSERT INTO lenses SELECT * FROM redis STOREAS STREAM"),
      "key.converter"                       -> ConfigValue("org.apache.kafka.connect.storage.StringConverter"),
      "value.converter"                     -> ConfigValue("io.confluent.connect.avro.AvroConverter"),
      "value.converter.schema.registry.url" -> ConfigValue("http://schema-registry:8081"),
    ),
  )
}
