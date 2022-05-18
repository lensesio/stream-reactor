package com.datamountaineer.streamreactor.connect

import io.confluent.kafka.serializers.KafkaAvroSerializer
import io.debezium.testing.testcontainers.ConnectorConfiguration
import io.lenses.streamreactor.connect.testcontainers.RedisContainer
import io.lenses.streamreactor.connect.testcontainers.scalatest.StreamReactorContainerPerSuite
import io.lenses.streamreactor.connect.testcontainers.scalatest.fixtures.connect.withConnector
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Using

class RedisTest extends AnyFlatSpec with StreamReactorContainerPerSuite with Matchers {

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
    Using.resources(container.hostNetwork.jedisClient,
                    createProducer[String, Object](classOf[StringSerializer], classOf[KafkaAvroSerializer]),
    ) {
      (client, producer) =>
        withConnector("redis-sink", sinkConfig()) {
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

          val userFields = client.xinfoStream("lenses").getFirstEntry.getFields
          userFields.get("firstName") should be("John")
          userFields.get("lastName") should be("Smith")
          userFields.get("age") should be("30")
          userFields.get("salary") should be("4830.0")
        }
    }
  }

  private def sinkConfig(): ConnectorConfiguration = {
    val config = ConnectorConfiguration.create
    config.`with`("connector.class", "com.datamountaineer.streamreactor.connect.redis.sink.RedisSinkConnector")
    config.`with`("tasks.max", "1")
    config.`with`("topics", "redis")
    config.`with`("connect.redis.host", container.networkAlias)
    config.`with`("connect.redis.port", Integer.valueOf(container.port))
    config.`with`("connect.redis.kcql", "INSERT INTO lenses SELECT * FROM redis STOREAS STREAM")
    config.`with`("key.converter", "org.apache.kafka.connect.storage.StringConverter")
    config.`with`("value.converter", "io.confluent.connect.avro.AvroConverter")
    config.`with`("value.converter.schema.registry.url", "http://schema-registry:8081")
    config
  }
}
