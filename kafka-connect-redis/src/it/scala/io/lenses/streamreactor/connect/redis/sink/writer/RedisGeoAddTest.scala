package io.lenses.streamreactor.connect.redis.sink.writer

import io.lenses.streamreactor.connect.redis.sink.config.RedisConfig
import io.lenses.streamreactor.connect.redis.sink.config.RedisConfigConstants
import io.lenses.streamreactor.connect.redis.sink.config.RedisConnectionInfo
import io.lenses.streamreactor.connect.redis.sink.config.RedisSinkSettings
import com.dimafeng.testcontainers.ForAllTestContainer
import com.dimafeng.testcontainers.GenericContainer
import io.lenses.streamreactor.connect.redis.sink.JedisClientBuilder
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import redis.clients.jedis.Jedis
import redis.clients.jedis.args.GeoUnit

import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.MapHasAsJava

class RedisGeoAddTest extends AnyWordSpec with Matchers with MockitoSugar with ForAllTestContainer {

  override val container = GenericContainer(
    dockerImage  = "redis:6-alpine",
    exposedPorts = Seq(6379),
  )

  "Redis Geo Add (GA) writer" should {

    "should write Kafka records to Redis using GEOADD command" in {

      val TOPIC = "address_topic"
      val KCQL  = s"SELECT town from $TOPIC PK country STOREAS GeoAdd"

      val props = Map(
        RedisConfigConstants.REDIS_HOST  -> "localhost",
        RedisConfigConstants.REDIS_PORT  -> container.mappedPort(6379).toString,
        RedisConfigConstants.KCQL_CONFIG -> KCQL,
      ).asJava

      val config         = RedisConfig(props)
      val connectionInfo = new RedisConnectionInfo("localhost", container.mappedPort(6379), None)
      val settings       = RedisSinkSettings(config)
      val writer         = new RedisGeoAdd(settings, JedisClientBuilder.createClient(settings))

      val schema = SchemaBuilder.struct().name("com.example.Cpu")
        .field("longitude", Schema.STRING_SCHEMA)
        .field("latitude", Schema.STRING_SCHEMA)
        .field("country", Schema.STRING_SCHEMA)
        .field("town", Schema.STRING_SCHEMA)

      val struct1 =
        new Struct(schema).put("longitude", "10").put("latitude", "20").put("country", "UK").put("town", "London")
      val struct2 =
        new Struct(schema).put("longitude", "10").put("latitude", "20").put("country", "UK").put("town", "Liverpool")

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)
      val sinkRecord2 = new SinkRecord(TOPIC, 0, null, null, schema, struct2, 2)

      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      // Clean up in-memory jedis
      jedis.flushAll()
      writer.write(Seq(sinkRecord1, sinkRecord2))

      val allrecords = jedis.georadius("UK", 10, 20, 1, GeoUnit.KM)
      val results    = allrecords.asScala.toList.map(_.getMember).map(_.toList.map(_.toChar).mkString)

      results.size shouldBe 2
      results.head shouldBe """{"town":"Liverpool"}"""
      results(1) shouldBe """{"town":"London"}"""
    }

    "should write Kafka records to Redis using GEOADD command and using prefix" in {

      val TOPIC = "address_topic"
      val KCQL  = s"INSERT INTO cities: SELECT town from $TOPIC PK country STOREAS GeoAdd"

      val props = Map(
        RedisConfigConstants.REDIS_HOST  -> "localhost",
        RedisConfigConstants.REDIS_PORT  -> container.mappedPort(6379).toString,
        RedisConfigConstants.KCQL_CONFIG -> KCQL,
      ).asJava

      val config         = RedisConfig(props)
      val connectionInfo = new RedisConnectionInfo("localhost", container.mappedPort(6379), None)
      val settings       = RedisSinkSettings(config)
      val writer         = new RedisGeoAdd(settings, JedisClientBuilder.createClient(settings))

      val schema = SchemaBuilder.struct().name("com.example.Cpu")
        .field("longitude", Schema.STRING_SCHEMA)
        .field("latitude", Schema.STRING_SCHEMA)
        .field("country", Schema.STRING_SCHEMA)
        .field("town", Schema.STRING_SCHEMA)

      val struct1 =
        new Struct(schema).put("longitude", "10").put("latitude", "20").put("country", "UK").put("town", "London")
      val struct2 =
        new Struct(schema).put("longitude", "10").put("latitude", "20").put("country", "UK").put("town", "Liverpool")

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)
      val sinkRecord2 = new SinkRecord(TOPIC, 0, null, null, schema, struct2, 2)

      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      // Clean up in-memory jedis
      jedis.flushAll()

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val allrecords = jedis.georadius("cities:UK", 10, 20, 1, GeoUnit.KM)
      val results    = allrecords.asScala.toList.map(_.getMember).map(_.toList.map(_.toChar).mkString)

      results.size shouldBe 2
      results.head shouldBe """{"town":"Liverpool"}"""
      results(1) shouldBe """{"town":"London"}"""
    }

    "should write Kafka records to Redis using GEOADD command and using prefix and using longitudeField and latitudeField" in {

      val TOPIC = "address_topic"
      val KCQL =
        s"INSERT INTO cities: SELECT town from $TOPIC PK country STOREAS GeoAdd (longitudeField=lng,latitudeField=lat)"

      val props = Map(
        RedisConfigConstants.REDIS_HOST  -> "localhost",
        RedisConfigConstants.REDIS_PORT  -> container.mappedPort(6379).toString,
        RedisConfigConstants.KCQL_CONFIG -> KCQL,
      ).asJava

      val config         = RedisConfig(props)
      val connectionInfo = new RedisConnectionInfo("localhost", container.mappedPort(6379), None)
      val settings       = RedisSinkSettings(config)
      val writer         = new RedisGeoAdd(settings, JedisClientBuilder.createClient(settings))

      val schema = SchemaBuilder.struct().name("com.example.Cpu")
        .field("lng", Schema.STRING_SCHEMA)
        .field("lat", Schema.STRING_SCHEMA)
        .field("country", Schema.STRING_SCHEMA)
        .field("town", Schema.STRING_SCHEMA)

      val struct1 = new Struct(schema).put("lng", "10").put("lat", "20").put("country", "UK").put("town", "London")
      val struct2 = new Struct(schema).put("lng", "10").put("lat", "20").put("country", "UK").put("town", "Liverpool")

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)
      val sinkRecord2 = new SinkRecord(TOPIC, 0, null, null, schema, struct2, 2)

      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      // Clean up in-memory jedis
      jedis.flushAll()

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val allrecords = jedis.georadius("cities:UK", 10, 20, 1, GeoUnit.KM)
      val results    = allrecords.asScala.toList.map(_.getMember).map(_.toList.map(_.toChar).mkString)

      results.size shouldBe 2
      results.head shouldBe """{"town":"Liverpool"}"""
      results(1) shouldBe """{"town":"London"}"""
    }

  }
}
