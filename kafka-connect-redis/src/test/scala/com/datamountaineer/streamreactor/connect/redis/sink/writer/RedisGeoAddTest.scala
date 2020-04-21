package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConfig, RedisConfigConstants, RedisConnectionInfo, RedisSinkSettings}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import redis.clients.jedis.{GeoUnit, Jedis}
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

class RedisGeoAddTest extends AnyWordSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  val redisServer = new RedisServer(6379)

  override def beforeAll() = redisServer.start()

  override def afterAll() = redisServer.stop()

  "Redis Geo Add (GA) writer" should {

    "should write Kafka records to Redis using GEOADD command" in {

      val TOPIC = "address_topic"
      val KCQL = s"SELECT town from $TOPIC PK country STOREAS GeoAdd"
      println("Testing KCQL : " + KCQL)
      val props = Map(
        RedisConfigConstants.REDIS_HOST -> "localhost",
        RedisConfigConstants.REDIS_PORT -> "6379",
        RedisConfigConstants.KCQL_CONFIG -> KCQL
      ).asJava

      val config = RedisConfig(props)
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisGeoAdd(settings)
      writer.createClient(settings)

      val schema = SchemaBuilder.struct().name("com.example.Cpu")
        .field("longitude", Schema.STRING_SCHEMA)
        .field("latitude", Schema.STRING_SCHEMA)
        .field("country", Schema.STRING_SCHEMA)
        .field("town", Schema.STRING_SCHEMA)


      val struct1 = new Struct(schema).put("longitude", "10").put("latitude", "20").put("country", "UK").put("town", "London")
      val struct2 = new Struct(schema).put("longitude", "10").put("latitude", "20").put("country", "UK").put("town", "Liverpool")
      val struct3 = new Struct(schema).put("country", "UK").put("town", "Manchester")

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)
      val sinkRecord2 = new SinkRecord(TOPIC, 0, null, null, schema, struct2, 2)
      val sinkRecord3 = new SinkRecord(TOPIC, 0, null, null, schema, struct3, 3)

      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      // Clean up in-memory jedis
      jedis.flushAll()

      writer.write(Seq(sinkRecord1))
      writer.write(Seq(sinkRecord2, sinkRecord3))

      val allrecords = jedis.georadius("UK", 10, 20, 1, GeoUnit.KM)
      val results = allrecords.asScala.toList.map(_.getMember).map(_.toList.map(_.toChar).mkString)

      results.size shouldBe 2
      results.head shouldBe """{"town":"Liverpool"}"""
      results(1) shouldBe """{"town":"London"}"""
    }

    "should write Kafka records to Redis using GEOADD command and using prefix" in {

      val TOPIC = "address_topic"
      val KCQL = s"INSERT INTO cities: SELECT town from $TOPIC PK country STOREAS GeoAdd"
      println("Testing KCQL : " + KCQL)
      val props = Map(
        RedisConfigConstants.REDIS_HOST -> "localhost",
        RedisConfigConstants.REDIS_PORT -> "6379",
        RedisConfigConstants.KCQL_CONFIG -> KCQL
      ).asJava

      val config = RedisConfig(props)
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisGeoAdd(settings)
      writer.createClient(settings)

      val schema = SchemaBuilder.struct().name("com.example.Cpu")
        .field("longitude", Schema.STRING_SCHEMA)
        .field("latitude", Schema.STRING_SCHEMA)
        .field("country", Schema.STRING_SCHEMA)
        .field("town", Schema.STRING_SCHEMA)


      val struct1 = new Struct(schema).put("longitude", "10").put("latitude", "20").put("country", "UK").put("town", "London")
      val struct2 = new Struct(schema).put("longitude", "10").put("latitude", "20").put("country", "UK").put("town", "Liverpool")
      val struct3 = new Struct(schema).put("country", "UK").put("town", "Manchester")

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)
      val sinkRecord2 = new SinkRecord(TOPIC, 0, null, null, schema, struct2, 2)
      val sinkRecord3 = new SinkRecord(TOPIC, 0, null, null, schema, struct3, 3)

      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      // Clean up in-memory jedis
      jedis.flushAll()

      writer.write(Seq(sinkRecord1))
      writer.write(Seq(sinkRecord2, sinkRecord3))

      val allrecords = jedis.georadius("cities:UK", 10, 20, 1, GeoUnit.KM)
      val results = allrecords.asScala.toList.map(_.getMember).map(_.toList.map(_.toChar).mkString)

      results.size shouldBe 2
      results.head shouldBe """{"town":"Liverpool"}"""
      results(1) shouldBe """{"town":"London"}"""
    }

    "should write Kafka records to Redis using GEOADD command and using prefix and using longitudeField and latitudeField" in {

      val TOPIC = "address_topic"
      val KCQL = s"INSERT INTO cities: SELECT town from $TOPIC PK country STOREAS GeoAdd (longitudeField=lng,latitudeField=lat)"
      println("Testing KCQL : " + KCQL)
      val props = Map(
        RedisConfigConstants.REDIS_HOST -> "localhost",
        RedisConfigConstants.REDIS_PORT -> "6379",
        RedisConfigConstants.KCQL_CONFIG -> KCQL
      ).asJava

      val config = RedisConfig(props)
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisGeoAdd(settings)
      writer.createClient(settings)

      val schema = SchemaBuilder.struct().name("com.example.Cpu")
        .field("lng", Schema.STRING_SCHEMA)
        .field("lat", Schema.STRING_SCHEMA)
        .field("country", Schema.STRING_SCHEMA)
        .field("town", Schema.STRING_SCHEMA)


      val struct1 = new Struct(schema).put("lng", "10").put("lat", "20").put("country", "UK").put("town", "London")
      val struct2 = new Struct(schema).put("lng", "10").put("lat", "20").put("country", "UK").put("town", "Liverpool")
      val struct3 = new Struct(schema).put("country", "UK").put("town", "Manchester")

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)
      val sinkRecord2 = new SinkRecord(TOPIC, 0, null, null, schema, struct2, 2)
      val sinkRecord3 = new SinkRecord(TOPIC, 0, null, null, schema, struct3, 3)

      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      // Clean up in-memory jedis
      jedis.flushAll()

      writer.write(Seq(sinkRecord1))
      writer.write(Seq(sinkRecord2, sinkRecord3))

      val allrecords = jedis.georadius("cities:UK", 10, 20, 1, GeoUnit.KM)
      val results = allrecords.asScala.toList.map(_.getMember).map(_.toList.map(_.toChar).mkString)

      results.size shouldBe 2
      results.head shouldBe """{"town":"Liverpool"}"""
      results(1) shouldBe """{"town":"London"}"""
    }

  }
}
