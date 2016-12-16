package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConnectionInfo, RedisSinkConfig, RedisSinkSettings}
import com.google.gson.Gson
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer
import scala.collection.JavaConverters._

class RedisInsertSortedSetSpec extends WordSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  val redisServer = new RedisServer(6379)

  override def beforeAll() = redisServer.start()

  override def afterAll() = redisServer.stop()

  "Redis INSERT into Sorted Set (SS) writer" should {

    "write Kafka records to a Redis Sorted Set" in {

      val KCQL = "INSERT INTO cpu_stats SELECT * from cpuTopic STOREAS SS"

      val config = mock[RedisSinkConfig]
      when(config.getString(REDIS_HOST)).thenReturn("localhost")
      when(config.getInt(REDIS_PORT)).thenReturn(6379)
      when(config.getString(REDIS_PASSWORD)).thenReturn("")
      when(config.getString(KCQL_CONFIG)).thenReturn(KCQL)
      when(config.getString(ERROR_POLICY)).thenReturn("THROW")
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisInsertSortedSet(settings)

      val schema = SchemaBuilder.struct().name("com.example.Cpu")
        .field("type", Schema.STRING_SCHEMA)
        .field("temperature", Schema.INT32_SCHEMA)
        .field("voltage", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord("topic", 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord("topic", 1, null, null, schema, struct2, 1)

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val gson = new Gson()
      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)

      val val1 = jedis.get("Alex")
      val1 should not be null

      val map1 = gson.fromJson(val1, classOf[java.util.Map[String, AnyRef]]).asScala
      map1("firstName").toString shouldBe "Alex"
      map1("age").toString shouldBe "30.0" //it gets back a java double!?

      val val2 = jedis.get("Mara")
      val2 should not be null

      val map2 = gson.fromJson(val2, classOf[java.util.Map[String, AnyRef]]).asScala
      map2("firstName") shouldBe "Mara"
      map2("age").toString shouldBe "22.0"
      map2("threshold").toString shouldBe "12.4"
    }

    "write Kafka records to Redis using CACHE mode with explicit KEY (using INSERT)" in {

      val TOPIC = "topic"
      val TABLE_NAME_RAW = "someTable"
      val QUERY_ALL = s"INSERT INTO $TABLE_NAME_RAW SELECT * FROM $TOPIC PK firstName"

      val config = mock[RedisSinkConfig]
      when(config.getString(REDIS_HOST)).thenReturn("localhost")
      when(config.getInt(REDIS_PORT)).thenReturn(6379)
      when(config.getString(REDIS_PASSWORD)).thenReturn("")
      when(config.getString(KCQL_CONFIG)).thenReturn(QUERY_ALL)
      when(config.getString(ERROR_POLICY)).thenReturn("THROW")
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)

      val settings = RedisSinkSettings(config)
      val writer = new RedisCache(settings)

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord("topic", 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord("topic", 1, null, null, schema, struct2, 1)

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val gson = new Gson()
      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)

      val val1 = jedis.get("Alex")
      val1 should not be null

      val map1 = gson.fromJson(val1, classOf[java.util.Map[String, AnyRef]]).asScala
      map1("firstName").toString shouldBe "Alex"
      map1("age").toString shouldBe "30.0" //it gets back a java double!?

      val val2 = jedis.get("Mara")
      val2 should not be null

      val map2 = gson.fromJson(val2, classOf[java.util.Map[String, AnyRef]]).asScala
      map2("firstName") shouldBe "Mara"
      map2("age").toString shouldBe "22.0"
      map2("threshold").toString shouldBe "12.4"
    }
  }
}
