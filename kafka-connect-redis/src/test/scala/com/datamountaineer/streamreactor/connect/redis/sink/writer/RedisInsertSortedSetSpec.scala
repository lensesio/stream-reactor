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

      val TOPIC = "cpuTopic"
      val KCQL = s"INSERT INTO cpu_stats SELECT * from $TOPIC STOREAS SS(score=ts)"
      println("Testing KCQL : " + KCQL)

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
        .field("temperature", Schema.FLOAT64_SCHEMA)
        .field("voltage", Schema.FLOAT64_SCHEMA)
        .field("ts", Schema.INT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("type", "Xeon").put("temperature", 60.4).put("voltage",  90.1).put("ts", System.currentTimeMillis)
      val struct2 = new Struct(schema).put("type", "i7")  .put("temperature", 62.1).put("voltage", 103.3).put("ts", System.currentTimeMillis+10)

      val sinkRecord1 = new SinkRecord(TOPIC, 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord(TOPIC, 1, null, null, schema, struct2, 1)

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val gson = new Gson()
      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)

      val val1 = jedis.zrange("cpu_stats", -1, 1000000000000000L)
      val1 should not be null
      val1.size shouldBe 2

      //      val map1 = gson.fromJson(val1, classOf[java.util.Map[String, AnyRef]]).asScala
      //      map1("firstName").toString shouldBe "Alex"
      //      map1("age").toString shouldBe "30.0" //it gets back a java double!?

    }

  }

}
