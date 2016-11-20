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

class RedisDbWriterTest extends WordSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  /**
    * This will get every message from <topic> and for every one create a (pipe-separated) <key< in Redis i.e:
    *   <topic>|<partition>|<offset
    */
  val KCQL1 = "SELECT * from topic"


  "Redis DB writer" should {

    /**
      * Get every <message> from `topicA` and store in a unique Redis key:
      *    <topicName>|<partition>|<offset>
      *
      *  INSERT INTO xx SELECT * FROM topic
      *  SELECT * from topic WITHFORMAT avro
      *
      *  The above 2 KCQLs have the same result ..
      */
    val KCQL1 = "SELECT * from topic WITHFORMAT avro"
    //val KCQL1 = "INSERT INTO xx SELECT * FROM topic"

    KCQL1 in {

      val TOPIC = "topic"
      val KCQL = KCQL1

      val config = getMockServer(KCQL)
      val settings = RedisSinkSettings(config)
      val writer = RedisDbWriter(settings)

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord(TOPIC, 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord(TOPIC, 1, null, null, schema, struct2, 1)

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)

      val val1 = jedis.get(s"$TOPIC|1|0")
      val1 should not be null

      val map1 = gson.fromJson(val1, classOf[java.util.Map[String, AnyRef]]).asScala
      map1("firstName").toString shouldBe "Alex"
      map1("age").toString shouldBe "30.0" //it gets back a java double!?

      val val2 = jedis.get(s"$TOPIC|1|1")
      val2 should not be null

      val map2 = gson.fromJson(val2, classOf[java.util.Map[String, AnyRef]]).asScala
      map2("firstName") shouldBe "Mara"
      map2("age").toString shouldBe "22.0"
      map2("threshold").toString shouldBe "12.4"
    }


    /**
      * Define a `field` as a PK (Primary Key)
      * Get every <message> from `topicA` and store in a unique Redis key:
      *    <topicName>|<partition>|<offset>
      *
      *  INSERT INTO xx SELECT * FROM topic
      *  SELECT * from topic WITHFORMAT avro
      *
      *  The above 2 KCQLs have the same result ..
      */
    // val KCQL = s"INSERT INTO $TABLE_NAME_RAW SELECT * FROM $TOPIC PK firstName"
    "SELECT * FROM $TOPIC PK firstName" in {

      val TOPIC = "topic"
      val TABLE_NAME_RAW = "someTable"
      val KCQL = s"SELECT * FROM $TOPIC PK firstName"

      val config = getMockServer(KCQL)
      val settings = RedisSinkSettings(config)
      val writer = RedisDbWriter(settings)

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord(TOPIC, 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord(TOPIC, 1, null, null, schema, struct2, 1)

      writer.write(Seq(sinkRecord1, sinkRecord2))

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

  override def beforeAll() = redisServer.start()

  override def afterAll() = redisServer.stop()

  val gson = new Gson()
  val redisServer = new RedisServer(6379)
  val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)

  val schema = SchemaBuilder.struct().name("com.example.Person")
    .field("firstName", Schema.STRING_SCHEMA)
    .field("age", Schema.INT32_SCHEMA)
    .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

  private def getMockServer(KCQLQuery: String) = {
    val config = mock[RedisSinkConfig]
    when(config.getString(REDIS_HOST)).thenReturn("localhost")
    when(config.getInt(REDIS_PORT)).thenReturn(6379)
    when(config.getString(REDIS_PASSWORD)).thenReturn("")
    when(config.getString(EXPORT_ROUTE_QUERY)).thenReturn(KCQLQuery)
    when(config.getString(ERROR_POLICY)).thenReturn("THROW")
    config
  }

}
