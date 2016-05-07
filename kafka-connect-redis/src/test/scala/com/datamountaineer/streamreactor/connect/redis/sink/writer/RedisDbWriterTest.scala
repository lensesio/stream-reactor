package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.schemas.StructFieldsExtractor
import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisConnectionInfo
import com.datamountaineer.streamreactor.connect.sink.StringKeyBuilder
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

  val redisServer = new RedisServer(6379)

  override def beforeAll() = redisServer.start()

  override def afterAll() = redisServer.stop()


  "RedisDbWriter" should {
    "write the given SinkRecords to the redis database" in {

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord("topic", 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord("topic", 1, null, null, schema, struct2, 1)


      val keyBuilder = mock[StringKeyBuilder]
      when(keyBuilder.build(sinkRecord1)).thenReturn("10")
      when(keyBuilder.build(sinkRecord2)).thenReturn("11")

      val fieldsExtractor = mock[StructFieldsExtractor]
      when(fieldsExtractor.get(struct1)).thenReturn(Seq("firstName" -> "Alex", "age" -> Int.box(30)))
      when(fieldsExtractor.get(struct2)).thenReturn(Seq("firstName" -> "Mara", "age" -> Int.box(22), "threshold" -> Double.box(12.4)))

      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val writer = RedisDbWriter(connectionInfo, fieldsExtractor, keyBuilder)

      writer.write(Seq(sinkRecord1, sinkRecord2))


      val gson = new Gson()
      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)

      val val1 = jedis.get("10")
      val1 should not be null

      val map1 = gson.fromJson(val1, classOf[java.util.Map[String, AnyRef]]).asScala
      map1.get("firstName").get shouldBe "Alex"
      map1.get("age").get.toString shouldBe "30.0" //it gets back a java double!?

      val val2 = jedis.get("11")
      val2 should not be null

      val map2 = gson.fromJson(val2, classOf[java.util.Map[String, AnyRef]]).asScala
      map2.get("firstName").get shouldBe "Mara"
      map2.get("age").get.toString shouldBe "22.0"
      map2.get("threshold").get.toString shouldBe "12.4"
    }
  }
}
