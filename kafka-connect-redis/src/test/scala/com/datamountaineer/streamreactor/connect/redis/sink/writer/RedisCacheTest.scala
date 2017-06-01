/*
 * Copyright 2017 Datamountaineer.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datamountaineer.streamreactor.connect.redis.sink.writer

import com.datamountaineer.streamreactor.connect.redis.sink.config.RedisSinkConfig._
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConnectionInfo, RedisSinkConfig, RedisSinkConfigConstants, RedisSinkSettings}
import com.google.gson.Gson
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.SinkRecord
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

class RedisCacheTest extends WordSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  val redisServer = new RedisServer(6379)

  override def beforeAll() = redisServer.start()

  override def afterAll() = redisServer.stop()

  "RedisDbWriter" should {

    "write Kafka records to Redis using CACHE mode" in {

      val TOPIC = "topic"
      val TABLE_NAME_RAW = "someTable"
      val QUERY_ALL = s"SELECT * FROM $TOPIC PK firstName"

      val config = mock[RedisSinkConfig]
      when(config.getString(RedisSinkConfigConstants.REDIS_HOST)).thenReturn("localhost")
      when(config.getInt(RedisSinkConfigConstants.REDIS_PORT)).thenReturn(6379)
      when(config.getString(RedisSinkConfigConstants.REDIS_PASSWORD)).thenReturn("")
      when(config.getString(RedisSinkConfigConstants.KCQL_CONFIG)).thenReturn(QUERY_ALL)
      when(config.getString(RedisSinkConfigConstants.ERROR_POLICY)).thenReturn("THROW")
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisCache(settings)

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord(TOPIC, 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord(TOPIC, 1, null, null, schema, struct2, 1)

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

    "write Kafka records to Redis using CACHE mode and PK field is not in the selected fields" in {

      val TOPIC = "topic"
      val TABLE_NAME_RAW = "someTable"
      val QUERY_ALL = s"SELECT age FROM $TOPIC PK firstName"

      val config = mock[RedisSinkConfig]
      when(config.getString(RedisSinkConfigConstants.REDIS_HOST)).thenReturn("localhost")
      when(config.getInt(RedisSinkConfigConstants.REDIS_PORT)).thenReturn(6379)
      when(config.getString(RedisSinkConfigConstants.REDIS_PASSWORD)).thenReturn("")
      when(config.getString(RedisSinkConfigConstants.KCQL_CONFIG)).thenReturn(QUERY_ALL)
      when(config.getString(RedisSinkConfigConstants.ERROR_POLICY)).thenReturn("THROW")
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisCache(settings)

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord(TOPIC, 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord(TOPIC, 1, null, null, schema, struct2, 1)

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val gson = new Gson()
      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)

      val val1 = jedis.get("Alex")
      val1 should not be null

      val map1 = gson.fromJson(val1, classOf[java.util.Map[String, AnyRef]]).asScala
      map1("age").toString shouldBe "30.0" //it gets back a java double!?

      val val2 = jedis.get("Mara")
      val2 should not be null

      val map2 = gson.fromJson(val2, classOf[java.util.Map[String, AnyRef]]).asScala
      map2("age").toString shouldBe "22.0"
    }


    "write Kafka records to Redis using CACHE mode with explicit KEY (using INSERT)" in {

      val TOPIC = "topic2"
      val KCQL = s"INSERT INTO KEY_PREFIX_ SELECT * FROM $TOPIC PK firstName"

      val config = mock[RedisSinkConfig]
      when(config.getString(RedisSinkConfigConstants.REDIS_HOST)).thenReturn("localhost")
      when(config.getInt(RedisSinkConfigConstants.REDIS_PORT)).thenReturn(6379)
      when(config.getString(RedisSinkConfigConstants.REDIS_PASSWORD)).thenReturn("")
      when(config.getString(RedisSinkConfigConstants.KCQL_CONFIG)).thenReturn(KCQL)
      when(config.getString(RedisSinkConfigConstants.ERROR_POLICY)).thenReturn("THROW")
      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)

      val settings = RedisSinkSettings(config)
      val writer = new RedisCache(settings)

      val schema = SchemaBuilder.struct().name("com.example.Person")
        .field("firstName", Schema.STRING_SCHEMA)
        .field("age", Schema.INT32_SCHEMA)
        .field("threshold", Schema.OPTIONAL_FLOAT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("firstName", "Alex").put("age", 30)
      val struct2 = new Struct(schema).put("firstName", "Mara").put("age", 22).put("threshold", 12.4)

      val sinkRecord1 = new SinkRecord(TOPIC, 1, null, null, schema, struct1, 0)
      val sinkRecord2 = new SinkRecord(TOPIC, 1, null, null, schema, struct2, 1)

      writer.write(Seq(sinkRecord1, sinkRecord2))

      val gson = new Gson()
      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)

      val val1 = jedis.get("KEY_PREFIX_Alex")
      val1 should not be null

      val map1 = gson.fromJson(val1, classOf[java.util.Map[String, AnyRef]]).asScala
      map1("firstName").toString shouldBe "Alex"
      map1("age").toString shouldBe "30.0" //it gets back a java double!?

      val val2 = jedis.get("KEY_PREFIX_Mara")
      val2 should not be null

      val map2 = gson.fromJson(val2, classOf[java.util.Map[String, AnyRef]]).asScala
      map2("firstName") shouldBe "Mara"
      map2("age").toString shouldBe "22.0"
      map2("threshold").toString shouldBe "12.4"
    }
  }
}
