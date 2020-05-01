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

import com.datamountaineer.streamreactor.connect.redis.sink.RedisSinkTask
import com.datamountaineer.streamreactor.connect.redis.sink.config.{RedisConfig, RedisConfigConstants, RedisConnectionInfo, RedisSinkSettings}
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.sink.{SinkRecord, SinkTaskContext}
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import redis.clients.jedis.Jedis
import redis.embedded.RedisServer

import scala.collection.JavaConverters._

class RedisMultipleSortedSetsTest extends AnyWordSpec with Matchers with BeforeAndAfterAll with MockitoSugar {

  val redisServer = new RedisServer(6379)

  override def beforeAll() = redisServer.start()

  override def afterAll() = redisServer.stop()

  "Redis INSERT into Multiple Sorted Sets (SS) writer" should {

    "write Kafka records a different Redis Sorted Set based on the value of the PK field" in {

      val TOPIC = "sensorsTopic"
      val KCQL = s"SELECT temperature, humidity FROM $TOPIC PK sensorID STOREAS SortedSet(score=ts) TTL = 60"

      val props = Map(
        RedisConfigConstants.REDIS_HOST->"localhost",
        RedisConfigConstants.REDIS_PORT->"6379",
        RedisConfigConstants.KCQL_CONFIG->KCQL
      ).asJava

      val config = RedisConfig(props)

      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val settings = RedisSinkSettings(config)
      val writer = new RedisMultipleSortedSets(settings)
      writer.createClient(settings)

      val schema = SchemaBuilder.struct().name("com.example.device")
        .field("sensorID", Schema.STRING_SCHEMA)
        .field("temperature", Schema.FLOAT64_SCHEMA)
        .field("humidity", Schema.FLOAT64_SCHEMA)
        .field("ts", Schema.INT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("sensorID", "sensor-123").put("temperature", 60.4).put("humidity", 90.1).put("ts", 1482180657010L)
      val struct2 = new Struct(schema).put("sensorID", "sensor-123").put("temperature", 62.1).put("humidity", 103.3).put("ts", 1482180657020L)
      val struct3 = new Struct(schema).put("sensorID", "sensor-789").put("temperature", 64.5).put("humidity", 101.1).put("ts", 1482180657030L)

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)
      val sinkRecord2 = new SinkRecord(TOPIC, 0, null, null, schema, struct2, 2)
      val sinkRecord3 = new SinkRecord(TOPIC, 0, null, null, schema, struct3, 3)

      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)

      writer.write(Seq(sinkRecord1))
      writer.write(Seq(sinkRecord2, sinkRecord3))

      jedis.zcard("sensor-123") shouldBe 2
      jedis.zcard("sensor-789") shouldBe 1

      val allSSrecords = jedis.zrange("sensor-789", 0, 999999999999L)
      val results = allSSrecords.asScala.toList
      results.head shouldBe """{"temperature":64.5,"humidity":101.1,"ts":1482180657030}"""

    }

    "multiple sorted sets task check" in {
      val TOPIC = "sensorsTopic"
      val KCQL = s"SELECT temperature, humidity FROM $TOPIC PK sensorID STOREAS SortedSet(score=ts)"

      val props = Map(
        RedisConfigConstants.REDIS_HOST->"localhost",
        RedisConfigConstants.REDIS_PORT->"6379",
        RedisConfigConstants.KCQL_CONFIG->KCQL
      ).asJava

      val context = mock[SinkTaskContext]
      when(context.configs()).thenReturn(props)
      val task = new RedisSinkTask
      task.initialize(context)
      task.start(props)

      val schema = SchemaBuilder.struct().name("com.example.device")
        .field("sensorID", Schema.STRING_SCHEMA)
        .field("temperature", Schema.FLOAT64_SCHEMA)
        .field("humidity", Schema.FLOAT64_SCHEMA)
        .field("ts", Schema.INT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("sensorID", "sensor-sink-task").put("temperature", 60.4).put("humidity", 90.1).put("ts", 1482180657010L)
      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)

      task.put(List(sinkRecord1).asJava)

      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      jedis.zcard("sensor-sink-task") shouldBe 1
    }

    "multiple sorted sets task check with prefix" in {
      val TOPIC = "sensorsTopic"
      val KCQL = s"INSERT INTO PREFIX- SELECT temperature, humidity FROM $TOPIC PK sensorID STOREAS SortedSet(score=ts) TTL = 60"

      val props = Map(
        RedisConfigConstants.REDIS_HOST->"localhost",
        RedisConfigConstants.REDIS_PORT->"6379",
        RedisConfigConstants.KCQL_CONFIG->KCQL
      ).asJava

      val context = mock[SinkTaskContext]
      when(context.configs()).thenReturn(props)
      val task = new RedisSinkTask
      task.initialize(context)
      task.start(props)

      val schema = SchemaBuilder.struct().name("com.example.device")
        .field("sensorID", Schema.STRING_SCHEMA)
        .field("temperature", Schema.FLOAT64_SCHEMA)
        .field("humidity", Schema.FLOAT64_SCHEMA)
        .field("ts", Schema.INT64_SCHEMA).build()

      val struct1 = new Struct(schema).put("sensorID", "sensor-sink-task-prefix").put("temperature", 60.4).put("humidity", 90.1).put("ts", 1482180657010L)
      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)

      task.put(List(sinkRecord1).asJava)

      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      jedis.zcard("PREFIX-sensor-sink-task-prefix") shouldBe 1
    }

    "multiple sorted sets 2 primary keys" in {
      val TOPIC = "sensorsTopic"
      val KCQL = s"SELECT temperature, humidity FROM $TOPIC PK sensorID, random STOREAS SortedSet(score=ts)"

      val props = Map(
        RedisConfigConstants.REDIS_HOST->"localhost",
        RedisConfigConstants.REDIS_PORT->"6379",
        RedisConfigConstants.KCQL_CONFIG->KCQL
      ).asJava

      val context = mock[SinkTaskContext]
      when(context.configs()).thenReturn(props)
      val task = new RedisSinkTask
      task.initialize(context)
      task.start(props)

      val schema = SchemaBuilder.struct().name("com.example.device")
        .field("sensorID", Schema.STRING_SCHEMA)
        .field("random", Schema.STRING_SCHEMA)
        .field("temperature", Schema.FLOAT64_SCHEMA)
        .field("humidity", Schema.FLOAT64_SCHEMA)
        .field("ts", Schema.INT64_SCHEMA).build()

      val struct1 = new Struct(schema)
        .put("sensorID", "sensor-sink-task")
        .put("random", "random")
        .put("temperature", 60.4)
        .put("humidity", 90.1)
        .put("ts", 1482180657010L)

      val sinkRecord1 = new SinkRecord(TOPIC, 0, null, null, schema, struct1, 1)

      task.put(List(sinkRecord1).asJava)

      val connectionInfo = new RedisConnectionInfo("localhost", 6379, None)
      val jedis = new Jedis(connectionInfo.host, connectionInfo.port)
      jedis.zcard("sensor-sink-task:random") shouldBe 1
    }
  }
}
